# -*- coding: utf-8 -*-
import os
import sys

# Configurar codificação UTF-8 explicitamente
if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain.chat_models import init_chat_model
from langchain.tools import tool
import time
import re
from .rate_limiter import RateLimiter, SmartCache

class AgentTools:
    def __init__(self, db_uri: str):
        # Inicializar rate limiter e cache inteligente
        self.rate_limiter = RateLimiter(max_requests_per_second=1, max_requests_per_minute=20)
        self.smart_cache = SmartCache(default_ttl=600)  # 10 minutos
        self.error_patterns = {
            'column_not_exist': r'column "([^"]+)" does not exist',
            'table_not_exist': r'relation "([^"]+)" does not exist',
            'date_range_error': r'year (-?\d+) is out of range',
            'syntax_error': r'syntax error at or near "([^"]+)"'
        }
        
        try:
            print(f"🔗 Tentando conectar ao banco: {db_uri.replace(db_uri.split('@')[0].split('//')[1], '***')}")
            
            # Configurar banco com segurança e amostras 
            self.db = SQLDatabase.from_uri(
                db_uri,
                sample_rows_in_table_info=3,  # Mostrar 3 linhas de exemplo
                schema='public',
                # Removendo include_tables para permitir acesso a todas as tabelas disponíveis
                custom_table_info=None,
                view_support=False,
                max_string_length=300
            )
            print("✅ Conexão com banco estabelecida")
            
            # Inicializar o modelo LLM
            self.llm = init_chat_model("gemini-2.5-flash", model_provider="google_genai")
            
            # Prompt de sistema melhorado com conhecimento específico
            system_prompt = f"""Você é um especialista em SQL e análise de dados com conhecimento específico do banco de dados PostgreSQL da empresa.

                            🚨 ATENÇÃO CRÍTICA: A tabela 'entidades' NÃO possui coluna 'id'. NUNCA use 'id' em consultas!

                            CONHECIMENTO OBRIGATÓRIO DO BANCO - USE EXATAMENTE ESTES NOMES:

                            🔹 TABELA: entidades (Clientes/Fornecedores/Vendedores/Transportadoras)
                            CAMPOS OBRIGATÓRIOS (MEMORIZE ESTES NOMES):
                            - enti_clie (ID da entidade - USE ESTE CAMPO como identificador único, NUNCA "id")
                            - enti_nome (Nome da entidade - USE ESTE CAMPO, NUNCA "nome")
                            - enti_tipo_enti (Tipo da entidade - USE ESTE CAMPO, NUNCA "tipo")
                            - enti_ende (Endereço)
                            - enti_esta (Estado)
                            - enti_tele (Telefone)
                            - enti_celu (Celular)

                            ⚠️ CAMPOS QUE NÃO EXISTEM (NUNCA USE):
                            - id (NÃO EXISTE - use enti_clie)
                            - nome (NÃO EXISTE - use enti_nome)
                            - tipo (NÃO EXISTE - use enti_tipo_enti)
                            - codigo (NÃO EXISTE - use enti_clie)
                            - nunca use a tabela de faturamento, sempre a de pedidos, ou notas fiscais.

                            VALORES DO CAMPO enti_tipo_enti:
                            - 'CL' = Cliente
                            - 'FO' = Fornecedor  
                            - 'VE' = Vendedor
                            - 'TR' = Transportadora
                            - 'OU' = Outros
                            - 'AM' = Ambos

                            🔹 TABELA: produtos
                            CAMPOS: prod_codi, prod_nome

                            🔹 TABELA: saldosprodutos  
                            CAMPOS: sapr_prod, sapr_sald

                            🔹 TABELA: pedidosvenda
                            CAMPOS: pedi_nume, pedi_data, pedi_forn, pedi_tota

                            🔹 TABELA: itenspedidovenda
                            CAMPOS: iped_pedi, iped_prod, iped_quan, iped_unit, iped_tota

                            🔹 TABELA: titulospagar
                            CAMPOS: titu_id, titu_forn, titu_valo, titu_venc

                            🔹 TABELA: titulosreceber
                            CAMPOS: titu_id, titu_clie, titu_valo, titu_venc

                            RELACIONAMENTOS IMPORTANTES:
                            - saldosprodutos.sapr_prod → produtos.prod_codi
                            - itenspedidovenda.iped_prod → produtos.prod_codi
                            - pedidosvenda.pedi_forn → entidades.enti_clie
                            - itenspedidovenda.iped_pedi → pedidosvenda.pedi_nume
                            - titulosreceber.titu_clie → entidades.enti_clie

                            REGRAS CRÍTICAS:
                            1. 🚨 NUNCA use 'id', 'nome', 'tipo' - use enti_clie, enti_nome, enti_tipo_enti
                            2. SEMPRE use os nomes EXATOS dos campos listados acima
                            3. NUNCA invente nomes de colunas
                            4. Para contagem: COUNT(enti_clie) ou COUNT(*) - NUNCA COUNT(id)
                            4a. Para soma de itens mais vendidos : SUM(iped_quan) - NUNCA SUM(id)
                            4b. Para soma de valor total de vendas : SUM(iped_tota) - NUNCA SUM(iped_quan)
                            4c. Para soma de valor total de titulos a pagar : SUM(titu_valo) - NUNCA SUM(titu_id)
                            4d. Para soma de valor total de titulos a receber : SUM(titu_valo) - NUNCA SUM(titu_id)
                            4e. Para buscar o valor do estoque a tabela correta é tabelaprecos, use o campo tabe_prod e tabe_prco
                            4f. Para buscar o valor do estoque de um produto específico use o campo sapr_prod e sapr_sald
                            5. SEMPRE use o campo enti_clie para filtros e joins
                            6. SEMPRE use LIMIT para evitar sobrecarga
                            7. APENAS comandos SELECT são permitidos
                            8. Se erro de coluna, consulte esta lista de campos obrigatórios
                            9. Responda em português brasileiro com markdown
                            10. Inclua insights e análises dos dados
                            11. Termine com sugestões de próximas consultas mas não as sqls, apenas as perguntas.

                            EXEMPLOS CORRETOS OBRIGATÓRIOS:

                            Para contar entidades:
                            SELECT COUNT(*) as total FROM entidades;

                            Para listar entidades:
                            SELECT enti_clie as codigo, enti_nome as nome, enti_tipo_enti as tipo 
                            FROM entidades LIMIT 10;

                            Para entidades por tipo:
                            SELECT enti_tipo_enti as tipo, COUNT(*) as quantidade 
                            FROM entidades 
                            GROUP BY enti_tipo_enti 
                            ORDER BY quantidade DESC;
                            """
                                        
            # Criar o agente SQL com configurações de segurança
            self.sql_agent = create_sql_agent(
                llm=self.llm,
                db=self.db,
                agent_type="openai-tools",
                verbose=True,
                system_message=system_prompt,
              
            )
            print('✅ Database inicializado:', self.db.dialect)
            print('✅ SQL Agent criado com sucesso (modo read-only)')
            print('✅ Tabelas configuradas:', ['produtos', 'saldosprodutos', 'entidades', 'titulospagar', 'titulosreceber'])
        except UnicodeDecodeError as ude:
            print(f'❌ Erro de codificação UTF-8 ao inicializar AgentTools: {ude}')
            print('💡 Sugestão: Verifique se todos os arquivos estão salvos em UTF-8')
            raise
        except ImportError as ie:
            print(f'❌ Erro de importação: {ie}')
            print('💡 Sugestão: Verifique se todas as dependências estão instaladas')
            raise
        except ConnectionError as ce:
            print(f'❌ Erro de conexão com banco: {ce}')
            print('💡 Sugestão: Verifique se o PostgreSQL está rodando e as credenciais estão corretas')
            raise
        except Exception as e:
            print(f'❌ Erro ao inicializar AgentTools: {e}')
            print(f'❌ Tipo do erro: {type(e).__name__}')
            print(f'❌ Detalhes: {str(e)}')
            raise

    def query_database(self, question: str) -> str:
        """Executa uma consulta SQL no banco de dados com rate limiting e cache inteligente."""
        
        # Verificar cache primeiro
        cache_key = f"query_{hash(question)}"
        cached_result = self.smart_cache.get(cache_key)
        if cached_result:
            return f"📋 **[Cache]** {cached_result}"
        
        # Verificar rate limiting
        if not self.rate_limiter.can_proceed():
            wait_time = self.rate_limiter.wait_time()
            return f"⏳ **Rate limit atingido.** Aguarde {wait_time:.1f} segundos antes de fazer nova consulta.\n\n💡 **Dica:** Use consultas mais específicas para otimizar o cache."
        
        try:
            # Pré-processar pergunta para evitar erros comuns
            processed_question = self._preprocess_question(question)
            
            # Adicionar contexto para respostas em linguagem natural
            enhanced_question = f"""
            {processed_question}
            
            INSTRUÇÕES CRÍTICAS:
            - SEMPRE use LIMIT nas consultas para evitar sobrecarga
            4. Para contagem: COUNT(enti_clie) ou COUNT(*) - NUNCA COUNT(id)
            4a. Para soma de itens mais vendidos : SUM(iped_quan) - NUNCA SUM(id)
            4b. Para soma de valor total de vendas : SUM(iped_tota) - NUNCA SUM(iped_quan)
            4c. Para soma de valor total de titulos a pagar : SUM(titu_valo) - NUNCA SUM(titu_id)
            4d. Para soma de valor total de titulos a receber : SUM(titu_valo) - NUNCA SUM(titu_id)
            4e. Para buscar o valor do estoque a tabela correta é tabelaprecos, use o campo tabe_prod e tabe_prco
            4f. Para buscar o valor do estoque de um produto específico use o campo sapr_prod e sapr_sald
            - o sistmea tem os prefixos de _empr e fili, sempre que solicitado empresa e filial filtrar, pela empresa e filial
            - Se encontrar erro de coluna inexistente, tente consultar o schema primeiro
            - Para datas, use funções de conversão adequadas (TO_DATE, CAST)
            - Responda em linguagem natural com insights sobre os dados
            - Use formatação markdown para melhor apresentação
            - Se não encontrar dados, explique possíveis motivos
            - Inclua análises e interpretações dos resultados
            - Termine com sugestões de consultas relacionadas
            
            TRATAMENTO DE ERROS:
            - Se coluna não existir, sugira colunas similares
            - Se tabela não existir, liste tabelas disponíveis
            - Para erros de data, use formatos padrão (YYYY-MM-DD)
            """
            
            result = self.sql_agent.invoke({"input": enhanced_question})
            output = result.get("output", str(result))
            
            # Verificar se houve erro e tentar recuperação
            if self._has_critical_error(output):
                recovery_result = self._attempt_error_recovery(question, output)
                if recovery_result:
                    output = recovery_result
            
            # Salvar no cache apenas se não houve erro
            if not self._has_critical_error(output):
                self.smart_cache.set(cache_key, output)
            
            return output
            
        except Exception as e:
            error_msg = str(e)
            error_type = self._classify_error(error_msg)
            
            return f"""**🚨 Erro na consulta:** {error_type}

**Detalhes:** {error_msg}

**🔧 Soluções sugeridas:**
{self._get_error_solutions(error_type, error_msg)}

**💡 Dicas para evitar erros:**
- Use consultas mais simples e específicas
- Consulte o schema das tabelas primeiro
- Evite consultas muito complexas
- Use LIMIT para limitar resultados
"""
    
    def get_table_info(self, table_name: str) -> str:
        """Retorna as informações de uma tabela específica com tratamento robusto de erros de data."""
        try:
            return self.db.get_table_info([table_name])
        except Exception as e:
            error_msg = str(e)
            # Tratamento específico para erros de data fora do range
            if 'year' in error_msg and 'is out of range' in error_msg:
                print(f"⚠️ Aviso: Tabela {table_name} contém dados com datas inválidas, mas schema será acessado")
                # Tentar acessar schema sem dados de exemplo
                try:
                    # Usar uma consulta direta para obter apenas a estrutura
                    result = self.db.run(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = 'public' LIMIT 20")
                    if result:
                        return f"Tabela {table_name} (estrutura básica - dados com datas inválidas ignorados):\n{result}"
                    else:
                        return f"Tabela {table_name}: Estrutura disponível (dados com datas inválidas ignorados)"
                except:
                    return f"Tabela {table_name}: Disponível (dados com datas inválidas ignorados - use consultas diretas)"
            return f"Erro ao obter informações da tabela {table_name}: {str(e)}"
    
    def get_database_schema(self, table_names: str = None) -> str:
        """Retorna informações sobre todas as tabelas configuradas com tratamento robusto de erros."""
        try:
            if table_names:
                tables = [t.strip() for t in table_names.split(',')]
            else:
                tables = ['produtos', 'saldosprodutos', 'entidades', 'titulospagar', 'titulosreceber']
            
            schema_info = "## Estrutura do Banco de Dados\n\n"
            
            for table in tables:
                try:
                    info = self.get_table_info(table)  # Usar nossa função melhorada
                    schema_info += f"### Tabela: {table}\n```sql\n{info}\n```\n\n"
                except Exception as e:
                    error_msg = str(e)
                    if 'year' in error_msg and 'is out of range' in error_msg:
                        fallback_schema = self._get_fallback_schema(table)
                        schema_info += f"### Tabela: {table}\n{fallback_schema}\n\n"
                    else:
                        schema_info += f"### Tabela: {table}\n*Tabela não encontrada ou sem permissão: {error_msg}*\n\n"
            
            return schema_info
        except Exception as e:
            return f"Erro ao obter schema do banco: {str(e)}"
    
    def _get_fallback_schema(self, table_name: str) -> str:
        """Retorna schema conhecido quando há erro de data inválida"""
        schemas = {
            'entidades': """
⚠️ Schema obtido via fallback devido a dados de data inválidos:

```sql
CREATE TABLE entidades (
    enti_clie INTEGER PRIMARY KEY,
    enti_nome VARCHAR(100),
    enti_tipo_enti VARCHAR(2), -- CL=Cliente, FO=Fornecedor, VE=Vendedor, TR=Transportadora
    enti_ende VARCHAR(200),
    enti_esta VARCHAR(2),
    enti_tele VARCHAR(20),
    enti_celu VARCHAR(20),
    enti_cnpj VARCHAR(18),
    enti_cpf VARCHAR(14)
);
```
            """,
            'titulospagar': """
⚠️ Schema obtido via fallback devido a dados de data inválidos:

```sql
CREATE TABLE titulospagar (
    titu_id INTEGER PRIMARY KEY,
    titu_forn INTEGER, -- Referência para entidades
    titu_valo DECIMAL(15,2),
    titu_venc DATE,
    titu_desc VARCHAR(200),
    titu_empr INTEGER -- Referência para empresas
);
```
            """,
            'empresas': """
⚠️ Schema obtido via fallback devido a dados de data inválidos:

```sql
CREATE TABLE empresas (
    empr_codi INTEGER PRIMARY KEY,
    empr_nome VARCHAR(100),
    empr_fant VARCHAR(100),
    empr_cnpj VARCHAR(18)
);
```
            """,
            'produtos': """
⚠️ Schema obtido via fallback devido a dados de data inválidos:

```sql
CREATE TABLE produtos (
    prod_codi INTEGER PRIMARY KEY,
    prod_nome VARCHAR(200),
    prod_prec DECIMAL(15,2)
);
```
            """,
            'pedidosvenda': """
⚠️ Schema obtido via fallback devido a dados de data inválidos:

```sql
CREATE TABLE pedidosvenda (
    pedi_nume INTEGER PRIMARY KEY,
    pedi_data DATE,
    pedi_forn INTEGER, -- Referência para entidades
    pedi_tota DECIMAL(15,2)
);
```
            """
        }
        
        if table_name in schemas:
            return schemas[table_name]
        else:
            return f"*Tabela {table_name} disponível (contém dados com datas inválidas - use consultas diretas)*"
    
    def _preprocess_question(self, question: str) -> str:
        """Pré-processa a pergunta para evitar erros comuns e mapear termos para nomes corretos"""
        
        # Detectar se é uma pergunta sobre contagem/quantidade de entidades
        if any(term in question.lower() for term in ['quantos', 'quantidade', 'numero', 'número', 'count', 'clientes', 'entidades']):
            return """
            PERGUNTA: Quantos clientes/entidades existem e quais são seus nomes?
            
            ATENÇÃO CRÍTICA: A tabela 'entidades' NÃO possui coluna 'id'. 
            
            CAMPOS CORRETOS da tabela entidades:
            - enti_clie (código do cliente - use como identificador único)
            - enti_nome (nome da entidade)
            - enti_tipo_enti (tipo: CL=Cliente, FO=Fornecedor, etc.)
            - enti_ende (endereço)
            - enti_esta (estado)
            - enti_tele (telefone)
            - enti_celu (celular)
            
            CONSULTA CORRETA OBRIGATÓRIA:
            SELECT COUNT(*) as total_entidades, 
                   COUNT(CASE WHEN enti_tipo_enti = 'CL' THEN 1 END) as total_clientes
            FROM entidades;
            
            Para listar nomes, use:
            SELECT enti_clie as codigo, enti_nome as nome, enti_tipo_enti as tipo 
            FROM entidades 
            WHERE enti_tipo_enti = 'CL' 
            ORDER BY enti_nome 
            LIMIT 10;
            
            NUNCA use 'id' - sempre use 'enti_clie' como identificador!
            """
        
        # Mapeamento inteligente de termos comuns
        replacements = {
            # Termos para entidades
            'entidades por tipo': 'SELECT enti_tipo_enti as tipo, COUNT(*) as quantidade FROM entidades GROUP BY enti_tipo_enti ORDER BY quantidade DESC',
            'tipos de entidades': 'SELECT enti_tipo_enti as tipo, COUNT(*) as quantidade FROM entidades GROUP BY enti_tipo_enti ORDER BY quantidade DESC',
            'gráfico das entidades': 'SELECT enti_tipo_enti as tipo, COUNT(*) as quantidade FROM entidades GROUP BY enti_tipo_enti ORDER BY quantidade DESC',
            'grafico das entidades': 'SELECT enti_tipo_enti as tipo, COUNT(*) as quantidade FROM entidades GROUP BY enti_tipo_enti ORDER BY quantidade DESC',
            
            # Mapeamento de campos - CRÍTICO: remover referências a 'id'
            'id_cliente': 'enti_clie',
            'codigo_cliente': 'enti_clie', 
            'nome_cliente': 'enti_nome',
            'nome_entidade': 'enti_nome',
            'tipo_entidade': 'enti_tipo_enti',
            'tipo_cliente': 'enti_tipo_enti',
            'endereco': 'enti_ende',
            'estado': 'enti_esta',
            'telefone': 'enti_tele',
            'celular': 'enti_celu',
            
            # Produtos
            'codigo_produto': 'prod_codi',
            'nome_produto': 'prod_nome',
            
            # Pedidos
            'numero_pedido': 'pedi_nume',
            'data_pedido': 'pedi_data',
            'cliente_pedido': 'pedi_forn',
            'total_pedido': 'pedi_tota',
            'valor_total': 'pedi_tota',
            
            # Itens pedido
            'quantidade': 'iped_quan',
            'preco_unitario': 'iped_unit',
            'valor_item': 'iped_tota',
            
            # Títulos
            'valor_titulo': 'titu_valo',
            'vencimento': 'titu_venc',
            
            # Termos genéricos que causam erro - CRÍTICO
            ' tipo ': ' enti_tipo_enti ',
            ' id ': ' enti_clie ',
            ' nome ': ' enti_nome ',
            ' codigo ': ' enti_clie ',
            'count(id)': 'COUNT(enti_clie)',
            'COUNT(id)': 'COUNT(enti_clie)',
            'select id': 'SELECT enti_clie',
            'SELECT id': 'SELECT enti_clie'
        }
        
        processed = question.lower()
        
        # Aplicar substituições
        for old_term, new_term in replacements.items():
            processed = processed.replace(old_term.lower(), new_term)
        
        # Adicionar contexto específico para consultas de entidades
        if 'tpo de entidade' in processed and ('tipo' in question.lower() or 'gráfico' in question.lower() or 'grafico' in question.lower()):
            processed = """
            Crie um gráfico/relatório das entidades agrupadas por tipo.
            
            IMPORTANTE: Use EXATAMENTE esta consulta:
            SELECT enti_tipo_enti as tipo, COUNT(*) as quantidade 
            FROM entidades 
            GROUP BY enti_tipo_enti 
            ORDER BY quantidade DESC
            LIMIT 10;
            
            Depois explique o significado de cada tipo:
            - CL = Cliente
            - FO = Fornecedor  
            - VE = Vendedor
            - TR = Transportadora
            - OU = Outros
            - AM = Ambos
            """
        
        # Tratar consultas de aniversariantes
        if 'aniversario' in question or 'nascimento' or 'aniversar' in question or 'data de nascimento' in question:
            processed = """
            Liste os próximos aniversariantes.
            
            IMPORTANTE: Use EXATAMENTE esta consulta para evitar erros de data:
            
            
                WITH dados_validos AS (
                    SELECT 
                        DISTINCT enti_empr, 
                        enti_clie AS codigo,
                        enti_nome AS nome,
                        enti_tipo_enti AS tipo,
                        enti_dana,
                        CASE 
                            WHEN enti_dana IS NOT NULL AND EXTRACT(YEAR FROM enti_dana) BETWEEN 1900 AND 2100 
                            THEN TO_CHAR(enti_dana, 'DD/MM') 
                            ELSE 'Data inválida' 
                        END AS aniversario,
                        CASE 
                            WHEN enti_dana IS NOT NULL AND EXTRACT(YEAR FROM enti_dana) BETWEEN 1900 AND 2100 
                            THEN EXTRACT(MONTH FROM enti_dana)
                            ELSE 99 
                        END AS mes_nascimento,
                        CASE 
                            WHEN enti_dana IS NOT NULL AND EXTRACT(YEAR FROM enti_dana) BETWEEN 1900 AND 2100 
                            THEN EXTRACT(DAY FROM enti_dana)    
                            ELSE 99
                        END AS dia_nascimento,
                        COALESCE(enti_fone, enti_celu, 'Sem telefone') AS contato,
                        enti_emai AS email
                    FROM entidades
                    WHERE enti_dana IS NOT NULL
                    AND EXTRACT(YEAR FROM enti_dana) BETWEEN 1900 AND 2100
                ),
                aniversarios_validos AS (
                    SELECT *,
                        MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int, 
                                    mes_nascimento::int, 
                                    dia_nascimento::int) AS aniversario_deste_ano
                    FROM dados_validos
                    WHERE
                    enti_empr = 1 AND
                    mes_nascimento BETWEEN 1 AND 12
                    AND dia_nascimento BETWEEN 1 AND 31
                    -- Filtra combinações impossíveis
                    AND (mes_nascimento, dia_nascimento) NOT IN ((2,30), (2,31), (4,31), (6,31), (9,31), (11,31))
                    -- Exclui 29/02 em ano não bissexto
                    AND NOT (
                        mes_nascimento = 2 AND dia_nascimento = 29 AND
                        NOT (
                            (EXTRACT(YEAR FROM CURRENT_DATE)::int % 4 = 0 AND EXTRACT(YEAR FROM CURRENT_DATE)::int % 100 != 0)
                            OR (EXTRACT(YEAR FROM CURRENT_DATE)::int % 400 = 0)
                        )
                    )
                )
                SELECT 
                    enti_empr, codigo,  nome, tipo, aniversario, mes_nascimento, dia_nascimento, contato, email
                FROM aniversarios_validos
                ORDER BY aniversario_deste_ano

            
            Esta consulta:
            - Filtra datas válidas para evitar erros
            - Mostra nome, tipo, data de aniversário e contato
            - Ordena por mês e dia do aniversário
            - Trata datas inválidas adequadamente
            - Inclui apenas clientes, vendedores e fornecedores
            - pegue sempre da data atual para calcular o próximo aniversário

            """

        # Tratar consultas de títulos a pagar com empresas
        if 'titulos' in processed and 'pagar' in processed and ('empresa' in processed or 'entidade' in processed):
            processed = """
            Liste os próximos títulos a pagar por empresa com entidades.
            
            IMPORTANTE: Use EXATAMENTE esta consulta para evitar erros de data:
            
            SELECT 
                e.empr_nome as empresa,
                t.titu_desc as descricao_titulo,
                t.titu_valo as valor_titulo,
                CASE 
                    WHEN t.titu_venc IS NOT NULL AND EXTRACT(YEAR FROM t.titu_venc) BETWEEN 1900 AND 2100 
                    THEN t.titu_venc::text 
                    ELSE 'Data inválida' 
                END as vencimento,
                COALESCE(ent.enti_nome, 'Sem entidade') as entidade_nome
            FROM titulospagar t
            JOIN empresas e ON t.titu_empr = e.empr_codi
            LEFT JOIN entidades ent ON t.titu_forn = ent.enti_clie
            WHERE t.titu_valo > 0
            ORDER BY e.empr_nome, t.titu_valo DESC
            LIMIT 10;
            
            Esta consulta:
            - Filtra datas válidas para evitar erros
            - Mostra empresa, valor, descrição e entidade
            - Ordena por empresa e valor
            - Trata dados nulos adequadamente
            """
        
        if 'faturamento' in processed:
            processed = """
            
            IMPORTANTE: Use EXATAMENTE esta consulta:
            SELECT 
                EXTRACT(MONTH FROM pedi_data) as mes,
                EXTRACT(YEAR FROM pedi_data) as ano,
                SUM(pedi_tota) as faturamento
            FROM pedidos
            GROUP BY mes, ano
            ORDER BY ano, mes;
            """

        return processed
    
    def _has_critical_error(self, output: str) -> bool:
        """Verifica se a saída contém erros críticos"""
        error_indicators = [
            'column "',
            'does not exist',
            'relation "',
            'year -',
            'is out of range',
            'syntax error',
            'UndefinedColumn',
            'UndefinedTable'
        ]
        
        return any(indicator in output for indicator in error_indicators)
    
    def _classify_error(self, error_msg: str) -> str:
        """Classifica o tipo de erro"""
        for error_type, pattern in self.error_patterns.items():
            if re.search(pattern, error_msg, re.IGNORECASE):
                return error_type.replace('_', ' ').title()
        
        return "Erro Desconhecido"
    
    def _get_error_solutions(self, error_type: str, error_msg: str) -> str:
        """Retorna soluções específicas para cada tipo de erro"""
        solutions = {
            'Column Not Exist': """
            - Consulte o schema da tabela primeiro: `DESCRIBE nome_da_tabela`
            - Verifique se o nome da coluna está correto
            - Use `SELECT * FROM tabela LIMIT 10` para ver as colunas disponíveis
            """,
            'Table Not Exist': """
            - Verifique se o nome da tabela está correto
            - Use consulta para listar tabelas disponíveis
            - Confirme se você tem permissão para acessar a tabela
            """,
            'Date Range Error': """
            - Use formato de data padrão: YYYY-MM-DD
            - Verifique se as datas estão em um intervalo válido
            - Use funções de conversão: TO_DATE() ou CAST()
            """,
            'Syntax Error': """
            - Verifique a sintaxe SQL
            - Confirme se todas as aspas estão fechadas
            - Verifique se os nomes das tabelas/colunas estão corretos
            """
        }
        
        return solutions.get(error_type, "- Tente uma consulta mais simples\n- Consulte a documentação SQL")
    
    def _attempt_error_recovery(self, original_question: str, error_output: str) -> str:
        """Tenta recuperar automaticamente de erros comuns"""
        try:
            # Se erro de coluna inexistente, tentar sugerir colunas similares
            if 'column "' in error_output and 'does not exist' in error_output:
                return self._suggest_similar_columns(original_question, error_output)
            
            # Se erro de tabela inexistente, listar tabelas disponíveis
            if 'relation "' in error_output and 'does not exist' in error_output:
                return self._list_available_tables()
            
            # Se erro de data, sugerir formato correto e ignorar dados inválidos
            if 'year' in error_output and 'is out of range' in error_output:
                return self._suggest_date_format_with_filter(original_question)
                
        except Exception:
            pass
        
        return None
    
    def _suggest_similar_columns(self, question: str, error_output: str) -> str:
        """Sugere colunas similares quando uma coluna não existe"""
        # Extrair nome da coluna do erro
        match = re.search(r'column "([^"]+)" does not exist', error_output)
        if not match:
            return None
            
        missing_column = match.group(1)
        
        # Mapear colunas comuns com nomes corretos do banco
        column_suggestions = {
            # Entidades
            'id': 'enti_clie',
            'codigo': 'enti_clie',
            'id_cliente': 'enti_clie',
            'codigo_cliente': 'enti_clie',
            'nome': 'enti_nome',
            'nome_cliente': 'enti_nome', 
            'nome_entidade': 'enti_nome',
            'tipo': 'enti_tipo_enti',
            'tipo_entidade': 'enti_tipo_enti',
            'tipo_cliente': 'enti_tipo_enti',
            'endereco': 'enti_ende',
            'estado': 'enti_esta',
            'telefone': 'enti_tele',
            'celular': 'enti_celu',
            
            # Produtos
            'codigo_produto': 'prod_codi',
            'nome_produto': 'prod_nome',
            
            # Pedidos
            'numero_pedido': 'pedi_nume',
            'data_pedido': 'pedi_data',
            'data': 'pedi_data',
            'fornecedor_pedido': 'pedi_forn',
            'total_pedido': 'pedi_tota',
            'valor_total': 'pedi_tota',
            'valor': 'pedi_tota',
            'faturamento': 'pedi_tota',

            
            # Itens pedido
            'quantidade': 'iped_quan',
            'preco_unitario': 'iped_unit',
            'preco': 'iped_unit',
            'valor_item': 'iped_tota',
            
            # Títulos
            'valor_titulo': 'titu_valo',
            'vencimento': 'titu_venc'
        }
        
        suggestion = column_suggestions.get(missing_column.lower())
        if suggestion:
            # Determinar a tabela baseada no contexto
            table_context = ""
            if suggestion.startswith('enti_'):
                table_context = " (tabela: entidades)"
            elif suggestion.startswith('prod_'):
                table_context = " (tabela: produtos)"
            elif suggestion.startswith('pedi_'):
                table_context = " (tabela: pedidosvenda)"
            elif suggestion.startswith('iped_'):
                table_context = " (tabela: itenspedidovenda)"
            elif suggestion.startswith('titu_'):
                table_context = " (tabela: titulospagar/titulosreceber)"
                
            return f"""**🔧 Correção automática detectada:**

A coluna `{missing_column}` não existe, mas encontrei a coluna correta: `{suggestion}`{table_context}

**💡 Sugestão:** Use `{suggestion}` ao invés de `{missing_column}`.

**Exemplo de consulta correta:**
```sql
SELECT {suggestion}, COUNT(*) 
FROM entidades 
GROUP BY {suggestion} 
LIMIT 10;
```

**📋 Campos disponíveis na tabela entidades:**
- enti_clie (ID da entidade)
- enti_nome (Nome)
- enti_tipo_enti (Tipo: CL, FO, VE, TR, OU, AM)
- enti_ende (Endereço)
- enti_esta (Estado)
- enti_tele (Telefone)
- enti_celu (Celular)
"""
        
        # Se não encontrou sugestão específica, mostrar campos disponíveis
        return f"""**❌ Coluna `{missing_column}` não encontrada**

**📋 Campos disponíveis por tabela:**

**entidades:**
- enti_clie, enti_nome, enti_tipo_enti, enti_ende, enti_esta, enti_tele, enti_celu

**produtos:**
- prod_codi, prod_nome

**pedidosvenda:**
- pedi_nume, pedi_data, pedi_forn, pedi_tota

**itenspedidovenda:**
- iped_pedi, iped_prod, iped_quan, iped_unit, iped_tota

**💡 Use EXATAMENTE estes nomes de campos em suas consultas.**
"""
    
    def _list_available_tables(self) -> str:
        """Lista tabelas disponíveis quando uma tabela não é encontrada"""
        return """**📋 Tabelas disponíveis no sistema:**

🔹 **entidades** - Clientes, fornecedores, vendedores, transportadoras
   Campos: enti_clie, enti_nome, enti_tipo_enti, enti_ende, enti_esta, enti_tele, enti_celu

🔹 **produtos** - Cadastro de produtos
   Campos: prod_codi, prod_nome

🔹 **saldosprodutos** - Saldos e movimentações de produtos
   Campos: sapr_prod, sapr_sald

🔹 **pedidosvenda** - Pedidos de venda
   Campos: pedi_nume, pedi_data, pedi_forn, pedi_tota

🔹 **itenspedidovenda** - Itens dos pedidos de venda
   Campos: iped_pedi, iped_prod, iped_quan, iped_unit, iped_tota

🔹 **titulospagar** - Contas a pagar
   Campos: titu_id, titu_forn, titu_valo, titu_venc

🔹 **titulosreceber** - Contas a receber
   Campos: titu_id, titu_clie, titu_valo, titu_venc

**💡 Exemplo de consulta correta:**
```sql
SELECT enti_tipo_enti, COUNT(*) as quantidade 
FROM entidades 
GROUP BY enti_tipo_enti 
ORDER BY quantidade DESC;
```
"""
    
    def _suggest_date_format(self, question: str) -> str:
        """Sugere formato correto de data"""
        return """**📅 Erro de formato de data detectado:**

**Formatos corretos:**
- `'2024-01-15'` (YYYY-MM-DD)
- `TO_DATE('15/01/2024', 'DD/MM/YYYY')`
- `CAST('2024-01-15' AS DATE)`

**💡 Dica:** Use sempre anos entre 1900 e 2100 e formatos padrão ISO.

**Exemplo de consulta correta:**
```sql
SELECT * FROM pedidosvenda WHERE pedi_data >= '2024-01-01' LIMIT 10
```
"""

    def _suggest_date_format_with_filter(self, question: str) -> str:
        """Sugere formato correto de data e como filtrar dados inválidos"""
        
        # Se a pergunta é sobre aniversariantes
        if 'aniversar' in question.lower() or 'nascimento' in question.lower():
            return """**📅 Erro de data detectado - Consulta de Aniversariantes:**

**🔧 Solução específica para listar aniversariantes:**

```sql
SELECT 
    enti_nome as nome,
    enti_tipo_enti as tipo,
    CASE 
        WHEN enti_nasc IS NOT NULL AND EXTRACT(YEAR FROM enti_nasc) BETWEEN 1900 AND 2100 
        THEN TO_CHAR(enti_nasc, 'DD/MM') 
        ELSE 'Data inválida' 
    END as aniversario,
    CASE 
        WHEN enti_nasc IS NOT NULL AND EXTRACT(YEAR FROM enti_nasc) BETWEEN 1900 AND 2100 
        THEN EXTRACT(MONTH FROM enti_nasc)
        ELSE 99 
    END as mes_nascimento,
    COALESCE(enti_tele, enti_celu, 'Sem telefone') as contato
FROM entidades 
WHERE enti_tipo_enti IN ('CL', 'VE', 'FO')
ORDER BY mes_nascimento, EXTRACT(DAY FROM enti_nasc)
LIMIT 20;
```

**✅ Esta consulta:**
- Evita erros de data usando CASE WHEN e EXTRACT
- Mostra nome, tipo, data de aniversário e contato
- Filtra apenas clientes, vendedores e fornecedores
- Trata datas inválidas adequadamente
- Ordena por mês e dia do aniversário

**💡 Use sempre esta estrutura para consultas de aniversariantes!**
"""
        
        # Se a pergunta é sobre títulos a pagar
        if 'titulo' in question.lower() and 'pagar' in question.lower():
            return """**📅 Erro de data detectado - Consulta de Títulos a Pagar:**

**🔧 Solução específica para títulos a pagar por empresa:**

```sql
SELECT 
    e.empr_nome as empresa,
    t.titu_desc as descricao,
    t.titu_valo as valor,
    CASE 
        WHEN t.titu_venc IS NOT NULL AND EXTRACT(YEAR FROM t.titu_venc) BETWEEN 1900 AND 2100 
        THEN t.titu_venc::text 
        ELSE 'Data inválida' 
    END as vencimento,
    COALESCE(ent.enti_nome, 'Sem entidade') as entidade
FROM titulospagar t
JOIN empresas e ON t.titu_empr = e.empr_codi
LEFT JOIN entidades ent ON t.titu_forn = ent.enti_clie
WHERE t.titu_valo > 0
ORDER BY e.empr_nome, t.titu_valo DESC
LIMIT 10;
```

**✅ Esta consulta:**
- Evita erros de data usando CASE WHEN
- Mostra empresa, valor, descrição e entidade
- Filtra apenas títulos com valor > 0
- Trata datas inválidas como texto
- Ordena por empresa e valor

**💡 Use sempre esta estrutura para consultas com datas problemáticas!**
"""
        
        # Solução geral para outros casos
        return """**📅 Erro de data detectado - dados inválidos no banco:**

**🚨 Problema:** O banco contém registros com datas inválidas (anos negativos ou fora do range).

**🔧 Soluções:**

**1. Filtrar datas válidas:**
```sql
SELECT * FROM tabela 
WHERE data_campo >= '1900-01-01' 
AND data_campo <= '2100-12-31' 
LIMIT 10
```

**2. Usar CASE WHEN para tratar datas inválidas:**
```sql
SELECT *,
    CASE 
        WHEN EXTRACT(YEAR FROM data_campo) BETWEEN 1900 AND 2100 
        THEN data_campo::text 
        ELSE 'Data inválida' 
    END as data_tratada
FROM tabela 
LIMIT 10
```

**3. Usar COALESCE para tratar nulos:**
```sql
SELECT *, COALESCE(data_campo, '2000-01-01') as data_valida 
FROM tabela 
WHERE EXTRACT(YEAR FROM data_campo) BETWEEN 1900 AND 2100
LIMIT 10
```

**💡 Dica:** Sempre use filtros de data para evitar registros com dados corrompidos.
"""
    