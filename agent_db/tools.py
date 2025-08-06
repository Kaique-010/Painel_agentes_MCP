from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain.chat_models import init_chat_model
from langchain.tools import tool

class AgentTools:
    def __init__(self, db_uri: str):
        try:
            # Configurar banco com segurança e amostras 
            self.db = SQLDatabase.from_uri(
                db_uri,
                sample_rows_in_table_info=3,  # Mostrar 3 linhas de exemplo
                schema='public',
                include_tables=['produtos','pedidos','itenspedidovendas','cheques','saldosprodutos', 'entidades', 'titulospagar', 'titulosreceber', 'moviestoque']  # Tabelas específicas
            )
            
            # Inicializar o modelo LLM
            self.llm = init_chat_model("gemini-2.5-flash", model_provider="google_genai")
            
            # Prompt de sistema melhorado com conhecimento específico
            system_prompt = """
Você é um assistente SQL especializado em consultas de dados de um sistema ERP/Financeiro.

CONHECIMENTO DO BANCO:
- produtos: Cadastro de produtos (campos chave: prod_codi, prod_nome)
- saldosprodutos: Saldos e movimentações de produtos (campos chave: sapr_prod, sapr_sald)
- pedidos: Cadastro de pedidos (campos chave: pedi_nume, pedi_data, pedi_forn, pedi_tota)
- itenspedidovendas: Itens de pedidos de vendas (campos chave: iped_pedi, iped_prod, iped_quan, iped_unit, iped_tota)
- cheques: Cadastro de cheques (campos chave: cheq_codi, cheq_data, cheq_forn)
- entidades: Cadastro de clientes/fornecedores/vendedores/transportadoras (campos chave: enti_clie, enti_nome, enti_tipo_enti, enti_ende, enti_esta, enti_tele, enti_celu)
- As Entidades são distintas por tipo no campo enti_tipo_enti, onde tem os seguintes valores:
    - Cliente CL
    - Fornecedor FO
    - Vendedor VE
    - Transportadora TR
    - Outros OU
    - AMBOS AM
- titulospagar: Contas a pagar (campos chave: titu_id, titu_forn, titu_valo, titu_venc)
- titulosreceber: Contas a receber (campos chave: titu_id, titu_clie, titu_valo, titu_venc)

RELACIONAMENTOS IMPORTANTES:
- saldosprodutos.sapr_prod → produtos.prod_codi
- itenspedidovendas.iped_prod → produtos.prod_codi
- pedidos.pedi_forn → entidades.enti_clie
- itenspedidovendas.iped_pedi → pedidos.pedi_nume
- titulosreceber.titu_clie → entidades.enti_clie

REGRAS IMPORTANTES:
- APENAS execute comandos SELECT para consultar dados
- NUNCA execute CREATE, DROP, ALTER, INSERT, UPDATE ou DELETE
- Use JOINS quando necessário para relacionar tabelas
- Sempre forneça insights e análises dos dados
- Responda em português brasileiro com linguagem natural
- Se não encontrar dados, explique possíveis motivos
- Sugira consultas relacionadas quando apropriado

FORMATAÇÃO DAS RESPOSTAS:
- Use markdown para melhor formatação
- Inclua insights e análises dos dados
- Destaque números importantes com **negrito**
- Use listas para organizar informações
- Sempre termine com sugestões de próximas consultas
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
        except Exception as e:
            print(f'❌ Erro ao inicializar AgentTools: {e}')
            raise

    def query_database(self, question: str) -> str:
        """Executa uma consulta SQL no banco de dados."""
        try:
            # Adicionar contexto para respostas em linguagem natural
            enhanced_question = f"""
            {question}
            
            INSTRUÇÕES ADICIONAIS:
            - Responda em linguagem natural com insights sobre os dados
            - Use formatação markdown para melhor apresentação
            - Se não encontrar dados, explique possíveis motivos
            - Inclua análises e interpretações dos resultados
            - Termine com sugestões de consultas relacionadas
            """
            
            result = self.sql_agent.invoke({"input": enhanced_question})
            return result.get("output", str(result))
        except Exception as e:
            return f"**Erro na consulta:** {str(e)}\n\n**Possíveis causas:**\n- Tabela pode estar vazia\n- Problemas de conectividade\n- Consulta muito complexa\n\n**Sugestões:**\n- Tente uma consulta mais simples\n- Verifique se as tabelas existem\n- Consulte a estrutura das tabelas primeiro"
    
    def get_table_info(self, table_name: str) -> str:
        """Retorna as informações de uma tabela específica."""
        try:
            return self.db.get_table_info([table_name])
        except Exception as e:
            return f"Erro ao obter informações da tabela: {str(e)}"
    
    def get_database_schema(self) -> str:
        """Retorna informações sobre todas as tabelas configuradas."""
        try:
            tables = ['produtos', 'saldosprodutos', 'entidades', 'titulospagar', 'titulosreceber']
            schema_info = "## Estrutura do Banco de Dados\n\n"
            
            for table in tables:
                try:
                    info = self.db.get_table_info([table])
                    schema_info += f"### Tabela: {table}\n```sql\n{info}\n```\n\n"
                except:
                    schema_info += f"### Tabela: {table}\n*Tabela não encontrada ou sem permissão*\n\n"
            
            return schema_info
        except Exception as e:
            return f"Erro ao obter schema do banco: {str(e)}"
    