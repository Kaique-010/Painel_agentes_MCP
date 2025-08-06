import asyncio
from fastapi.responses import StreamingResponse
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent
from prompts import AGENT_SYSTEM_PROMPT
from mcp_serves import MCP_SERVERS_CONFIG
from langchain.chat_models import init_chat_model
from agent_db.core import AgentDB
import json
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

print("POSTGRES_HOST:", os.getenv("POSTGRES_HOST"))
print("POSTGRES_PORT:", os.getenv("POSTGRES_PORT"))
print("POSTGRES_USER:", os.getenv("POSTGRES_USER"))
print("POSTGRES_PASSWORD:", os.getenv("POSTGRES_PASSWORD"))
print("POSTGRES_DB:", os.getenv("POSTGRES_DB"))
print("MCP_SERVERS_CONFIG:", MCP_SERVERS_CONFIG)


templates = Jinja2Templates(directory="templates")

agent_executor = None
agent_db = None
config = {'configurable': {'thread_id': '1'}}

class perguntaInput(BaseModel):
    pergunta: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    global agent_executor, agent_db
    print("🚀 Inicializando agentes...")
    
    # Inicializar agente MCP
    try:
        print("🔄 Inicializando agente MCP...")
        memoria = MemorySaver()
        model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")
        print("✅ Modelo LLM inicializado")
        
        mcp_client = MultiServerMCPClient(MCP_SERVERS_CONFIG)
        print("✅ MCP Client criado")
        
        tools = await mcp_client.get_tools()
        print(f"✅ Tools obtidas: {len(tools)} ferramentas")
        
        agent_executor = create_react_agent(
            model=model,
            tools=tools,
            system_prompt=AGENT_SYSTEM_PROMPT,
            memory=memoria,
        )
        print("✅ Agente MCP pronto com tools:", [t.name for t in tools])
        
    except Exception as mcp_error:
        print(f"❌ Erro ao inicializar agente MCP: {mcp_error}")
        print(f"❌ Tipo do erro MCP: {type(mcp_error).__name__}")
        print("⚠️ Continuando sem o agente MCP...")
        agent_executor = None
    
    # Inicializar agente de banco de dados
    try:
        print("🔄 Inicializando agente de banco de dados...")
        agent_db = AgentDB()
        print("✅ Agente de Banco de Dados inicializado")
    except Exception as db_error:
        print(f"❌ Erro ao inicializar agente de banco: {db_error}")
        print(f"❌ Tipo do erro DB: {type(db_error).__name__}")
        print("⚠️ Continuando sem o agente de banco de dados...")
        agent_db = None

    yield  

    print("🛑 Finalizando aplicação")

# Criar o app com lifespan
app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_index(request: Request):
    print("Template index renderizado com Sucesso")
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/agente1")
async def read_agente1(request: Request):
    print("Template agente1 renderizado com Sucesso")
    return templates.TemplateResponse("agente1.html", {"request": request})

@app.get("/agente2")
async def read_agente2(request: Request):
    print("Template agente2 renderizado com Sucesso")
    return templates.TemplateResponse("agente2.html", {"request": request})

@app.get("/agente_db")
async def read_agente_db(request: Request):
    print("Template agente_db renderizado com Sucesso")
    return templates.TemplateResponse("agente_db.html", {"request": request})

@app.post("/pergunta")
async def fazer_pergunta(pergunta: perguntaInput):
    global agent_executor
    
    if agent_executor is None:
        async def error_generator():
            yield "data: Erro: Agente não foi inicializado corretamente\n\n"
            yield "data: [DONE]\n\n"
        return StreamingResponse(error_generator(), media_type="text/event-stream")
    
    pergunta_texto = pergunta.pergunta
    print(f"📝 Pergunta recebida: {pergunta_texto}")
    
    async def response_generator():
        try:
            async for step in agent_executor.astream(
                {'messages': [{'role': 'user', 'content': pergunta_texto}]},
                config,
                stream_mode='values'
            ):
                if 'messages' in step and step['messages']:
                    msg = step['messages'][-1]
                    resposta_final = getattr(msg, "content", str(msg))
                    
                    if resposta_final and resposta_final.strip():
                        print(f'📤 Enviando: {resposta_final[:100]}...')
                        
                        # Limpar e formatar a resposta
                        resposta_limpa = str(resposta_final).strip()
                        yield f"data: {resposta_limpa}\n\n"
                        
                        await asyncio.sleep(0.1)
            
            # Sinalizar fim da resposta
            yield "data: [DONE]\n\n"
            print("✅ Resposta completa enviada")
            
        except Exception as e:
            print(f"❌ Erro durante processamento: {e}")
            yield f"data: Erro: {str(e)}\n\n"
            yield "data: [DONE]\n\n"
    
    return StreamingResponse(
        response_generator(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )

@app.post("/pergunta_db")
async def fazer_pergunta_db(pergunta: perguntaInput):
    global agent_db
    
    async def generate():
        try:
            if not agent_db:
                error_chunk = {
                    "type": "error",
                    "content": "Agente de banco de dados não está disponível. Verifique se o PostgreSQL está rodando e as configurações estão corretas."
                }
                yield f"data: {json.dumps(error_chunk)}\n\n"
                return
            
            # Executar o workflow do agente de banco de dados
            result = agent_db.run(pergunta.pergunta)
            
            # Função para criar streaming mais natural
            def create_natural_chunks(text):
                """Divide o texto em chunks naturais para streaming"""
                import re
                
                # Dividir por sentenças, mantendo pontuação
                sentences = re.split(r'(?<=[.!?])\s+', text)
                chunks = []
                
                for sentence in sentences:
                    if len(sentence) > 100:
                        # Para sentenças muito longas, dividir por vírgulas ou outros delimitadores
                        sub_parts = re.split(r'(?<=[,;:])\s+', sentence)
                        for part in sub_parts:
                            if len(part) > 50:
                                # Para partes ainda muito longas, dividir por palavras
                                words = part.split(' ')
                                current_chunk = ""
                                for word in words:
                                    if len(current_chunk + word) < 30:
                                        current_chunk += word + " "
                                    else:
                                        if current_chunk.strip():
                                            chunks.append(current_chunk.strip())
                                        current_chunk = word + " "
                                if current_chunk.strip():
                                    chunks.append(current_chunk.strip())
                            else:
                                chunks.append(part)
                    else:
                        chunks.append(sentence)
                
                return [chunk.strip() for chunk in chunks if chunk.strip()]
            
            # Criar chunks naturais
            chunks = create_natural_chunks(result)
            
            # Enviar chunks com timing variável para simular escrita humana
            for i, chunk in enumerate(chunks):
                # Calcular delay baseado no tamanho do chunk
                delay = min(0.05 + len(chunk) * 0.01, 0.3)
                
                chunk_data = {
                    "type": "content",
                    "content": chunk,
                    "is_complete": False
                }
                yield f"data: {json.dumps(chunk_data)}\n\n"
                await asyncio.sleep(delay)
            
            # Enviar sinal de fim
            yield f"data: {json.dumps({'type': 'end', 'is_complete': True})}\n\n"
            
        except Exception as e:
            error_chunk = {
                "type": "error",
                "content": f"Erro: {str(e)}"
            }
            yield f"data: {json.dumps(error_chunk)}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
               
