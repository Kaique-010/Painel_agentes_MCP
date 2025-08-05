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
import json

templates = Jinja2Templates(directory="templates")

agent_executor = None
config = {'configurable': {'thread_id': '1'}}

class perguntaInput(BaseModel):
    pergunta: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    global agent_executor
    try:
        print("🚀 Inicializando agente...")
        memoria = MemorySaver()
        model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")
        mcp_client = MultiServerMCPClient(MCP_SERVERS_CONFIG)
        tools = await mcp_client.get_tools()
        agent_executor = create_react_agent(
            model=model,
            tools=tools,
            system_prompt=AGENT_SYSTEM_PROMPT,
            memory=memoria,
        )
        print("✅ Agente pronto com tools:", [t.name for t in tools])
    except Exception as e:
        print(f"❌ Erro ao inicializar agente: {e}")
        agent_executor = None

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
               
