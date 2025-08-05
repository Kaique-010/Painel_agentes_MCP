import asyncio
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent
from prompts import AGENT_SYSTEM_PROMPT
from mcp_serves import MCP_SERVERS_CONFIG
from langchain.chat_models import init_chat_model

async def main():
    
    memoria = MemorySaver()
    print('Memória criada:', memoria)
    model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")
    mcp_client = MultiServerMCPClient(MCP_SERVERS_CONFIG)
    tools = await mcp_client.get_tools()
    
    agent_executor = create_react_agent(
        model=model,
        tools=tools,
        system_prompt=AGENT_SYSTEM_PROMPT,
        memory=memoria,
    )
    print('Agent executor:', agent_executor)
    print('Tools:', [tool.name for tool in tools])
    
    config = {'configurable': {'thread_id': '1'}}
    
    print("\n=== Agente de Pesquisa Iniciado ===")
    print("Digite 'sair' para encerrar")
    print("=" * 40)

    while True:
        try:
            user_input = input('\nDigite: ').strip()
            
            if user_input.lower() in ['sair', 'quit', 'exit']:
                print("Encerrando...")
                break
                
            if not user_input:
                print("Por favor, digite uma pergunta.")
                continue
            
            input_message = {
                'role': 'user',
                'content': user_input,
            }
            
            print("\nProcessando...")
            
            async for step in agent_executor.astream(
                {'messages': [input_message]}, 
                config, 
                stream_mode='values'
            ):
                # Verificar se o step contém messages antes de tentar acessar
                if 'messages' in step and step['messages']:
                    step['messages'][-1].pretty_print()
                    
        except KeyboardInterrupt:
            print("\n\nEncerrando...")
            break
        except Exception as e:
            print(f"\nErro: {e}")
            print("Tente novamente ou digite 'sair' para encerrar.")
            continue

if __name__ == "__main__":
    asyncio.run(main())
