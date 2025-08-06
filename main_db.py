from agent_db.core import AgentDB

if __name__ == '__main__':
    agent_db = AgentDB()
    pergunta = "Quantos produtos temos cadastrados?"

    resposta = agent_db.run(pergunta)
    print(resposta)

    