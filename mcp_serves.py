import os

from dotenv import load_dotenv


load_dotenv()

SMITHERY_API_KEY = os.getenv('SMITHERY_API_KEY')
MCP_SERVERS_CONFIG = {
    'sequentialthinking-tools': {
        'url': f'https://server.smithery.ai/@xinzhongyouhai/mcp-sequentialthinking-tools/mcp?api_key={SMITHERY_API_KEY}&profile=fun-grasshopper-kwJEdG',
        'transport': 'streamable_http',
    },
    'buscas': {
        'url': f'https://server.smithery.ai/exa/mcp?api_key=5aa32046-6caf-4f75-b2c2-a403b4c0c886&profile=liable-rhinoceros-zBrJHu',
        'transport': 'streamable_http',
    },
    'contexto': {
        'url': f'https://server.smithery.ai/@upstash/context7-mcp/mcp?api_key=5aa32046-6caf-4f75-b2c2-a403b4c0c886&profile=liable-rhinoceros-zBrJHu',
        'transport': 'streamable_http',
    },

}
