from functools import cache
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Carregar variáveis de ambiente
load_dotenv()

class config:
    #postgres
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT')
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_DB = os.getenv('POSTGRES_DB')
    
    CACHE_TTL_DAYS = int(os.getenv('CACHE_TTL_DAYS', '7'))
    
    @classmethod
    def get_database_url(cls):
        """Retorna a URL do banco com caracteres especiais codificados"""
        password_encoded = quote_plus(cls.POSTGRES_PASSWORD)
        return f"postgresql://{cls.POSTGRES_USER}:{password_encoded}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"




