import hashlib
import json
from datetime import datetime, timedelta
import psycopg2
from config_db import config


class CacheManager:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                dbname=config.POSTGRES_DB
            )
            self._init_db()
            print('✅ Cache Manager inicializado com sucesso')
        except Exception as e:
            print(f'❌ Erro ao conectar ao banco: {e}')
            raise
    
    def _init_db(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_agente (
                    cach_id SERIAL PRIMARY KEY,
                    cach_hash TEXT UNIQUE NOT NULL,
                    cach_text TEXT,
                    cach_resp TEXT,    
                    cach_crea_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    cach_upda_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    cach_expi_at TIMESTAMP
                )
                """
            )
            self.conn.commit()  


    def get_query_cache(self, query: str) -> str:
        return hashlib.sha256(query.encode()).hexdigest()
    
    def get_cache(self, query_hash: str) -> str:
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT cach_resp FROM cache_agente 
                    WHERE cach_hash = %s AND cach_expi_at > %s
                    """,
                    (query_hash, datetime.now())
                )
                result = cursor.fetchone()
                if result:
                    return result[0]
                return None
        except Exception as e:
            print(f'❌ Erro ao buscar cache: {e}')
            return None
      
    def set_cache(self, query_hash: str, resp: str):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO cache_agente (cach_hash, cach_resp, cach_expi_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (cach_hash) DO UPDATE SET
                        cach_resp = EXCLUDED.cach_resp,
                        cach_upda_at = CURRENT_TIMESTAMP,
                        cach_expi_at = EXCLUDED.cach_expi_at
                    """,
                    (query_hash, resp, datetime.now() + timedelta(days=config.CACHE_TTL_DAYS))
                )
                self.conn.commit()
        except Exception as e:
            print(f'❌ Erro ao salvar cache: {e}')
    
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()


