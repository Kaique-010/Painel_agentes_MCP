# -*- coding: utf-8 -*-
import os
import sys

# Configurar codificação UTF-8 explicitamente
if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'

import hashlib
import json
from datetime import datetime, timedelta
import psycopg2
from config_db import config
import time


class CacheManager:
    def __init__(self):
        # Conectar ao banco PostgreSQL
        try:
            self.connection = psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                database=config.POSTGRES_DB
            )
            self._init_db()
            print('✅ Cache Manager inicializado com sucesso')
        except Exception as e:
            print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
            raise
    
    def _init_db(self):
        """Verifica se a tabela de cache existe (usa estrutura existente)"""
        cursor = self.connection.cursor()
        
        # Verifica se a tabela existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'cache_agente'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("✅ Tabela cache_agente encontrada (usando estrutura existente)")
        else:
            print("⚠️ Tabela cache_agente não encontrada")
        
        cursor.close()
        print("✅ Cache table initialized successfully")



    def get_query_cache(self, query: str) -> str:
        return hashlib.sha256(query.encode()).hexdigest()
    
    def get(self, query_hash: str):
        """Recupera uma resposta do cache se existir e não expirou"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                SELECT cach_resp FROM cache_agente 
                WHERE cach_hash = %s AND cach_expi_at > NOW()
                """, 
                (query_hash,)
            )
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            cursor.close()
    
    def cleanup_expired(self):
        """Remove entradas expiradas do cache"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "DELETE FROM cache_agente WHERE cach_expi_at < NOW()"
            )
            deleted_count = cursor.rowcount
            self.connection.commit()
            if deleted_count > 0:
                print(f"🧹 Cache: {deleted_count} entradas expiradas removidas")
        except Exception as e:
            print(f"❌ Erro ao limpar cache: {e}")
        finally:
            cursor.close()
    
    def get_stats(self):
        """Retorna estatísticas do cache"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_entries,
                    COUNT(*) FILTER (WHERE cach_expi_at > NOW()) as active_entries,
                    COUNT(*) FILTER (WHERE cach_expi_at <= NOW()) as expired_entries
                FROM cache_agente
            """)
            result = cursor.fetchone()
            return {
                'total_entries': result[0],
                'active_entries': result[1], 
                'expired_entries': result[2]
            }
        except Exception as e:
            print(f"❌ Erro ao obter estatísticas do cache: {e}")
            return {'total_entries': 0, 'active_entries': 0, 'expired_entries': 0}
        finally:
            cursor.close()
    
    def set(self, query_hash: str, query_text: str, response: str):
        """Salva uma resposta no cache com expiração"""
        expiry_date = datetime.now() + timedelta(days=config.CACHE_TTL_DAYS)
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO cache_agente (cach_hash, cach_text, cach_resp, cach_expi_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (cach_hash) 
                DO UPDATE SET 
                    cach_resp = EXCLUDED.cach_resp,
                    cach_expi_at = EXCLUDED.cach_expi_at,
                    cach_upda_at = NOW()
                """,
                (query_hash, query_text, response, expiry_date)
            )
            self.connection.commit()
        finally:
            cursor.close()
    
    def __del__(self):
        if hasattr(self, 'connection'):
            self.connection.close()


