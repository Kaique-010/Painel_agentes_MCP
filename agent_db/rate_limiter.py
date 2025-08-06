# -*- coding: utf-8 -*-
import time
import threading
from collections import defaultdict, deque
from typing import Dict, Optional

class RateLimiter:
    """
    Rate limiter para controlar requisições por segundo/minuto
    """
    def __init__(self, max_requests_per_second: int = 2, max_requests_per_minute: int = 30):
        self.max_requests_per_second = max_requests_per_second
        self.max_requests_per_minute = max_requests_per_minute
        self.requests_per_second = deque()
        self.requests_per_minute = deque()
        self.lock = threading.Lock()
        
    def can_proceed(self) -> bool:
        """
        Verifica se pode prosseguir com a requisição
        """
        with self.lock:
            current_time = time.time()
            
            # Limpar requisições antigas (mais de 1 segundo)
            while self.requests_per_second and current_time - self.requests_per_second[0] > 1:
                self.requests_per_second.popleft()
                
            # Limpar requisições antigas (mais de 1 minuto)
            while self.requests_per_minute and current_time - self.requests_per_minute[0] > 60:
                self.requests_per_minute.popleft()
            
            # Verificar limites
            if len(self.requests_per_second) >= self.max_requests_per_second:
                return False
                
            if len(self.requests_per_minute) >= self.max_requests_per_minute:
                return False
                
            # Registrar a requisição
            self.requests_per_second.append(current_time)
            self.requests_per_minute.append(current_time)
            
            return True
    
    def wait_time(self) -> float:
        """
        Retorna o tempo de espera necessário em segundos
        """
        with self.lock:
            current_time = time.time()
            
            # Verificar tempo de espera para limite por segundo
            if self.requests_per_second:
                oldest_request = self.requests_per_second[0]
                if len(self.requests_per_second) >= self.max_requests_per_second:
                    return max(0, 1 - (current_time - oldest_request))
            
            # Verificar tempo de espera para limite por minuto
            if self.requests_per_minute:
                oldest_request = self.requests_per_minute[0]
                if len(self.requests_per_minute) >= self.max_requests_per_minute:
                    return max(0, 60 - (current_time - oldest_request))
                    
            return 0

class SmartCache:
    """
    Cache inteligente com TTL e invalidação automática
    """
    def __init__(self, default_ttl: int = 300):  # 5 minutos
        self.cache = {}
        self.timestamps = {}
        self.access_count = defaultdict(int)
        self.default_ttl = default_ttl
        self.lock = threading.Lock()
        
    def get(self, key: str, ttl: Optional[int] = None) -> Optional[str]:
        """
        Recupera item do cache se ainda válido
        """
        with self.lock:
            if key not in self.cache:
                return None
                
            # Verificar TTL
            ttl_to_use = ttl or self.default_ttl
            if time.time() - self.timestamps[key] > ttl_to_use:
                # Cache expirado
                del self.cache[key]
                del self.timestamps[key]
                if key in self.access_count:
                    del self.access_count[key]
                return None
                
            # Incrementar contador de acesso
            self.access_count[key] += 1
            return self.cache[key]
    
    def set(self, key: str, value: str, ttl: Optional[int] = None):
        """
        Armazena item no cache
        """
        with self.lock:
            self.cache[key] = value
            self.timestamps[key] = time.time()
            self.access_count[key] = 0
            
            # Limpar cache se muito grande (manter apenas os 1000 mais recentes)
            if len(self.cache) > 1000:
                self._cleanup_cache()
    
    def _cleanup_cache(self):
        """
        Remove itens mais antigos do cache
        """
        # Ordenar por timestamp e manter apenas os 800 mais recentes
        sorted_items = sorted(self.timestamps.items(), key=lambda x: x[1], reverse=True)
        keys_to_keep = [item[0] for item in sorted_items[:800]]
        
        # Remover itens antigos
        keys_to_remove = set(self.cache.keys()) - set(keys_to_keep)
        for key in keys_to_remove:
            if key in self.cache:
                del self.cache[key]
            if key in self.timestamps:
                del self.timestamps[key]
            if key in self.access_count:
                del self.access_count[key]
    
    def invalidate(self, pattern: str = None):
        """
        Invalida cache por padrão ou limpa tudo
        """
        with self.lock:
            if pattern:
                keys_to_remove = [key for key in self.cache.keys() if pattern in key]
                for key in keys_to_remove:
                    if key in self.cache:
                        del self.cache[key]
                    if key in self.timestamps:
                        del self.timestamps[key]
                    if key in self.access_count:
                        del self.access_count[key]
            else:
                self.cache.clear()
                self.timestamps.clear()
                self.access_count.clear()
    
    def stats(self) -> Dict:
        """
        Retorna estatísticas do cache
        """
        with self.lock:
            return {
                'total_items': len(self.cache),
                'most_accessed': max(self.access_count.items(), key=lambda x: x[1]) if self.access_count else None,
                'cache_size_mb': sum(len(str(v)) for v in self.cache.values()) / (1024 * 1024)
            }