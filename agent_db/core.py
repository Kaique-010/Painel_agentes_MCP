# -*- coding: utf-8 -*-
import os
import sys

# Configurar codificação UTF-8 explicitamente
if sys.platform.startswith('win'):
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from typing import Annotated, TypedDict
from .cache.manager import CacheManager
from .tools import AgentTools
from config_db import config

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    pergunta: str
    resposta: str
    cache_hit: bool

class AgentDB:
    def __init__(self):
        self.cache_manager = CacheManager()
        # Limpar cache expirado na inicialização
        self.cache_manager.cleanup_expired()
        
        # Inicializar AgentTools
        db_uri = config.get_database_url()
        self.agent_tools = AgentTools(db_uri)
        self.workflow = self._build_workflow()
        
        # Estatísticas do cache
        stats = self.cache_manager.get_stats()
        print(f"📊 Cache: {stats['active_entries']} entradas ativas, {stats['expired_entries']} expiradas")
    
    def _build_workflow(self):
        # Cria o grafo de estado
        workflow = StateGraph(AgentState)
        
        # Adiciona os nós
        workflow.add_node("checa_cache", self._checa_cache)
        workflow.add_node("processa_pergunta", self._process_query)
        workflow.add_node("salva_cache", self._salva_cache)
        
        # Define as conexões
        workflow.add_edge(START, "checa_cache")
        workflow.add_conditional_edges(
            "checa_cache",
            self._route_query,
            {"cache_hit": END, "process": "processa_pergunta"}
        )
        workflow.add_edge("processa_pergunta", "salva_cache")
        workflow.add_edge("salva_cache", END)
        
        print("Workflow construído com sucesso!")
        print(workflow)
        print("Nós:", workflow.nodes)
        print("Arestas:", workflow.edges)
        print("Condições:", workflow.add_conditional_edges)

                
        return workflow.compile()

    def _checa_cache(self, state: AgentState) -> AgentState:
        pergunta = state["pergunta"]
        query_hash = self.cache_manager.get_query_cache(pergunta)
        cache = self.cache_manager.get(query_hash)
        
        if cache:
            state["resposta"] = cache
            state["cache_hit"] = True
        else:
            state["cache_hit"] = False
            
        return state
    
    def _route_query(self, state: AgentState) -> str:
        return "cache_hit" if state["cache_hit"] else "process"
    
    def _process_query(self, state: AgentState) -> AgentState:
        pergunta = state["pergunta"]
        resposta = self.agent_tools.query_database(pergunta)
        state["resposta"] = resposta
        return state
    
    def _salva_cache(self, state: AgentState) -> AgentState:
        if not state["cache_hit"]:
            pergunta = state["pergunta"]
            resposta = state["resposta"]

            # ⛔ Se a resposta for vazia, None ou inválida, não salva o cache
            if not resposta or (isinstance(resposta, dict) and not resposta.get("dados")):
                print("❌ Resposta vazia ou inválida. Cache não salvo.")
                return state
            
            query_hash = self.cache_manager.get_query_cache(pergunta)
            self.cache_manager.set(query_hash, pergunta, resposta)
            print("✅ Cache salvo com sucesso.")
        
        return state

    
    def run(self, pergunta: str) -> str:
        initial_state = {
            "messages": [],
            "pergunta": pergunta,
            "resposta": "",
            "cache_hit": False
        }
        
        final_state = self.workflow.invoke(initial_state)
        return final_state["resposta"]








