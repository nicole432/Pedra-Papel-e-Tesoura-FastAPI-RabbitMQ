"""
estado.py — Estado global compartilhado entre FastAPI e Worker.
Importado pelos dois módulos para garantir que é o mesmo objeto em memória.
"""
from protocolo import ResultadoPartida

partidas: dict[str, ResultadoPartida] = {}
salas: dict[str, list[str]] = {}