import uuid
import json
import pika
import redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from logger import configurar_logger, obter_logger
from protocolo import JogadaMessage, ResultadoPartida
from estado import partidas, salas

configurar_logger()
log = obter_logger("FastAPI")

app = FastAPI(title="Pedra Papel Tesoura — API REST + RabbitMQ + Redis")

cache = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
CACHE_TTL = 60  # segundos que o resultado fica no cache


def cache_salvar_resultado(partida_id: str, resultado: dict):
    """Salva resultado da partida no Redis com TTL de 60s."""
    try:
        cache.setex(f"resultado:{partida_id}", CACHE_TTL, json.dumps(resultado))
        log.info(f"[Redis] Resultado salvo no cache: partida={partida_id}")
    except Exception as e:
        log.warning(f"[Redis] Erro ao salvar cache: {e}")


def cache_obter_resultado(partida_id: str) -> dict | None:
    """Busca resultado do Redis. Retorna None se não existir."""
    try:
        valor = cache.get(f"resultado:{partida_id}")
        if valor:
            log.info(f"[Redis] Cache HIT: partida={partida_id}")
            return json.loads(valor)
        log.info(f"[Redis] Cache MISS: partida={partida_id}")
        return None
    except Exception as e:
        log.warning(f"[Redis] Erro ao buscar cache: {e}")
        return None



@app.on_event("startup")
def startup_event():
    from worker import iniciar_worker_background
    iniciar_worker_background(partidas, salas, cache)
    try:
        cache.ping()
        log.info("[Redis] Conexão estabelecida com sucesso.")
    except Exception:
        log.warning("[Redis] Redis indisponível — cache desativado.")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RABBITMQ_URL = "amqp://guest:guest@localhost/"
FILA_JOGADAS = "jogadas"


def publicar_jogada(msg: JogadaMessage):
    try:
        conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        canal = conn.channel()
        canal.queue_declare(queue=FILA_JOGADAS, durable=True)
        canal.basic_publish(
            exchange="",
            routing_key=FILA_JOGADAS,
            body=msg.codificar(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        conn.close()
        log.info(f"[RabbitMQ] Jogada publicada: player={msg.player_id} escolha={msg.escolha}")
    except Exception as e:
        log.error(f"[RabbitMQ] Erro ao publicar: {e}")
        raise HTTPException(status_code=503, detail="Fila indisponível. RabbitMQ offline?")



class EntrarPayload(BaseModel):
    sala_id: str | None = None


class JogadaPayload(BaseModel):
    player_id: str
    partida_id: str
    escolha: str


@app.get("/")
def raiz():
    return FileResponse("frontend/index.html")


@app.post("/entrar")
def entrar(payload: EntrarPayload):
    player_id = str(uuid.uuid4())[:8]

    if not payload.sala_id:
        sala_id    = str(uuid.uuid4())[:6].upper()
        partida_id = str(uuid.uuid4())[:8]
        salas[sala_id] = {"jogadores": [player_id], "partida_id": partida_id}
        partidas[partida_id] = ResultadoPartida(partida_id=partida_id, jogadas={})
        log.info(f"Sala criada: sala={sala_id} partida={partida_id} j1={player_id}")
        return {"player_id": player_id, "sala_id": sala_id, "partida_id": partida_id, "posicao": 1, "mensagem": "Sala criada! Aguardando Jogador 2..."}

    sala_id = payload.sala_id.upper()
    if sala_id not in salas:
        raise HTTPException(status_code=404, detail="Sala não encontrada.")
    if len(salas[sala_id]["jogadores"]) >= 2:
        raise HTTPException(status_code=409, detail="Sala cheia.")

    salas[sala_id]["jogadores"].append(player_id)
    partida_id = salas[sala_id]["partida_id"]
    log.info(f"Jogador2 entrou: sala={sala_id} partida={partida_id} j2={player_id}")
    return {"player_id": player_id, "sala_id": sala_id, "partida_id": partida_id, "posicao": 2, "mensagem": "Você entrou na sala! Boa sorte!"}


@app.get("/sala/{sala_id}")
def status_sala(sala_id: str):
    sala_id = sala_id.upper()
    if sala_id not in salas:
        raise HTTPException(status_code=404, detail="Sala não encontrada.")
    return {"sala_id": sala_id, "jogadores": len(salas[sala_id]["jogadores"]), "partida_id": salas[sala_id]["partida_id"]}


@app.post("/jogada")
def fazer_jogada(payload: JogadaPayload):
    if payload.escolha not in ("pedra", "papel", "tesoura"):
        raise HTTPException(status_code=400, detail="Escolha inválida.")
    if payload.partida_id not in partidas:
        raise HTTPException(status_code=404, detail="Partida não encontrada.")

    partida = partidas[payload.partida_id]
    if partida.encerrada:
        raise HTTPException(status_code=409, detail="Partida já encerrada.")
    if payload.player_id in partida.jogadas:
        raise HTTPException(status_code=409, detail="Você já jogou nesta rodada.")

    msg = JogadaMessage(partida_id=payload.partida_id, player_id=payload.player_id, escolha=payload.escolha)
    publicar_jogada(msg)
    return {"status": "jogada_enviada", "mensagem": "Jogada enviada! Aguardando o oponente..."}


@app.get("/resultado/{partida_id}")
def obter_resultado(partida_id: str):
    """
    1º tenta buscar do cache Redis (rápido)
    2º se não tiver no cache, busca da memória
    3º se a partida estiver encerrada, salva no cache para as próximas consultas
    """

    cached = cache_obter_resultado(partida_id)
    if cached:
        return cached

    if partida_id not in partidas:
        raise HTTPException(status_code=404, detail="Partida não encontrada.")

    resultado = partidas[partida_id].to_dict()

    # salva no cache
    if resultado["encerrada"]:
        cache_salvar_resultado(partida_id, resultado)

    return resultado


@app.post("/sair")
def sair(payload: dict):
    """Remove jogador da sala quando ele clica em Sair."""
    player_id = payload.get("player_id")
    sala_id   = (payload.get("sala_id") or "").upper()

    if sala_id in salas:
        jogadores = salas[sala_id]["jogadores"]
        if player_id in jogadores:
            jogadores.remove(player_id)
            log.info(f"Jogador {player_id} saiu da sala {sala_id}. Restam {len(jogadores)} jogador(es).")

    return {"status": "ok"}


@app.post("/nova-partida/{sala_id}")
def nova_partida(sala_id: str):
    sala_id = sala_id.upper()
    if sala_id not in salas:
        raise HTTPException(status_code=404, detail="Sala não encontrada.")

    partida_id = str(uuid.uuid4())[:8]
    partidas[partida_id] = ResultadoPartida(partida_id=partida_id, jogadas={})
    salas[sala_id]["partida_id"] = partida_id

    encerradas = [pid for pid, p in list(partidas.items()) if p.encerrada and pid != partida_id]
    for pid in encerradas:
        del partidas[pid]

    log.info(f"Nova partida: sala={sala_id} partida={partida_id}")
    return {"partida_id": partida_id, "mensagem": "Nova partida iniciada!"}


@app.get("/placar/{sala_id}")
def obter_placar(sala_id: str):
    """Retorna histórico de rodadas e placar da sala, lido do Redis."""
    sala_id = sala_id.upper()
    try:
        dados_raw = cache.get(f"placar:{sala_id}")
        if not dados_raw:
            return {"rodadas": [], "pontos": {}}
        return json.loads(dados_raw)
    except Exception as e:
        log.warning(f"[Redis] Erro ao buscar placar: {e}")
        return {"rodadas": [], "pontos": {}}


app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")