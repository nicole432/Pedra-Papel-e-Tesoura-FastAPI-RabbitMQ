import uuid
import pika
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from logger import configurar_logger, obter_logger
from protocolo import JogadaMessage, ResultadoPartida

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
configurar_logger()
log = obter_logger("FastAPI")

app = FastAPI(title="Pedra Papel Tesoura — API REST + RabbitMQ")


@app.on_event("startup")
def startup_event():
    """Inicia o worker RabbitMQ em background thread junto com o servidor."""
    from worker import iniciar_worker_background
    iniciar_worker_background(partidas)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RABBITMQ_URL = "amqp://guest:guest@localhost/"
FILA_JOGADAS = "jogadas"

# ---------------------------------------------------------------------------
# Estado global compartilhado entre FastAPI e Worker (mesmo processo)
# Em produção real usaria Redis ou banco de dados.
# ---------------------------------------------------------------------------
partidas: dict[str, ResultadoPartida] = {}
salas: dict[str, list[str]] = {}   # sala_id -> [player_id1, player_id2]


# ---------------------------------------------------------------------------
# Helpers RabbitMQ
# ---------------------------------------------------------------------------
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
        log.info(f"Jogada publicada na fila: player={msg.player_id} escolha={msg.escolha}")
    except Exception as e:
        log.error(f"Erro ao publicar no RabbitMQ: {e}")
        raise HTTPException(status_code=503, detail="Fila indisponível. RabbitMQ offline?")


# ---------------------------------------------------------------------------
# Schemas Pydantic
# ---------------------------------------------------------------------------
class EntrarPayload(BaseModel):
    sala_id: str | None = None   # None → cria nova sala


class JogadaPayload(BaseModel):
    player_id: str
    partida_id: str
    escolha: str


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------
@app.get("/")
def raiz():
    return FileResponse("frontend/index.html")


@app.post("/entrar")
def entrar(payload: EntrarPayload):
    """
    Jogador entra em uma sala.
    - Se sala_id não informado → cria nova sala e retorna o id.
    - Se sala_id informado → entra na sala existente (máx 2 jogadores).
    Retorna: {player_id, sala_id, partida_id, posicao}
    """
    player_id = str(uuid.uuid4())[:8]

    # Criar nova sala
    if not payload.sala_id:
        sala_id = str(uuid.uuid4())[:6].upper()
        partida_id = str(uuid.uuid4())[:8]
        salas[sala_id] = [player_id]
        partidas[partida_id] = ResultadoPartida(partida_id=partida_id, jogadas={})
        log.info(f"Nova sala criada: sala={sala_id} partida={partida_id} jogador=1({player_id})")
        return {
            "player_id": player_id,
            "sala_id": sala_id,
            "partida_id": partida_id,
            "posicao": 1,
            "mensagem": "Sala criada! Aguardando Jogador 2...",
        }

    # Entrar em sala existente
    sala_id = payload.sala_id.upper()
    if sala_id not in salas:
        raise HTTPException(status_code=404, detail="Sala não encontrada.")
    if len(salas[sala_id]) >= 2:
        raise HTTPException(status_code=409, detail="Sala cheia.")

    salas[sala_id].append(player_id)

    # Encontra a partida desta sala
    partida_id = None
    for pid, partida in partidas.items():
        if not partida.encerrada and len(partida.jogadas) < 2:
            partida_id = pid
            break

    if not partida_id:
        raise HTTPException(status_code=404, detail="Partida não encontrada para esta sala.")

    log.info(f"Jogador 2 entrou: sala={sala_id} partida={partida_id} jogador=2({player_id})")
    return {
        "player_id": player_id,
        "sala_id": sala_id,
        "partida_id": partida_id,
        "posicao": 2,
        "mensagem": "Você entrou na sala! Boa sorte!",
    }


@app.post("/jogada")
def fazer_jogada(payload: JogadaPayload):
    """
    Recebe a jogada do cliente e publica no RabbitMQ.
    O worker consome a fila e processa o resultado.
    """
    if payload.escolha not in ("pedra", "papel", "tesoura"):
        raise HTTPException(status_code=400, detail="Escolha inválida.")

    if payload.partida_id not in partidas:
        raise HTTPException(status_code=404, detail="Partida não encontrada.")

    partida = partidas[payload.partida_id]

    if partida.encerrada:
        raise HTTPException(status_code=409, detail="Partida já encerrada.")

    if payload.player_id in partida.jogadas:
        raise HTTPException(status_code=409, detail="Você já jogou nesta rodada.")

    msg = JogadaMessage(
        partida_id=payload.partida_id,
        player_id=payload.player_id,
        escolha=payload.escolha,
    )
    publicar_jogada(msg)

    aguardando = len(partida.jogadas) == 0  # ainda nenhuma jogada registrada
    return {
        "status": "jogada_enviada",
        "mensagem": "Jogada enviada! Aguardando o oponente..." if aguardando else "Jogada enviada! Processando resultado...",
    }


@app.get("/resultado/{partida_id}")
def obter_resultado(partida_id: str):
    """
    Polling do frontend: retorna o estado atual da partida.
    O worker atualiza `partidas` em memória ao processar a fila.
    """
    if partida_id not in partidas:
        raise HTTPException(status_code=404, detail="Partida não encontrada.")
    return partidas[partida_id].to_dict()


@app.post("/nova-partida/{sala_id}")
def nova_partida(sala_id: str):
    """Reseta a partida da sala para jogar novamente."""
    sala_id = sala_id.upper()
    if sala_id not in salas:
        raise HTTPException(status_code=404, detail="Sala não encontrada.")

    partida_id = str(uuid.uuid4())[:8]
    partidas[partida_id] = ResultadoPartida(partida_id=partida_id, jogadas={})

    # Remove partidas antigas encerradas para não acumular memória
    encerradas = [pid for pid, p in partidas.items() if p.encerrada and pid != partida_id]
    for pid in encerradas:
        del partidas[pid]

    log.info(f"Nova partida iniciada: sala={sala_id} partida={partida_id}")
    return {"partida_id": partida_id, "mensagem": "Nova partida iniciada!"}


# ---------------------------------------------------------------------------
# Servir arquivos estáticos do frontend
# ---------------------------------------------------------------------------
app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")