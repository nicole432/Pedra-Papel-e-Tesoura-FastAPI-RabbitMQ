import json
import threading
import time
import pika
import redis as redis_lib

from logger import configurar_logger, obter_logger
from protocolo import JogadaMessage, ResultadoPartida

configurar_logger()
log = obter_logger("Worker")

RABBITMQ_URL = "amqp://guest:guest@localhost/"
FILA_JOGADAS = "jogadas"
CACHE_TTL    = 60
PLACAR_TTL   = 3600  # histórico fica 1 hora no Redis

REGRAS = {
    "pedra":   "tesoura",
    "papel":   "pedra",
    "tesoura": "papel",
}


def verificar_vencedor(jogadas: dict) -> str:
    ids = list(jogadas.keys())
    e1, e2 = jogadas[ids[0]], jogadas[ids[1]]
    if e1 == e2:
        return "empate"
    if REGRAS[e1] == e2:
        return ids[0]
    return ids[1]


def atualizar_placar(cache, sala_id: str, partida: ResultadoPartida):
    """Atualiza o histórico e placar da sala no Redis."""
    try:
        chave = f"placar:{sala_id}"
        dados_raw = cache.get(chave)
        dados = json.loads(dados_raw) if dados_raw else {"rodadas": [], "pontos": {}}

        # Monta o registro desta rodada
        ids   = list(partida.jogadas.keys())
        rodada = {
            "rodada":   len(dados["rodadas"]) + 1,
            "jogadas":  partida.jogadas,
            "vencedor": partida.vencedor,
        }
        dados["rodadas"].append(rodada)

        # Atualiza pontos
        if partida.vencedor == "empate":
            for pid in ids:
                dados["pontos"].setdefault(pid, {"vitorias": 0, "empates": 0, "derrotas": 0})
                dados["pontos"][pid]["empates"] += 1
        else:
            for pid in ids:
                dados["pontos"].setdefault(pid, {"vitorias": 0, "empates": 0, "derrotas": 0})
            dados["pontos"][partida.vencedor]["vitorias"] += 1
            perdedor = [p for p in ids if p != partida.vencedor][0]
            dados["pontos"][perdedor]["derrotas"] += 1

        cache.setex(chave, PLACAR_TTL, json.dumps(dados))
        log.info(f"[Redis] Placar atualizado: sala={sala_id} rodada={rodada['rodada']}")
    except Exception as e:
        log.warning(f"[Redis] Erro ao atualizar placar: {e}")


def processar_mensagem(ch, method, properties, body, partidas: dict, salas: dict, cache):
    try:
        msg = JogadaMessage.decodificar(body.decode())
        log.info(f"[RabbitMQ] Mensagem recebida: partida={msg.partida_id} player={msg.player_id} escolha={msg.escolha}")

        if msg.partida_id not in partidas:
            log.warning(f"Partida {msg.partida_id} não encontrada. Descartando.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        partida: ResultadoPartida = partidas[msg.partida_id]

        if partida.encerrada:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if msg.player_id in partida.jogadas:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        partida.jogadas[msg.player_id] = msg.escolha
        log.info(f"Jogada registrada: {len(partida.jogadas)}/2 na partida {msg.partida_id}")

        if len(partida.jogadas) == 2:
            vencedor = verificar_vencedor(partida.jogadas)
            partida.vencedor = vencedor
            partida.encerrada = True
            log.info(f"Partida {msg.partida_id} encerrada! Vencedor: {vencedor}")

            # Salva resultado no cache
            try:
                cache.setex(f"resultado:{msg.partida_id}", CACHE_TTL, json.dumps(partida.to_dict()))
                log.info(f"[Redis] Resultado cacheado: partida={msg.partida_id}")
            except Exception as e:
                log.warning(f"[Redis] Erro ao cachear resultado: {e}")

            # Encontra a sala desta partida e atualiza o placar
            sala_id = next((sid for sid, s in salas.items() if s["partida_id"] == msg.partida_id or
                           any(msg.player_id in s["jogadores"] for _ in [1])), None)
            # Busca sala pelo player_id
            sala_id = next((sid for sid, s in salas.items() if msg.player_id in s["jogadores"]), None)
            if sala_id:
                atualizar_placar(cache, sala_id, partida)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        log.error(f"Erro ao processar mensagem: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def rodar_worker(partidas: dict, salas: dict, cache):
    while True:
        try:
            log.info("Worker conectando ao RabbitMQ...")
            conn  = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            canal = conn.channel()
            canal.queue_declare(queue=FILA_JOGADAS, durable=True)
            canal.basic_qos(prefetch_count=1)
            canal.basic_consume(
                queue=FILA_JOGADAS,
                on_message_callback=lambda ch, m, p, b: processar_mensagem(ch, m, p, b, partidas, salas, cache),
            )
            log.info(f"Worker ouvindo fila '{FILA_JOGADAS}'...")
            canal.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("RabbitMQ indisponível. Tentando em 5s...")
            time.sleep(5)
        except Exception as e:
            log.error(f"Erro no worker: {e}. Reiniciando em 3s...")
            time.sleep(3)


def iniciar_worker_background(partidas: dict, salas: dict, cache):
    t = threading.Thread(target=rodar_worker, args=(partidas, salas, cache), daemon=True)
    t.start()
    log.info("Worker iniciado em background thread.")
    return t