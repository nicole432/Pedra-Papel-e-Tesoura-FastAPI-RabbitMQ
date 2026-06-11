"""
worker.py — Consumidor RabbitMQ

Roda em paralelo ao FastAPI (thread separada ou processo separado).
Consome a fila 'jogadas', aplica as regras do jogo e atualiza o
dicionário `partidas` compartilhado com o FastAPI.

Para executar standalone:
    python worker.py

O main.py também pode importar e iniciar o worker em background thread:
    from worker import iniciar_worker_background
    iniciar_worker_background(partidas)
"""

import json
import threading
import time
import pika

from logger import configurar_logger, obter_logger
from protocolo import JogadaMessage, ResultadoPartida

configurar_logger()
log = obter_logger("Worker")

RABBITMQ_URL = "amqp://guest:guest@localhost/"
FILA_JOGADAS = "jogadas"

REGRAS = {
    "pedra":   "tesoura",   # pedra vence tesoura
    "papel":   "pedra",     # papel vence pedra
    "tesoura": "papel",     # tesoura vence papel
}


def verificar_vencedor(jogadas: dict) -> str:
    """
    Recebe dict {player_id: escolha} com exatamente 2 entradas.
    Retorna o player_id vencedor ou 'empate'.
    """
    ids = list(jogadas.keys())
    e1, e2 = jogadas[ids[0]], jogadas[ids[1]]

    if e1 == e2:
        return "empate"
    if REGRAS[e1] == e2:
        return ids[0]
    return ids[1]


def processar_mensagem(ch, method, properties, body, partidas: dict):
    """Callback invocado pelo pika para cada mensagem na fila."""
    try:
        msg = JogadaMessage.decodificar(body.decode())
        log.info(f"Mensagem recebida: partida={msg.partida_id} player={msg.player_id} escolha={msg.escolha}")

        if msg.partida_id not in partidas:
            log.warning(f"Partida {msg.partida_id} não encontrada. Descartando mensagem.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        partida: ResultadoPartida = partidas[msg.partida_id]

        if partida.encerrada:
            log.warning(f"Partida {msg.partida_id} já encerrada. Descartando mensagem.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if msg.player_id in partida.jogadas:
            log.warning(f"Jogador {msg.player_id} já jogou. Descartando duplicata.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Registra a jogada
        partida.jogadas[msg.player_id] = msg.escolha
        log.info(f"Jogada registrada: {len(partida.jogadas)}/2 na partida {msg.partida_id}")

        # Se os dois jogaram, calcula o resultado
        if len(partida.jogadas) == 2:
            vencedor = verificar_vencedor(partida.jogadas)
            partida.vencedor = vencedor
            partida.encerrada = True

            if vencedor == "empate":
                log.info(f"Partida {msg.partida_id} encerrada: EMPATE")
            else:
                escolha_v = partida.jogadas[vencedor]
                outro = [pid for pid in partida.jogadas if pid != vencedor][0]
                escolha_p = partida.jogadas[outro]
                log.info(
                    f"Partida {msg.partida_id} encerrada: "
                    f"vencedor={vencedor} ({escolha_v}) vs perdedor={outro} ({escolha_p})"
                )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        log.error(f"Erro ao processar mensagem: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def rodar_worker(partidas: dict):
    """Loop principal do worker com reconexão automática."""
    while True:
        try:
            log.info("Worker conectando ao RabbitMQ...")
            conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            canal = conn.channel()
            canal.queue_declare(queue=FILA_JOGADAS, durable=True)
            canal.basic_qos(prefetch_count=1)
            canal.basic_consume(
                queue=FILA_JOGADAS,
                on_message_callback=lambda ch, m, p, b: processar_mensagem(ch, m, p, b, partidas),
            )
            log.info(f"Worker aguardando mensagens na fila '{FILA_JOGADAS}'...")
            canal.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("RabbitMQ indisponível. Tentando novamente em 5s...")
            time.sleep(5)
        except Exception as e:
            log.error(f"Erro inesperado no worker: {e}. Reiniciando em 3s...")
            time.sleep(3)


def iniciar_worker_background(partidas: dict):
    """Inicia o worker em uma thread daemon (chamado pelo main.py)."""
    t = threading.Thread(target=rodar_worker, args=(partidas,), daemon=True)
    t.start()
    log.info("Worker iniciado em background thread.")
    return t


# Execução standalone
if __name__ == "__main__":
    from main import partidas as estado_global
    rodar_worker(estado_global)