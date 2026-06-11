import logging
import sys


def configurar_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def obter_logger(nome: str) -> logging.Logger:
    return logging.getLogger(nome)