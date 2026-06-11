import json
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass(frozen=True)
class JogadaMessage:
    """Mensagem publicada na fila do RabbitMQ pelo FastAPI."""
    partida_id: str
    player_id: str
    escolha: str  # "pedra" | "papel" | "tesoura"

    def codificar(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def decodificar(dados: str) -> "JogadaMessage":
        return JogadaMessage(**json.loads(dados))


@dataclass
class ResultadoPartida:
    """Estado de uma partida — salvo em memória pelo worker."""
    partida_id: str
    jogadas: dict           # {player_id: escolha}
    vencedor: Optional[str] = None   # player_id | "empate" | None
    encerrada: bool = False

    def to_dict(self) -> dict:
        return {
            "partida_id": self.partida_id,
            "jogadas": self.jogadas,
            "vencedor": self.vencedor,
            "encerrada": self.encerrada,
        }