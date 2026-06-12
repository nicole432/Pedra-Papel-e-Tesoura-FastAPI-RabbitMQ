Pedra, Papel e Tesoura

1. Suba os serviços
  docker-compose up -d

2. Instale as dependências
  pip install -r requirements.txt

3. Inicie o servidor
  python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

4. Acesse
  http://localhost:8000

Tecnologias:
FastAPI — API REST
RabbitMQ — fila de mensagens (jogadas)
Redis — cache de resultados e histórico de partidas
