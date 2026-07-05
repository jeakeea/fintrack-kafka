.PHONY: up down logs ps topics gateway orchestrator ledger antifraud notification demo

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

# создать топики с несколькими партициями (история про ordering by account)
topics:
	bash scripts/create_topics.sh

gateway:
	uvicorn services.gateway.main:app --reload --port 8000

orchestrator:
	python -m services.orchestrator

ledger:
	python -m services.ledger

antifraud:
	python -m services.antifraud

notification:
	python -m services.notification

demo:
	python -m scripts.demo
