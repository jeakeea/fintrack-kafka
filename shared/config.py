"""Единая конфигурация из переменных окружения (.env)."""
import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    postgres_dsn: str = os.getenv("POSTGRES_DSN", "postgresql://fintrack:fintrack@localhost:5432/fintrack")
    gateway_port: int = int(os.getenv("GATEWAY_PORT", "8000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
