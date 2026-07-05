"""HTTP-вход системы переводов: приём заявок, идемпотентность, статус саги.

FastAPI-приложение (запуск: `uvicorn services.gateway.main:app --port 8000`).
POST /transfers публикует `payment.initiated` в Kafka и стартует сагу. Повторный
запрос с тем же `Idempotency-Key` возвращает существующий payment_id и НЕ
публикует событие повторно. Пул БД и продюсер живут на время жизни приложения.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from shared.config import settings
from shared.db import close_pool, get_pool
from shared.events import TransferData, make_event, new_id
from shared.kafka_client import Producer
from shared.topics import PAYMENT_INITIATED

logging.basicConfig(level=settings.log_level)
log = logging.getLogger("gateway")


class TransferRequest(BaseModel):
    from_account: str
    to_account: str
    amount: int = Field(gt=0, description="Сумма в минорных единицах (копейки), > 0")
    currency: str = "RUB"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Поднять пул БД и продюсер на старте, аккуратно закрыть на остановке."""
    app.state.pool = await get_pool()
    app.state.producer = Producer()
    await app.state.producer.start()
    log.info("gateway started port=%s", settings.gateway_port)
    try:
        yield
    finally:
        await app.state.producer.stop()
        await close_pool()
        log.info("gateway stopped")


app = FastAPI(title="FinTrack v2 – Gateway", lifespan=lifespan)


@app.post("/transfers", status_code=202)
async def create_transfer(
    req: TransferRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    """Принять заявку на перевод и запустить сагу.

    Идемпотентность по `Idempotency-Key`: повтор с тем же ключом возвращает уже
    созданный payment_id (status="duplicate") и НЕ публикует событие заново.
    """
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    pool = app.state.pool

    # Быстрый путь: ключ уже известен -> вернуть прежний payment_id без публикации.
    existing = await pool.fetchrow(
        "SELECT payment_id FROM idempotency_keys WHERE key = $1", idempotency_key
    )
    if existing is not None:
        log.info("duplicate transfer key=%s payment_id=%s", idempotency_key, existing["payment_id"])
        return {"payment_id": existing["payment_id"], "status": "duplicate"}

    payment_id = new_id()
    # idempotency_keys + saga_state пишем в одной транзакции; ON CONFLICT закрывает
    # гонку параллельных запросов с одинаковым ключом.
    async with pool.acquire() as conn:
        async with conn.transaction():
            inserted = await conn.fetchrow(
                """INSERT INTO idempotency_keys (key, payment_id) VALUES ($1, $2)
                   ON CONFLICT (key) DO NOTHING
                   RETURNING payment_id""",
                idempotency_key, payment_id,
            )
            if inserted is None:
                row = await conn.fetchrow(
                    "SELECT payment_id FROM idempotency_keys WHERE key = $1", idempotency_key
                )
                log.info("duplicate transfer (race) key=%s payment_id=%s", idempotency_key, row["payment_id"])
                return {"payment_id": row["payment_id"], "status": "duplicate"}

            await conn.execute(
                """INSERT INTO saga_state (payment_id, status, step, from_account, to_account, amount)
                   VALUES ($1, 'INITIATED', 'INITIATED', $2, $3, $4)""",
                payment_id, req.from_account, req.to_account, req.amount,
            )

    data = TransferData(
        payment_id=payment_id,
        from_account=req.from_account,
        to_account=req.to_account,
        amount=req.amount,
        currency=req.currency,
        idempotency_key=idempotency_key,
    ).model_dump()
    event = make_event(PAYMENT_INITIATED, payment_id, data)
    # Ключ публикации = from_account -> события одного счёта идут по порядку.
    await app.state.producer.send(PAYMENT_INITIATED, event, key=req.from_account)

    log.info("transfer accepted payment_id=%s from=%s to=%s amount=%s",
             payment_id, req.from_account, req.to_account, req.amount)
    return {"payment_id": payment_id, "status": "accepted"}


@app.get("/transfers/{payment_id}")
async def get_transfer(payment_id: str):
    """Текущий статус перевода (состояние саги). 404, если не найдено."""
    row = await app.state.pool.fetchrow(
        """SELECT payment_id, status, step, from_account, to_account, amount, reason, updated_at
           FROM saga_state WHERE payment_id = $1""",
        payment_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="payment not found")
    return dict(row)


@app.get("/accounts")
async def list_accounts():
    """Список счетов с балансами (из вью balances). Баланс – в копейках."""
    rows = await app.state.pool.fetch(
        "SELECT account_id, owner_name, balance FROM balances ORDER BY account_id"
    )
    return [dict(r) for r in rows]
