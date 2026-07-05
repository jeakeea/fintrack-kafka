"""Ledger-сервис: двойная запись + transactional outbox.

Потребляет ledger.transfer.requested, атомарно проводит дебет/кредит и пишет
событие-результат в outbox в той же транзакции. Отдельная корутина-publisher
доставляет неопубликованные строки outbox в Kafka (at-least-once).

Идемпотентность: проверка по outbox.payload->>'payment_id' + UNIQUE-констрейнт
ledger_entries(payment_id, account_id, direction). Партиционирование по from_account
гарантирует последовательную обработку операций одного счёта.
"""
from __future__ import annotations

import asyncio
import json
import logging

from shared.config import settings
from shared.db import close_pool, get_pool
from shared.events import LedgerResult, make_event
from shared.kafka_client import Producer, loads, run_consumer
from shared.topics import (
    LEDGER_TRANSFER_COMPLETED,
    LEDGER_TRANSFER_FAILED,
    LEDGER_TRANSFER_REQUESTED,
)

log = logging.getLogger("ledger")


async def _emit(conn, topic: str, key: str, payment_id: str, success: bool, reason: str | None) -> None:
    """Записать событие-результат в outbox (не публикуя напрямую в Kafka)."""
    event = make_event(
        topic, payment_id,
        LedgerResult(payment_id=payment_id, success=success, reason=reason).model_dump(),
    )
    await conn.execute(
        "INSERT INTO outbox(topic, msg_key, payload) VALUES($1, $2, $3::jsonb)",
        topic, key, json.dumps(event),
    )


async def handle(msg) -> None:
    env = loads(msg)
    data = env["data"]
    payment_id = data["payment_id"]
    from_account = data["from_account"]
    to_account = data["to_account"]
    amount = int(data["amount"])

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Идемпотентность: результат по этому payment_id уже формировался?
            already = await conn.fetchval(
                "SELECT 1 FROM outbox WHERE payload->>'payment_id' = $1 LIMIT 1", payment_id
            )
            if already:
                log.info("payment %s уже обработан – идемпотентный пропуск", payment_id)
                return

            balance = await conn.fetchval(
                "SELECT balance FROM balances WHERE account_id = $1", from_account
            ) or 0

            if amount > balance:
                await _emit(conn, LEDGER_TRANSFER_FAILED, from_account, payment_id, False, "insufficient_funds")
                log.info("payment %s FAILED insufficient_funds (balance=%s, amount=%s)", payment_id, balance, amount)
                return

            # Двойная запись: дебет источника + кредит получателя в одной транзакции
            await conn.execute(
                "INSERT INTO ledger_entries(payment_id, account_id, direction, amount) "
                "VALUES($1, $2, 'debit', $3) ON CONFLICT DO NOTHING",
                payment_id, from_account, amount,
            )
            await conn.execute(
                "INSERT INTO ledger_entries(payment_id, account_id, direction, amount) "
                "VALUES($1, $2, 'credit', $3) ON CONFLICT DO NOTHING",
                payment_id, to_account, amount,
            )
            await _emit(conn, LEDGER_TRANSFER_COMPLETED, from_account, payment_id, True, None)
            log.info("payment %s COMPLETED (%s -> %s, amount=%s)", payment_id, from_account, to_account, amount)


async def outbox_publisher(producer: Producer) -> None:
    """Фоновая доставка неопубликованных событий из outbox в Kafka."""
    pool = await get_pool()
    while True:
        try:
            rows = await pool.fetch(
                "SELECT id, topic, msg_key, payload FROM outbox "
                "WHERE published_at IS NULL ORDER BY id LIMIT 100"
            )
            for r in rows:
                payload = r["payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)
                await producer.send(r["topic"], payload, key=r["msg_key"])
                await pool.execute("UPDATE outbox SET published_at = now() WHERE id = $1", r["id"])
                log.info("outbox -> %s опубликовано (id=%s)", r["topic"], r["id"])
        except Exception:  # noqa: BLE001
            log.exception("ошибка outbox-publisher")
        await asyncio.sleep(0.3)


async def main() -> None:
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    await get_pool()
    producer = Producer()
    await producer.start()
    pub_task = asyncio.create_task(outbox_publisher(producer))
    log.info("ledger запущен, слушаю %s", LEDGER_TRANSFER_REQUESTED)
    try:
        await run_consumer([LEDGER_TRANSFER_REQUESTED], "ledger", handle, dlq_enabled=True, max_retries=3)
    finally:
        pub_task.cancel()
        await producer.stop()
        await close_pool()
