"""Точка входа antifraud-сервиса: ``python -m services.antifraud``.

Консьюмер ANTIFRAUD_CHECK_REQUESTED прогоняет каждый перевод через
:class:`~services.antifraud.rules.AntifraudEngine` и публикует результат в
ANTIFRAUD_CHECK_COMPLETED. DLQ и commit-after-handle берёт на себя
``shared.kafka_client.run_consumer``.
"""
from __future__ import annotations

import asyncio
import logging

from shared.config import settings
from shared.events import AntifraudResult, make_event
from shared.kafka_client import Producer, loads, run_consumer
from shared.topics import ANTIFRAUD_CHECK_COMPLETED, ANTIFRAUD_CHECK_REQUESTED

from .rules import AntifraudEngine

log = logging.getLogger("services.antifraud")


def _make_handler(engine: AntifraudEngine, producer: Producer):
    """Собрать обработчик сообщений с захваченными движком и продюсером."""

    async def handle(msg: object) -> None:
        event = loads(msg)
        data = event["data"]
        payment_id = data.get("payment_id") or event["payment_id"]
        from_account = data["from_account"]
        amount = int(data["amount"])

        verdict = engine.check(from_account, amount)
        result = AntifraudResult(
            payment_id=payment_id,
            approved=verdict.approved,
            reason=verdict.reason,
            score=verdict.score,
        )
        out = make_event(
            ANTIFRAUD_CHECK_COMPLETED,
            payment_id,
            result.model_dump(),
            trace_id=event.get("trace_id"),
        )
        await producer.send(ANTIFRAUD_CHECK_COMPLETED, out, key=from_account)
        log.info(
            "antifraud payment=%s account=%s amount=%s -> approved=%s reason=%s score=%.4f",
            payment_id, from_account, amount, verdict.approved, verdict.reason, verdict.score,
        )

    return handle


async def main() -> None:
    """Запустить продюсер и бесконечный цикл консьюмера антифрода."""
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = AntifraudEngine()
    producer = Producer()
    await producer.start()
    log.info("antifraud service started, group=antifraud")
    try:
        await run_consumer(
            [ANTIFRAUD_CHECK_REQUESTED],
            group_id="antifraud",
            handler=_make_handler(engine, producer),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("antifraud service stopped")
