"""Тонкие обёртки над aiokafka: продюсер + цикл консьюмера с ретраями и DLQ.

Все сервисы используют этот модуль, чтобы поведение (сериализация, commit-after-handle,
DLQ) было единым.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .config import settings
from .topics import dlq

log = logging.getLogger(__name__)


def loads(msg) -> dict:
    """Распарсить value сообщения Kafka в dict (конверт события)."""
    return json.loads(msg.value)


class Producer:
    """Продюсер с acks=all. Значение – dict, сериализуется в JSON."""

    def __init__(self) -> None:
        self._p: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._p = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap, acks="all")
        await self._p.start()

    async def stop(self) -> None:
        if self._p is not None:
            await self._p.stop()

    async def send(self, topic: str, value: dict, key: str | None = None) -> None:
        assert self._p is not None, "Producer не запущен (await start())"
        payload = json.dumps(value, default=str).encode("utf-8")
        await self._p.send_and_wait(topic, key=key.encode("utf-8") if key else None, value=payload)


Handler = Callable[[object], Awaitable[None]]


async def run_consumer(topics: list[str], group_id: str, handler: Handler, *,
                       dlq_enabled: bool = True, max_retries: int = 3) -> None:
    """Запустить бесконечный цикл консьюмера.

    Для каждого сообщения вызывает handler(msg). Коммит оффсета – только после
    успешной обработки (at-least-once). После max_retries неудач – сообщение
    публикуется в <topic>.dlq и оффсет коммитится, чтобы не блокировать партицию.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap, acks="all")
    await consumer.start()
    await producer.start()
    log.info("consumer started group=%s topics=%s", group_id, topics)
    try:
        async for msg in consumer:
            ok = False
            for attempt in range(1, max_retries + 1):
                try:
                    await handler(msg)
                    ok = True
                    break
                except Exception:
                    log.exception("handler failed group=%s topic=%s attempt=%s", group_id, msg.topic, attempt)
                    await asyncio.sleep(0.2 * attempt)
            if not ok and dlq_enabled:
                await producer.send_and_wait(dlq(msg.topic), key=msg.key, value=msg.value)
                log.warning("message sent to DLQ topic=%s", dlq(msg.topic))
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()
