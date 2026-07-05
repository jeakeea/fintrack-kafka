"""SAGA-оркестратор переводов.

Подписан на `payment.initiated` и ответы antifraud/ledger. Ведёт `saga_state`
и дирижирует шагами саги, публикуя команды следующим сервисам. Все публикации –
с key=from_account (гарантия порядка обработки по счёту-источнику).

Переходы саги защищены условными UPDATE (guarded transitions): повторная доставка
события (at-least-once) не откатывает сагу назад и не дублирует терминальные
события/уведомления.

Поток саги:
    PAYMENT_INITIATED          -> ANTIFRAUD_PENDING -> ANTIFRAUD_CHECK_REQUESTED
    ANTIFRAUD_CHECK_COMPLETED  -> approved:  LEDGER_PENDING -> LEDGER_TRANSFER_REQUESTED
                                  rejected:  FAILED -> PAYMENT_FAILED + NOTIFICATION_REQUESTED
    LEDGER_TRANSFER_COMPLETED  -> COMPLETED -> PAYMENT_COMPLETED + NOTIFICATION_REQUESTED
    LEDGER_TRANSFER_FAILED     -> FAILED -> PAYMENT_FAILED + NOTIFICATION_REQUESTED
"""
from __future__ import annotations

import asyncio
import logging

from shared.config import settings
from shared.db import close_pool, get_pool
from shared.events import NotificationData, PaymentOutcomeData, TransferData, make_event
from shared.kafka_client import Producer, loads, run_consumer
from shared.topics import (
    ANTIFRAUD_CHECK_COMPLETED,
    ANTIFRAUD_CHECK_REQUESTED,
    LEDGER_TRANSFER_COMPLETED,
    LEDGER_TRANSFER_FAILED,
    LEDGER_TRANSFER_REQUESTED,
    NOTIFICATION_REQUESTED,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    PAYMENT_INITIATED,
)

logging.basicConfig(level=settings.log_level)
log = logging.getLogger("orchestrator")

GROUP_ID = "orchestrator"
TOPICS = [
    PAYMENT_INITIATED,
    ANTIFRAUD_CHECK_COMPLETED,
    LEDGER_TRANSFER_COMPLETED,
    LEDGER_TRANSFER_FAILED,
]


class Orchestrator:
    """Обработчик событий саги. Хранит ссылки на пул БД и продюсер."""

    def __init__(self, pool, producer: Producer) -> None:
        self.pool = pool
        self.producer = producer

    async def handle(self, msg) -> None:
        """Точка входа консьюмера: разобрать конверт и направить по типу события."""
        event = loads(msg)
        event_type = event["event_type"]
        payment_id = event["payment_id"]
        data = event.get("data") or {}
        trace_id = event.get("trace_id")

        if event_type == PAYMENT_INITIATED:
            await self._on_initiated(payment_id, data, trace_id)
        elif event_type == ANTIFRAUD_CHECK_COMPLETED:
            await self._on_antifraud(payment_id, data, trace_id)
        elif event_type == LEDGER_TRANSFER_COMPLETED:
            await self._on_ledger_completed(payment_id, data, trace_id)
        elif event_type == LEDGER_TRANSFER_FAILED:
            await self._on_ledger_failed(payment_id, data, trace_id)
        else:
            log.warning("unexpected event_type=%s payment_id=%s", event_type, payment_id)

    # ---- Шаги саги ----

    async def _on_initiated(self, payment_id: str, data: dict, trace_id: str | None) -> None:
        """Старт саги: зафиксировать реквизиты и запросить проверку антифрода."""
        from_account = data["from_account"]
        # UPSERT с защитой от повторной доставки (at-least-once): обновляемся только
        # пока сага в начальной фазе. Повтор payment.initiated для саги, ушедшей в
        # LEDGER_PENDING/COMPLETED/FAILED, не откатывает её назад и не публикует
        # команду антифроду заново.
        row = await self.pool.fetchrow(
            """INSERT INTO saga_state (payment_id, status, step, from_account, to_account, amount, updated_at)
               VALUES ($1, 'ANTIFRAUD_PENDING', 'ANTIFRAUD_REQUESTED', $2, $3, $4, now())
               ON CONFLICT (payment_id) DO UPDATE
                 SET status = 'ANTIFRAUD_PENDING', step = 'ANTIFRAUD_REQUESTED',
                     from_account = EXCLUDED.from_account, to_account = EXCLUDED.to_account,
                     amount = EXCLUDED.amount, updated_at = now()
                 WHERE saga_state.status IN ('INITIATED', 'ANTIFRAUD_PENDING')
               RETURNING payment_id""",
            payment_id, from_account, data["to_account"], data["amount"],
        )
        if row is None:
            log.info("skip duplicate payment.initiated payment_id=%s (сага уже дальше)", payment_id)
            return
        transfer = self._transfer_data(
            payment_id, from_account, data["to_account"], data["amount"],
            currency=data.get("currency", "RUB"),
            idempotency_key=data.get("idempotency_key", payment_id),
        )
        await self._publish(ANTIFRAUD_CHECK_REQUESTED, payment_id, transfer, from_account, trace_id)

    async def _on_antifraud(self, payment_id: str, data: dict, trace_id: str | None) -> None:
        """Ответ антифрода: одобрено -> в леджер, отклонено -> провал саги."""
        saga = await self._load_saga(payment_id)
        if saga is None:
            return
        from_account = saga["from_account"]
        if data.get("approved"):
            # Переход разрешён только из ожидания антифрода (или повтор в LEDGER_PENDING –
            # команда леджеру идемпотентна). Терминальную сагу не трогаем.
            row = await self.pool.fetchrow(
                """UPDATE saga_state SET status = 'LEDGER_PENDING', step = 'LEDGER_REQUESTED',
                          updated_at = now()
                   WHERE payment_id = $1 AND status IN ('ANTIFRAUD_PENDING', 'LEDGER_PENDING')
                   RETURNING payment_id""",
                payment_id,
            )
            if row is None:
                log.info("skip stale antifraud result payment_id=%s status=%s", payment_id, saga["status"])
                return
            transfer = self._transfer_data(
                payment_id, from_account, saga["to_account"], saga["amount"],
                idempotency_key=payment_id,
            )
            await self._publish(LEDGER_TRANSFER_REQUESTED, payment_id, transfer, from_account, trace_id)
        else:
            reason = data.get("reason") or "antifraud rejected"
            await self._fail(payment_id, saga, reason, trace_id)

    async def _on_ledger_completed(self, payment_id: str, data: dict, trace_id: str | None) -> None:
        """Леджер провёл перевод: завершить сагу и уведомить."""
        saga = await self._load_saga(payment_id)
        if saga is None:
            return
        # Терминальный переход выполняется ровно один раз: повтор события не даёт
        # продублировать payment.completed и уведомление.
        row = await self.pool.fetchrow(
            """UPDATE saga_state SET status = 'COMPLETED', step = 'COMPLETED',
                      updated_at = now()
               WHERE payment_id = $1 AND status NOT IN ('COMPLETED', 'FAILED')
               RETURNING payment_id""",
            payment_id,
        )
        if row is None:
            log.info("skip duplicate ledger result payment_id=%s (сага уже терминальна)", payment_id)
            return
        from_account = saga["from_account"]
        outcome = PaymentOutcomeData(payment_id=payment_id, status="completed").model_dump()
        await self._publish(PAYMENT_COMPLETED, payment_id, outcome, from_account, trace_id)
        await self._notify(
            payment_id, from_account, "completed",
            f"Перевод {self._fmt_amount(saga['amount'])} на счёт {saga['to_account']} выполнен.",
            trace_id,
        )

    async def _on_ledger_failed(self, payment_id: str, data: dict, trace_id: str | None) -> None:
        """Леджер отклонил перевод (нет средств и т.п.): провал саги."""
        saga = await self._load_saga(payment_id)
        if saga is None:
            return
        reason = data.get("reason") or "ledger transfer failed"
        await self._fail(payment_id, saga, reason, trace_id)

    # ---- Вспомогательное ----

    async def _fail(self, payment_id: str, saga, reason: str, trace_id: str | None) -> None:
        """Перевести сагу в FAILED, опубликовать payment.failed и уведомление."""
        row = await self.pool.fetchrow(
            """UPDATE saga_state SET status = 'FAILED', step = 'FAILED', reason = $2,
                      updated_at = now()
               WHERE payment_id = $1 AND status NOT IN ('COMPLETED', 'FAILED')
               RETURNING payment_id""",
            payment_id, reason,
        )
        if row is None:
            log.info("skip duplicate fail payment_id=%s (сага уже терминальна)", payment_id)
            return
        from_account = saga["from_account"]
        outcome = PaymentOutcomeData(payment_id=payment_id, status="failed", reason=reason).model_dump()
        await self._publish(PAYMENT_FAILED, payment_id, outcome, from_account, trace_id)
        await self._notify(payment_id, from_account, "failed", f"Перевод отклонён: {reason}", trace_id)

    async def _load_saga(self, payment_id: str):
        """Прочитать состояние саги; если нет – предупредить и вернуть None."""
        saga = await self.pool.fetchrow(
            "SELECT * FROM saga_state WHERE payment_id = $1", payment_id
        )
        if saga is None:
            log.warning("event for unknown saga payment_id=%s (пропуск)", payment_id)
        return saga

    async def _notify(self, payment_id: str, recipient: str, status: str, text: str,
                      trace_id: str | None) -> None:
        """Опубликовать запрос на уведомление (recipient – id счёта-источника)."""
        notif = NotificationData(
            payment_id=payment_id, recipient=recipient, status=status, text=text
        ).model_dump()
        await self._publish(NOTIFICATION_REQUESTED, payment_id, notif, recipient, trace_id)

    async def _publish(self, topic: str, payment_id: str, data: dict, key: str,
                       trace_id: str | None) -> None:
        """Собрать конверт и отправить в Kafka (key=from_account)."""
        event = make_event(topic, payment_id, data, trace_id=trace_id)
        await self.producer.send(topic, event, key=key)
        log.info("published topic=%s payment_id=%s", topic, payment_id)

    @staticmethod
    def _transfer_data(payment_id: str, from_account: str, to_account: str, amount,
                       currency: str = "RUB", idempotency_key: str | None = None) -> dict:
        """Полезная нагрузка TransferData как dict (валидация по контракту)."""
        return TransferData(
            payment_id=payment_id,
            from_account=from_account,
            to_account=to_account,
            amount=int(amount),
            currency=currency,
            idempotency_key=idempotency_key or payment_id,
        ).model_dump()

    @staticmethod
    def _fmt_amount(amount) -> str:
        """Копейки -> строка рублей для текста уведомления (без float в деньгах)."""
        rub, kop = divmod(int(amount), 100)
        return f"{rub},{kop:02d} ₽"


async def main() -> None:
    """Поднять пул БД и продюсер, запустить бесконечный цикл консьюмера саги."""
    pool = await get_pool()
    producer = Producer()
    await producer.start()
    orch = Orchestrator(pool, producer)
    log.info("orchestrator starting group=%s topics=%s", GROUP_ID, TOPICS)
    try:
        await run_consumer(TOPICS, GROUP_ID, orch.handle)
    finally:
        await producer.stop()
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
