"""Точка входа notification-сервиса: ``python -m services.notification``.

Консьюмер NOTIFICATION_REQUESTED для каждого события:
  * определяет получателя – резолвит id счёта из ``recipient`` в ``owner_name``
    по таблице ``accounts`` (если счёт не найден, использует значение как есть);
  * берёт текст пуша из события; если текста нет – собирает по статусу
    (completed/failed, +reason при отказе);
  * пишет строку в таблицу ``notifications`` и логирует «отправку».

DLQ и commit-after-handle берёт на себя ``shared.kafka_client.run_consumer``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import asyncpg

from shared.config import settings
from shared.db import close_pool, get_pool
from shared.events import NotificationData
from shared.kafka_client import loads, run_consumer
from shared.topics import NOTIFICATION_REQUESTED

log = logging.getLogger("services.notification")

# Отображение валюты в символ для текста уведомления.
_CURRENCY_SIGN = {"RUB": "₽", "USD": "$", "EUR": "€"}


def _format_amount(amount: Optional[Any], currency: str) -> str:
    """Отформатировать сумму из копеек в «1500.00 ₽» (пусто, если суммы нет)."""
    if amount is None:
        return ""
    sign = _CURRENCY_SIGN.get(currency, currency)
    return f"{int(amount) / 100:.2f} {sign}"


def _build_text(status: str, amount_str: str, reason: Optional[str]) -> str:
    """Собрать текст пуша по статусу перевода."""
    head = f"перевод {amount_str}".rstrip()
    if status == "completed":
        return f"{head} выполнен"
    text = f"{head} отклонён"
    if reason:
        text += f" ({reason})"
    return text


async def _resolve_recipient(pool: asyncpg.Pool, data: dict[str, Any]) -> str:
    """Определить получателя: owner_name счёта из БД, иначе значение как есть.

    Оркестратор кладёт в `recipient` id счёта-источника – резолвим его в имя
    владельца; если счёт не найден (или пришло уже имя), оставляем как есть.
    """
    candidate = (data.get("recipient") or data.get("account")
                 or data.get("recipient_account") or data.get("from_account")
                 or data.get("to_account"))
    if not candidate:
        return "клиент"
    row = await pool.fetchrow(
        "SELECT owner_name FROM accounts WHERE id = $1", str(candidate)
    )
    if row and row["owner_name"]:
        return str(row["owner_name"])
    return str(candidate)


async def handle(msg: object) -> None:
    """Обработать одно событие NOTIFICATION_REQUESTED."""
    event = loads(msg)
    data: dict[str, Any] = event.get("data") or {}
    payment_id = data.get("payment_id") or event["payment_id"]
    status = data.get("status") or "completed"
    reason = data.get("reason")
    channel = data.get("channel") or "push"
    currency = data.get("currency") or "RUB"
    amount = data.get("amount")

    pool = await get_pool()
    recipient = await _resolve_recipient(pool, data)
    # Оркестратор присылает готовый текст – используем его; собираем сами только
    # если события шлёт продюсер без текста (обратная совместимость контракта).
    text = data.get("text") or _build_text(status, _format_amount(amount, currency), reason)

    # Валидируем строку через доменную модель перед записью.
    notification = NotificationData(
        payment_id=payment_id,
        recipient=recipient,
        channel=channel,
        status=status,
        text=text,
    )
    await pool.execute(
        "INSERT INTO notifications (payment_id, recipient, channel, status, text) "
        "VALUES ($1, $2, $3, $4, $5)",
        notification.payment_id,
        notification.recipient,
        notification.channel,
        notification.status,
        notification.text,
    )
    log.info("PUSH -> %s: %s", recipient, text)


async def main() -> None:
    """Прогреть пул БД и запустить бесконечный цикл консьюмера уведомлений."""
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    await get_pool()  # прогрев пула соединений
    log.info("notification service started, group=notification")
    try:
        await run_consumer(
            [NOTIFICATION_REQUESTED],
            group_id="notification",
            handler=handle,
        )
    finally:
        await close_pool()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("notification service stopped")
