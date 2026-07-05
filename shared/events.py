"""Контракт событий: общий конверт + доменные полезные нагрузки.

Все суммы – целые минорные единицы (копейки). Никаких float в деньгах.
Каждое событие сериализуется в конверт `EventEnvelope` (см. make_event).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field


def new_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=new_id)
    event_type: str
    occurred_at: str = Field(default_factory=now_iso)
    payment_id: str
    trace_id: str
    data: dict[str, Any]


def make_event(event_type: str, payment_id: str, data: dict[str, Any],
               trace_id: Optional[str] = None) -> dict[str, Any]:
    """Собрать событие-конверт (dict, готовый к отправке в Kafka)."""
    return EventEnvelope(
        event_type=event_type,
        payment_id=payment_id,
        trace_id=trace_id or payment_id,
        data=data,
    ).model_dump()


# ---- Доменные полезные нагрузки (поле data конверта) ----

class TransferData(BaseModel):
    payment_id: str
    from_account: str
    to_account: str
    amount: int          # минорные единицы (копейки), > 0
    currency: str = "RUB"
    idempotency_key: str


class AntifraudResult(BaseModel):
    payment_id: str
    approved: bool
    reason: Optional[str] = None
    score: float = 0.0


class LedgerResult(BaseModel):
    payment_id: str
    success: bool
    reason: Optional[str] = None


class NotificationData(BaseModel):
    payment_id: str
    recipient: str
    channel: str = "push"
    status: str          # "completed" | "failed"
    text: str


class PaymentOutcomeData(BaseModel):
    """Полезная нагрузка терминальных событий payment.completed / payment.failed."""
    payment_id: str
    status: str          # "completed" | "failed"
    reason: Optional[str] = None
