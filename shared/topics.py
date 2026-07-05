"""Единый каталог топиков Kafka – общий контракт для всех сервисов.

Ключ партиционирования для денежных потоков – `from_account`:
события одного счёта попадают в одну партицию и обрабатываются строго по порядку.
"""

# Жизненный цикл платежа (gateway -> orchestrator -> мир)
PAYMENT_INITIATED = "payment.initiated"          # produce: gateway
PAYMENT_COMPLETED = "payment.completed"          # produce: orchestrator
PAYMENT_FAILED = "payment.failed"                # produce: orchestrator

# Команды оркестратора сервисам и их ответы
ANTIFRAUD_CHECK_REQUESTED = "antifraud.check.requested"   # produce: orchestrator -> antifraud
ANTIFRAUD_CHECK_COMPLETED = "antifraud.check.completed"   # produce: antifraud -> orchestrator
LEDGER_TRANSFER_REQUESTED = "ledger.transfer.requested"   # produce: orchestrator -> ledger
LEDGER_TRANSFER_COMPLETED = "ledger.transfer.completed"   # produce: ledger -> orchestrator
LEDGER_TRANSFER_FAILED = "ledger.transfer.failed"         # produce: ledger -> orchestrator

# Уведомления
NOTIFICATION_REQUESTED = "notification.requested"         # produce: orchestrator -> notification

ALL_TOPICS = [
    PAYMENT_INITIATED,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    ANTIFRAUD_CHECK_REQUESTED,
    ANTIFRAUD_CHECK_COMPLETED,
    LEDGER_TRANSFER_REQUESTED,
    LEDGER_TRANSFER_COMPLETED,
    LEDGER_TRANSFER_FAILED,
    NOTIFICATION_REQUESTED,
]


def dlq(topic: str) -> str:
    """Имя dead-letter-топика для исходного топика."""
    return f"{topic}.dlq"
