"""Правила антифрода: лимит суммы и велосити-контроль (in-memory).

Состояние велосити живёт в памяти процесса: для каждого `from_account` храним
временные метки последних переводов в скользящем окне. Для демо этого достаточно;
в проде окно держали бы во внешнем хранилище (Redis/Aerospike).
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional

# Порог одной операции: 5 000.00 ₽ = 500000 копеек. Больше – отклоняем.
DEFAULT_AMOUNT_LIMIT = 500_000
# Велосити: не более N переводов с одного счёта за окно в секундах.
DEFAULT_VELOCITY_MAX = 3
DEFAULT_VELOCITY_WINDOW_SEC = 60.0


@dataclass(frozen=True)
class Verdict:
    """Решение движка по одному переводу."""

    approved: bool
    score: float
    reason: Optional[str] = None


class AntifraudEngine:
    """Движок антифрод-правил с in-memory счётчиком велосити.

    Потокобезопасность не требуется: консьюмер обрабатывает сообщения
    последовательно в одном event-loop.
    """

    def __init__(self, *, amount_limit: int = DEFAULT_AMOUNT_LIMIT,
                 velocity_max: int = DEFAULT_VELOCITY_MAX,
                 velocity_window_sec: float = DEFAULT_VELOCITY_WINDOW_SEC) -> None:
        self._amount_limit = amount_limit
        self._velocity_max = velocity_max
        self._velocity_window = velocity_window_sec
        self._history: dict[str, deque[float]] = defaultdict(deque)

    def _register(self, from_account: str, now: float) -> int:
        """Зафиксировать попытку перевода и вернуть число попыток в окне."""
        window = self._history[from_account]
        window.append(now)
        threshold = now - self._velocity_window
        while window and window[0] < threshold:
            window.popleft()
        return len(window)

    def check(self, from_account: str, amount: int, *,
              now: Optional[float] = None) -> Verdict:
        """Проверить перевод по правилам и вернуть вердикт.

        Каждая попытка учитывается в велосити-окне. Сначала проверяем лимит
        суммы, затем велосити; иначе аппрувим с простым риск-скорингом.
        """
        ts = time.monotonic() if now is None else now
        attempts = self._register(from_account, ts)

        if amount > self._amount_limit:
            return Verdict(approved=False, score=0.99, reason="amount_limit")
        if attempts > self._velocity_max:
            return Verdict(approved=False, score=0.95, reason="velocity")

        # Простой скоринг: доля суммы от двойного лимита, обрезанная до [0, 1].
        score = round(min(1.0, amount / (2 * self._amount_limit)), 4)
        return Verdict(approved=True, score=score, reason=None)
