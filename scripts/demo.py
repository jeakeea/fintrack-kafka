"""End-to-end демо FinTrack v2.

Гоняет сценарии через HTTP gateway и показывает работу саги, идемпотентности,
антифрода, двойной записи и DLQ. Запуск: python -m scripts.demo
(нужны поднятые: docker compose up + ledger/antifraud/notification/orchestrator/gateway).
"""
from __future__ import annotations

import sys
import time
import uuid

import httpx

BASE = "http://localhost:8000"


def rub(kop: int) -> str:
    return f"{kop / 100:,.2f} ₽".replace(",", " ")


def get_accounts() -> dict:
    r = httpx.get(f"{BASE}/accounts", timeout=10)
    r.raise_for_status()
    return {a["account_id"]: a for a in r.json()}


def print_balances(title: str) -> None:
    print(f"\n  Балансы – {title}:")
    for acc in get_accounts().values():
        print(f"    {acc['account_id']:<10} {acc['owner_name']:<16} {rub(acc['balance'])}")


def transfer(frm: str, to: str, amount: int, idem: str | None = None) -> tuple[dict, str]:
    idem = idem or str(uuid.uuid4())
    r = httpx.post(
        f"{BASE}/transfers",
        json={"from_account": frm, "to_account": to, "amount": amount, "currency": "RUB"},
        headers={"Idempotency-Key": idem},
        timeout=10,
    )
    r.raise_for_status()
    return r.json(), idem


def wait_terminal(payment_id: str, timeout: float = 20.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(f"{BASE}/transfers/{payment_id}", timeout=10)
        if r.status_code == 200 and r.json().get("status") in ("COMPLETED", "FAILED"):
            return r.json()
        time.sleep(0.5)
    return {"status": "TIMEOUT"}


def scenario(title: str, frm: str, to: str, amount: int, expect: str | None = None) -> str:
    print(f"\n=== {title} ===")
    print(f"  {frm} → {to} на {rub(amount)}")
    resp, _ = transfer(frm, to, amount)
    pid = resp["payment_id"]
    print(f"  gateway: {resp}")
    final = wait_terminal(pid)
    mark = "" if (expect is None or final.get("status") == expect) else f"  ⚠️ ожидали {expect}"
    print(f"  итог саги: status={final.get('status')} reason={final.get('reason')}{mark}")
    return pid


def main() -> None:
    try:
        print_balances("старт")
    except Exception as e:  # noqa: BLE001
        print(f"Не достучался до gateway на {BASE}. Подними инфраструктуру и сервисы.\n  {e}")
        sys.exit(1)

    # 1. Успешный перевод
    scenario("Сценарий 1 – успешный перевод", "acc_ivan", "acc_maria", 150000, expect="COMPLETED")
    print_balances("после сценария 1")

    # 2. Нехватка средств: сначала сливаем oleg почти в ноль (сумма < лимита антифрода),
    #    затем пробуем перевести больше остатка.
    scenario("Сценарий 2a – подготовка (oleg почти в ноль)", "acc_oleg", "acc_ivan", 480000, expect="COMPLETED")
    scenario("Сценарий 2 – нехватка средств (ledger отклоняет)", "acc_oleg", "acc_ivan", 200000, expect="FAILED")

    # 3. Отклонение антифродом (сумма выше лимита 5 000.00)
    scenario("Сценарий 3 – отклонение антифродом (amount_limit)", "acc_ivan", "acc_maria", 600000, expect="FAILED")

    # 4. Дубликат запроса с одним Idempotency-Key
    print("\n=== Сценарий 4 – дубликат (один Idempotency-Key) ===")
    key = str(uuid.uuid4())
    r1, _ = transfer("acc_ivan", "acc_oleg", 100000, idem=key)
    r2, _ = transfer("acc_ivan", "acc_oleg", 100000, idem=key)
    print(f"  1-й ответ: {r1}")
    print(f"  2-й ответ: {r2}")
    same = r1["payment_id"] == r2["payment_id"]
    print(f"  payment_id совпадает: {same} (ожидаем True – второй запрос помечен как duplicate, деньги не двигаются дважды)")
    wait_terminal(r1["payment_id"])

    print_balances("финал")
    print("\nГотово. Открой Kafka UI: http://localhost:8080 – посмотри топики, партиции и DLQ.")


if __name__ == "__main__":
    main()
