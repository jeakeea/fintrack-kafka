"""Gateway-сервис: синхронный REST-вход системы переводов (FastAPI).

Принимает POST /transfers (идемпотентность по Idempotency-Key), публикует
payment.initiated и отдаёт статус саги/балансы читающими эндпоинтами.
"""
