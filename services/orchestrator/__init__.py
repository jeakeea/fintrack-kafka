"""Orchestrator-сервис: SAGA-оркестратор перевода.

Вход:  payment.initiated + ответы antifraud/ledger (group_id="orchestrator").
Выход: команды шагам саги и терминальные payment.completed / payment.failed.
Состояние саги хранится в таблице saga_state.
"""
