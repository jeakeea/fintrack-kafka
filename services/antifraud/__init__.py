"""Antifraud-сервис: проверка переводов по правилам лимита суммы и велосити.

Вход:  ANTIFRAUD_CHECK_REQUESTED (group_id="antifraud").
Выход: ANTIFRAUD_CHECK_COMPLETED с полезной нагрузкой AntifraudResult
       (ключ публикации = from_account, чтобы сохранить порядок по счёту).
"""
