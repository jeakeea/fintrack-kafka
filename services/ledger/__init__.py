"""Ledger-сервис: двойная запись, вычисляемый баланс и transactional outbox.

Вход:  LEDGER_TRANSFER_REQUESTED (group_id="ledger").
Выход: LEDGER_TRANSFER_COMPLETED / LEDGER_TRANSFER_FAILED – пишутся в outbox
       в одной транзакции с проводками; публикует их отдельный relay.
"""
