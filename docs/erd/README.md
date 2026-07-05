# ERD – модель данных FinTrack v2

Диаграмма: [`erd.puml`](erd.puml) (PlantUML). Источник истины – [`db/001_init.sql`](../../db/001_init.sql).

## Таблицы

| Таблица | Назначение | Владелец |
|---|---|---|
| `accounts` | Счета клиентов (id, владелец, валюта) | ledger |
| `ledger_entries` | Проводки двойной записи (debit/credit) | ledger |
| `outbox` | Transactional outbox – события для публикации в Kafka | ledger |
| `idempotency_keys` | Маппинг `Idempotency-Key → payment_id` | gateway |
| `saga_state` | Состояние саги по каждому платежу | orchestrator |
| `notifications` | Журнал отправленных уведомлений | notification |

Единственный физический внешний ключ – `ledger_entries.account_id → accounts.id`.
Остальные связи логические, по сквозному ключу `payment_id`: сервисы намеренно
изолированы по данным (каждый владеет своими таблицами), межсервисных FK нет.

## Двойная запись (double-entry bookkeeping)

Каждый перевод порождает **ровно две проводки** в `ledger_entries`, и обе
пишутся в **одной транзакции БД**:

- `debit` со счёта-источника (`from_account`) на сумму `amount`;
- `credit` на счёт-получателя (`to_account`) на ту же сумму `amount`.

Сумма всех `debit` всегда равна сумме всех `credit` – деньги не появляются
и не исчезают, меняется только их распределение между счетами. Суммы хранятся
в **минорных единицах** (копейки, `BIGINT`), `CHECK (amount > 0)` запрещает
нулевые и отрицательные проводки.

Идемпотентность проводок обеспечивает ограничение
`UNIQUE (payment_id, account_id, direction)`: повторная обработка того же
события `ledger.transfer.requested` (at-least-once доставка) не задвоит
debit/credit – вторая вставка нарушит уникальность и будет проигнорирована.

## Как считается баланс (вью `balances`)

Баланс не хранится отдельным полем – он **вычисляется** из проводок, поэтому
всегда консистентен с историей:

```sql
balance(account) = SUM(credit.amount) − SUM(debit.amount)
```

Реализация – вью `balances`:

```sql
CREATE OR REPLACE VIEW balances AS
SELECT a.id AS account_id,
       a.owner_name,
       COALESCE(SUM(CASE WHEN e.direction = 'credit' THEN e.amount
                         ELSE -e.amount END), 0) AS balance
FROM accounts a
LEFT JOIN ledger_entries e ON e.account_id = a.id
GROUP BY a.id, a.owner_name;
```

`LEFT JOIN` + `COALESCE(..., 0)` гарантируют корректный нулевой баланс для
счёта без проводок. Стартовые остатки задаются «открывающими» `credit`-проводками
в [`db/002_seed.sql`](../../db/002_seed.sql) (напр. Иван – 10 000.00 ₽ = `1000000` копеек).

## Outbox

`ledger` в той же транзакции, что и проводки, вставляет строку в `outbox`
(`topic`, `msg_key`, `payload JSONB`). Отдельный publisher-relay читает
неопубликованные строки (частичный индекс `idx_outbox_unpublished` по
`published_at IS NULL`), публикует их в Kafka и проставляет `published_at`.
Это гарантирует: либо проводки и событие зафиксированы вместе, либо ничего –
событие не теряется и не уходит без проводок (см. [ADR-0004](../adr/0004-outbox-pattern.md)).
