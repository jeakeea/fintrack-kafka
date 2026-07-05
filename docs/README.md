# Документация FinTrack v2

Аналитический пакет по системе P2P-переводов – от требований до архитектурных
решений. Спецификации требований, модели данных и REST API оформлены по
шаблонам базы знаний в стиле Confluence (история изменений первым разделом,
критерии приёмки «Если/Когда/Тогда», табличные описания методов и сущностей).

Источник истины по контрактам – код: [`../shared/`](../shared/) (топики и схемы
событий) и [`../db/001_init.sql`](../db/001_init.sql) (схема БД). Документы
сверены с реализацией; важные особенности контракта вынесены в раздел
«Замечания по контракту» внизу.

## Состав пакета

| Артефакт | Файл | Что внутри |
|---|---|---|
| **Функциональные требования** | [`requirements/functional.md`](requirements/functional.md) | Границы системы, акторы, user stories, use cases, приоритизация MoSCoW |
| **User story: перевод денег** | [`requirements/user-story-transfer.md`](requirements/user-story-transfer.md) | Ключевая история по шаблону: Я как / Хочу / Для того, чтобы + критерии приёмки Если/Когда/Тогда |
| **Нефункциональные требования** | [`requirements/non-functional.md`](requirements/non-functional.md) | Измеримые SLO: консистентность, порядок, throughput, латентность, lag, доступность |
| **Логическая модель данных** | [`data-model/logical-model.md`](data-model/logical-model.md) | UML-диаграмма классов + табличные описания сущностей по шаблону |
| **ERD** | [`erd/erd.puml`](erd/erd.puml), [`erd/README.md`](erd/README.md) | Физическая модель, двойная запись, вычисляемый баланс |
| **Спецификация POST /transfers** | [`api/rest-post-transfers.md`](api/rest-post-transfers.md) | Создание перевода: параметры, примеры, детальная логика работы |
| **Спецификация GET /transfers/{id}** | [`api/rest-get-transfer-status.md`](api/rest-get-transfer-status.md) | Статус перевода (состояние саги) |
| **Спецификация GET /accounts** | [`api/rest-get-accounts.md`](api/rest-get-accounts.md) | Счета и вычисляемые балансы |
| **OpenAPI (REST gateway)** | [`openapi.yaml`](openapi.yaml) | Машиночитаемый контракт REST-входа |
| **AsyncAPI (события Kafka)** | [`asyncapi.yaml`](asyncapi.yaml) | 9 топиков, конверт `EventEnvelope`, payload-схемы, send/receive по каналам |
| **C4: Context / Container** | [`c4/context.puml`](c4/context.puml), [`c4/container.puml`](c4/container.puml) | Система в окружении и её контейнеры со связями |
| **Sequence-диаграммы** | [`sequence/`](sequence/) | Happy path, отказ антифрода, недостаток средств, дубликат по ключу |
| **ADR** | [`adr/`](adr/) | 6 архитектурных решений с альтернативами и последствиями |

## Рекомендуемый порядок чтения

1. [`requirements/functional.md`](requirements/functional.md) – что делает система
   и где её границы; затем детальная
   [user story перевода](requirements/user-story-transfer.md).
2. [`c4/context.puml`](c4/context.puml) → [`c4/container.puml`](c4/container.puml) –
   из чего система состоит.
3. [`sequence/happy_path.puml`](sequence/happy_path.puml) – как проходит успешный
   перевод; следом три сценария отказа/дубликата.
4. Контракты: [спецификации REST-методов](api/) + [`openapi.yaml`](openapi.yaml),
   каталог событий [`asyncapi.yaml`](asyncapi.yaml).
5. Данные: [`data-model/logical-model.md`](data-model/logical-model.md) и
   [`erd/`](erd/).
6. [`adr/`](adr/) – почему выбраны именно эти решения.
7. [`requirements/non-functional.md`](requirements/non-functional.md) – целевые SLO.

## Каталог ADR

- [0001 – Шина событий: Apache Kafka](adr/0001-why-kafka.md)
- [0002 – Партиционирование по счёту-источнику](adr/0002-partitioning-by-account.md)
- [0003 – Двухуровневая стратегия идемпотентности](adr/0003-idempotency-strategy.md)
- [0004 – Transactional Outbox в леджере](adr/0004-outbox-pattern.md)
- [0005 – Деньги как целые минорные единицы](adr/0005-money-as-minor-units.md)
- [0006 – SAGA: оркестрация vs хореография](adr/0006-saga-orchestration-vs-choreography.md)

## Как смотреть диаграммы и контракты

`*.puml` – PlantUML:

- **VS Code**: расширение «PlantUML» (jebbs.plantuml), предпросмотр по `Alt+D`.
  C4-диаграммы подключают библиотеку по `!include https://…` – при рендере нужен
  интернет.
- **IntelliJ IDEA**: плагин «PlantUML Integration».
- **CLI**: `plantuml docs/**/*.puml` (Java + Graphviz) – сгенерирует PNG/SVG.
- **Онлайн**: https://www.plantuml.com/plantuml.

YAML-контракты:

- **AsyncAPI** – AsyncAPI Studio (https://studio.asyncapi.com) или
  `npx @asyncapi/cli validate docs/asyncapi.yaml`.
- **OpenAPI** – Swagger Editor (https://editor.swagger.io) или
  `npx @redocly/cli lint docs/openapi.yaml`; живой Swagger UI запущенного
  gateway – http://localhost:8000/docs.

## Замечания по контракту

- **payment.completed / payment.failed**: полезная нагрузка – модель
  `PaymentOutcomeData` (`shared/events.py`): `{payment_id, status, reason?}`,
  статус в нижнем регистре (`completed`/`failed`) – в отличие от статусов
  `saga_state`, которые пишутся в верхнем (`COMPLETED`/`FAILED`).
- **notification.requested**: `recipient` = id счёта-источника; сервис
  notification резолвит его в `owner_name` по таблице `accounts` (если счёт не
  найден – использует значение как есть). Готовый текст уведомления передаётся
  в событии и имеет приоритет над локальной сборкой текста.
- **ledger.transfer.requested**: orchestrator подставляет `idempotency_key` =
  `payment_id` (исходный клиентский ключ в `saga_state` не хранится). На
  идемпотентность не влияет: леджер идемпотентен по
  `UNIQUE(payment_id, account_id, direction)`.
- **Переходы саги защищены** условными UPDATE: повторная доставка события
  (at-least-once) не откатывает сагу назад и не дублирует терминальные
  события и уведомления.
- Межсервисных внешних ключей по `payment_id` в БД нет намеренно (изоляция
  данных по сервисам) – связи логические, в ERD показаны пунктиром.
