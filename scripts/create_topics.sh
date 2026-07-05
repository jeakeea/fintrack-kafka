#!/usr/bin/env bash
# Создать топики с несколькими партициями (история про ordering by from_account).
# Auto-create в compose включён, но для реальных партиций топики лучше завести явно.
set -euo pipefail

CONTAINER="${CONTAINER:-fintrack-kafka}"
BROKER="${BROKER:-kafka:9092}"
PARTITIONS="${PARTITIONS:-6}"

TOPICS=(
  payment.initiated payment.completed payment.failed
  antifraud.check.requested antifraud.check.completed
  ledger.transfer.requested ledger.transfer.completed ledger.transfer.failed
  notification.requested
)

create() {
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}

for t in "${TOPICS[@]}"; do
  create "$t" "$PARTITIONS"
  create "$t.dlq" 1
done

echo "Готово. Список топиков:"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list
