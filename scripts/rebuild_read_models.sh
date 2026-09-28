#!/bin/sh
# Rebuild vitals-query's read models from scratch by replaying the Kafka topic.
#
#   1. vitals-query must be stopped (Kafka refuses to move a consumer group's position while it has
#      active members).
#   2. Empty the read-model tables, including applied_events (otherwise every replayed event would be
#      skipped as "already applied").
#   3. Move the vitals-query consumer group back to the first offset of every partition.
#   4. Start vitals-query: it consumes the whole topic again and rebuilds the tables.
#
# While the rebuild runs, the query API returns incomplete data. A production system would rebuild into
# new tables (or a new schema) with a new consumer group, and switch reads over once it has caught up
# ("blue/green" rebuild), so readers never see a half-built model.
#
# Only events still in Kafka come back: rows loaded straight into Postgres (the synthetic datasets) are
# not events, and the topic must retain everything (retention.ms=-1) for a full rebuild.
set -e

if pgrep -f 'vitals-query-0.0.1-SNAPSHOT.jar|com.vitalstream.query.VitalsQueryApplication' >/dev/null; then
  echo "vitals-query is running; stop it first (Ctrl+C in its terminal)." >&2
  exit 1
fi

echo "== emptying read models"
docker exec vitalstream-postgres psql -U vitalstream -c \
  'TRUNCATE readmodel.patient_latest_vitals, readmodel.device_minute_stats, readmodel.applied_events;'

echo "== resetting consumer group vitals-query to the start of the topic"
docker exec vitalstream-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group vitals-query --topic vitals.readings.avro --reset-offsets --to-earliest --execute

echo "Done. Start vitals-query to replay the topic and rebuild the read models."
