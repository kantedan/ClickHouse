#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Kafka is not available in fast tests
# Tag no-replicated-database: named collections created by SQL are not replicated

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `kafka_producer_idle_timeout_ms` the INSERT queries into a Kafka table share one producer. Each INSERT must still
# deliver all of its rows, also while other ones run or fail, the INSERT after a failed one must work, and DROP must close
# the producer, also while an INSERT uses it.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
topic=$(echo "${name}" | tr '_' '-')
keeper_topic="${topic}-keeper"
broker="127.0.0.1:9092"

cleanup()
{
    local exit_code=$?

    trap - EXIT INT TERM
    set +e

    for table in shared keeper; do
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${name}_${table}" 2>/dev/null
        $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${name}_${table}" 2>/dev/null
    done
    for t in "$topic" "$keeper_topic"; do
        timeout 10 rpk topic delete "$t" --brokers "$broker" > /dev/null 2>&1
    done

    exit $exit_code
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# $1: topic. Prints the end offset of partition 0, which is the number of messages in the topic.
end_offset()
{
    timeout 10 rpk topic describe "$1" -p --brokers "$broker" 2>/dev/null | awk '$1 == "0" {print $NF}'
}

# $1: topic, $2: number of messages, $3: structure of the messages. Prints the number of rows, of distinct `x`, and their sum.
consume()
{
    timeout 60 rpk topic consume "$1" --brokers "$broker" --offset start --num "$2" --format '%v\n' \
        | $CLICKHOUSE_LOCAL --input-format JSONEachRow --structure "$3" -q "SELECT count(), uniqExact(x), sum(x) FROM table"
}

rpk topic create "$topic" -p 1 --brokers "$broker" > /dev/null 2>&1
rpk topic create "$keeper_topic" -p 1 --brokers "$broker" > /dev/null 2>&1

$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION ${name}_shared AS
        kafka_broker_list = '${broker}', kafka_topic_list = '${topic}', kafka_group_name = '${name}', kafka_format = 'JSONEachRow',
        \`kafka.message_max_bytes\` = '10000';
    CREATE NAMED COLLECTION ${name}_keeper AS
        kafka_broker_list = '${broker}', kafka_topic_list = '${keeper_topic}', kafka_group_name = '${name}', kafka_format = 'JSONEachRow',
        \`kafka.queue_buffering_max_messages\` = '1', \`kafka.linger_ms\` = '60000';
    CREATE TABLE ${name}_shared (x UInt32, s String) ENGINE = Kafka(${name}_shared) SETTINGS kafka_producer_idle_timeout_ms = 600000"
$CLICKHOUSE_CLIENT --allow_kafka_offsets_storage_in_keeper 1 -q "
    CREATE TABLE ${name}_keeper (x UInt32) ENGINE = Kafka(${name}_keeper)
        SETTINGS kafka_keeper_path = '/clickhouse/test/${name}', kafka_replica_name = 'r1', kafka_producer_idle_timeout_ms = 600000"

for i in 0 1 2; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_shared SETTINGS async_insert = 0 SELECT number + $((i * 10)), '' FROM numbers(10)"
done

# The message of one INSERT is larger than `message.max.bytes`, so it fails while the others run.
for i in {1..8}; do
    $CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_shared SETTINGS async_insert = 0 SELECT number + $((i * 1000)), '' FROM numbers(1000)" &
done
too_large=$($CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_shared SETTINGS async_insert = 0 SELECT 100000, repeat('a', 20000)" 2>&1)
wait
echo "too large: $(grep -m1 -c 'Message size too large' <<< "$too_large")"

# The error made the shared producer not reusable, so the INSERT queries after it get a new one.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_shared SETTINGS async_insert = 0 SELECT number + 9000, '' FROM numbers(10)"

# Each row is delivered exactly once: 30 + 8000 + 10 messages.
echo "shared: $(consume "$topic" 8040 'x UInt32, s String'), end offset: $(end_offset "$topic")"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${name}_shared"

# The second INSERT shares the producer of the first one, and waits for room in the one-message queue when DROP closes it.
$CLICKHOUSE_CLIENT -q "INSERT INTO ${name}_keeper SETTINGS async_insert = 0 VALUES (1)"
echo "keeper: $(consume "$keeper_topic" 1 'x UInt32')"
query_id="${name}_keeper_insert"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "INSERT INTO ${name}_keeper SETTINGS async_insert = 0 VALUES (2), (3)" > "${CLICKHOUSE_TMP}/${name}_keeper.out" 2>&1 &
for _ in {1..300}; do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['KafkaMessagesProduced'] >= 1 FROM system.processes WHERE query_id = '$query_id'")" = 1 ] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "DROP TABLE ${name}_keeper"
wait
echo "keeper drop: $(grep -m1 -c 'ABORTED' "${CLICKHOUSE_TMP}/${name}_keeper.out")"
