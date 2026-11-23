#pragma once

#include <Columns/IColumn_fwd.h>
#include <Storages/IMessageProducer.h>
#include <Storages/Kafka/KafkaProducerPool.h>
#include <cppkafka/cppkafka.h>

#include <optional>

namespace DB
{
class Block;

/// Produces the messages of one `INSERT`. The `librdkafka` producer comes from `KafkaProducerPool` and may be shared with
/// other `INSERT` queries, so `finish` waits only for the delivery reports of the messages of this `INSERT`.
class KafkaProducer : public IMessageProducer
{
public:
    KafkaProducer(
        KafkaProducerPool & pool,
        const std::string & topic_,
        std::chrono::milliseconds poll_timeout,
        std::atomic<bool> & shutdown_called_,
        const Block & header,
        bool map_virtual_columns_on_write = false);

    ~KafkaProducer() override;

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;

    void start(const ContextPtr &) override {}
    void finish() override;
    void cancel() noexcept override;

private:
    /// Taken from the pool when `write` builds the sink, as before, so that a new producer connects to the brokers while the
    /// pipeline starts.
    const KafkaProducerHandlePtr producer;
    const KafkaDeliveryTrackerPtr deliveries;

    const std::string topic;
    const std::chrono::milliseconds timeout;

    /// Belongs to the storage, which the `INSERT` pipeline keeps alive.
    std::atomic<bool> & shutdown_called;

    /// `MessageQueueSink::onException` calls `finish` again after an exception, also after one from `finish`.
    bool finished = false;

    std::optional<size_t> key_column_index;
    std::optional<size_t> timestamp_column_index;
    std::optional<size_t> headers_name_column_index;
    std::optional<size_t> headers_value_column_index;
};

}
