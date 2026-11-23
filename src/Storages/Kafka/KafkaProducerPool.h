#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Common/CurrentMetrics.h>
#include <Common/Logger.h>
#include <base/defines.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <cppkafka/configuration.h>
#include <cppkafka/producer.h>
#include <librdkafka/rdkafka.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>

namespace CurrentMetrics
{
    extern const Metric KafkaProducers;
}

namespace DB
{

class BackgroundSchedulePool;
struct StorageID;

/// Counts the messages of one `INSERT` that wait for their delivery reports, and the failed ones.
/// Every message holds a reference to the tracker until its delivery report is served, possibly by another thread
/// that polls the same producer, so the tracker outlives an `INSERT` that does not wait for its messages.
class KafkaDeliveryTracker : public boost::intrusive_ref_counter<KafkaDeliveryTracker, boost::thread_safe_counter>
{
public:
    /// Called before a message is passed to the producer. Returns the opaque of the message.
    void * messageQueued();
    /// Called when the producer did not take the message, so no delivery report will come for it.
    void messageNotQueued();

    /// The delivery report callback of `KafkaProducerHandle`.
    static void onDeliveryReport(rd_kafka_t *, const rd_kafka_message_t * message, void *);

    size_t pending() const { return pending_messages.load(); }
    size_t failed() const { return failed_messages.load(); }
    rd_kafka_resp_err_t firstError() const { return first_error.load(); }

private:
    std::atomic<size_t> pending_messages = 0;
    std::atomic<size_t> failed_messages = 0;
    std::atomic<rd_kafka_resp_err_t> first_error = RD_KAFKA_RESP_ERR_NO_ERROR;
};

using KafkaDeliveryTrackerPtr = boost::intrusive_ptr<KafkaDeliveryTracker>;

/// A `librdkafka` producer. Every message produced through it must have a `KafkaDeliveryTracker` as its opaque.
class KafkaProducerHandle
{
public:
    KafkaProducerHandle(cppkafka::Configuration configuration, LoggerPtr log_);

    /// Purges the messages that still wait for delivery and serves their delivery reports,
    /// because `rd_kafka_destroy` drops them without the reports, so their trackers would never be released.
    ~KafkaProducerHandle();

    cppkafka::Producer & getProducer() { return producer; }
    rd_kafka_t * getHandle() const { return producer.get_handle(); }

    /// After an error the producer is not given to new `INSERT` queries, so that the next `INSERT` gets a new one,
    /// which also applies a fixed configuration, e.g. a new password in a named collection.
    void invalidate() { invalidated = true; }
    /// False after `invalidate` or after a fatal error, e.g. of an idempotent producer.
    bool isReusable() const;

    /// Called when an `INSERT` stops using the producer.
    void touch() { last_used_at = std::chrono::steady_clock::now(); }
    std::chrono::steady_clock::duration idleFor() const { return std::chrono::steady_clock::now() - last_used_at.load(); }

private:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaProducers};
    LoggerPtr log;
    cppkafka::Producer producer;
    std::atomic<bool> invalidated = false;
    std::atomic<std::chrono::steady_clock::time_point> last_used_at = std::chrono::steady_clock::now();
};

using KafkaProducerHandlePtr = std::shared_ptr<KafkaProducerHandle>;

/// Gives producers to the `INSERT` queries into one Kafka table.
/// With a zero `idle_timeout`, every `INSERT` gets a new producer. Otherwise, the `INSERT` queries share one producer,
/// so that they do not pay for creating it and connecting to the brokers every time, and it is closed after it has
/// not been used for `idle_timeout`. `librdkafka` producers are thread-safe, and each `INSERT` waits only for its own
/// messages, see `KafkaDeliveryTracker`.
class KafkaProducerPool
{
public:
    using ConfigurationFactory = std::function<cppkafka::Configuration()>;

    KafkaProducerPool(
        ConfigurationFactory make_configuration_,
        std::chrono::milliseconds idle_timeout_,
        BackgroundSchedulePool & schedule_pool,
        const StorageID & storage_id,
        LoggerPtr log_);

    /// Returns the shared producer if it is reusable, otherwise creates one. Throws `ABORTED` after `shutdown`.
    KafkaProducerHandlePtr acquire();

    /// Closes the shared producer. The `INSERT` queries that use it keep their references, and the last one closes it.
    void shutdown();

private:
    KafkaProducerHandlePtr createProducer() const;
    KafkaProducerHandlePtr tryGetSharedProducer();
    void closeIdleProducer();

    const ConfigurationFactory make_configuration;
    const std::chrono::milliseconds idle_timeout;
    LoggerPtr log;

    /// Only one `INSERT` creates the shared producer, the others wait for it. `shutdown` and `closeIdleProducer` do not
    /// take this mutex, because building the configuration may access the network, e.g. for Kerberos.
    std::mutex creation_mutex;

    /// Producers are closed outside of this mutex.
    std::mutex mutex;
    KafkaProducerHandlePtr shared_producer TSA_GUARDED_BY(mutex);
    bool is_shutdown TSA_GUARDED_BY(mutex) = false;

    /// Declared last, so that it is deactivated before the members it uses are destroyed.
    BackgroundSchedulePoolTaskHolder cleanup_task;
};

}
