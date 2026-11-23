#include <Storages/Kafka/KafkaProducerPool.h>

#include <Core/BackgroundSchedulePool.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

namespace
{

/// How often the shared producer is polled and checked for the idle timeout.
constexpr size_t PRODUCER_CLEANUP_INTERVAL_MS = 500;

/// Bounds the wait for the delivery reports of the purged messages when a producer is closed.
constexpr int PURGED_DELIVERY_REPORTS_TIMEOUT_MS = 5000;

cppkafka::Configuration prepareConfiguration(cppkafka::Configuration configuration)
{
    /// Every message needs its delivery report to release its tracker.
    configuration.set("delivery.report.only.error", "false");
    /// Nothing reads the statistics of a producer.
    configuration.set("statistics.interval.ms", "0");
    rd_kafka_conf_set_dr_msg_cb(configuration.get_handle(), &KafkaDeliveryTracker::onDeliveryReport);
    return configuration;
}

}

void * KafkaDeliveryTracker::messageQueued()
{
    intrusive_ptr_add_ref(this);
    pending_messages.fetch_add(1);
    return this;
}

void KafkaDeliveryTracker::messageNotQueued()
{
    pending_messages.fetch_sub(1);
    intrusive_ptr_release(this);
}

void KafkaDeliveryTracker::onDeliveryReport(rd_kafka_t *, const rd_kafka_message_t * message, void *)
{
    auto * tracker = static_cast<KafkaDeliveryTracker *>(message->_private);
    chassert(tracker);

    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        auto no_error = RD_KAFKA_RESP_ERR_NO_ERROR;
        tracker->first_error.compare_exchange_strong(no_error, message->err);
        tracker->failed_messages.fetch_add(1);
    }

    /// The `INSERT` reads the errors after it sees no pending messages.
    tracker->pending_messages.fetch_sub(1);
    intrusive_ptr_release(tracker);
}

KafkaProducerHandle::KafkaProducerHandle(cppkafka::Configuration configuration, LoggerPtr log_)
    : log(std::move(log_))
    , producer(prepareConfiguration(std::move(configuration)))
{
}

KafkaProducerHandle::~KafkaProducerHandle()
{
    rd_kafka_t * handle = getHandle();
    if (rd_kafka_outq_len(handle) == 0)
        return;

    rd_kafka_purge(handle, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
    if (rd_kafka_flush(handle, PURGED_DELIVERY_REPORTS_TIMEOUT_MS) != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_WARNING(log, "{} messages or events are still in the queue of the Kafka producer that is being closed",
            rd_kafka_outq_len(handle));
}

bool KafkaProducerHandle::isReusable() const
{
    return !invalidated && rd_kafka_fatal_error(getHandle(), nullptr, 0) == RD_KAFKA_RESP_ERR_NO_ERROR;
}

KafkaProducerPool::KafkaProducerPool(
    ConfigurationFactory make_configuration_,
    std::chrono::milliseconds idle_timeout_,
    BackgroundSchedulePool & schedule_pool,
    const StorageID & storage_id,
    LoggerPtr log_)
    : make_configuration(std::move(make_configuration_))
    , idle_timeout(idle_timeout_)
    , log(std::move(log_))
{
    if (idle_timeout.count() > 0)
        cleanup_task = schedule_pool.createTask(storage_id, log->name() + " (producer cleanup)", [this] { closeIdleProducer(); });
}

KafkaProducerHandlePtr KafkaProducerPool::createProducer() const
{
    auto producer = std::make_shared<KafkaProducerHandle>(make_configuration(), log);
    LOG_TRACE(log, "Kafka producer created");
    return producer;
}

KafkaProducerHandlePtr KafkaProducerPool::tryGetSharedProducer()
{
    std::lock_guard lock(mutex);
    if (is_shutdown)
        throw Exception(ErrorCodes::ABORTED, "Table is detached");

    if (shared_producer && shared_producer->isReusable())
        return shared_producer;

    return nullptr;
}

KafkaProducerHandlePtr KafkaProducerPool::acquire()
{
    if (idle_timeout.count() == 0)
    {
        {
            std::lock_guard lock(mutex);
            if (is_shutdown)
                throw Exception(ErrorCodes::ABORTED, "Table is detached");
        }
        return createProducer();
    }

    if (auto producer = tryGetSharedProducer())
        return producer;

    std::lock_guard creation_lock(creation_mutex);
    if (auto producer = tryGetSharedProducer())
        return producer;

    auto producer = createProducer();
    /// The previous producer is not reusable. The last `INSERT` that still uses it closes it, otherwise it is closed here.
    KafkaProducerHandlePtr previous_producer;
    {
        std::lock_guard lock(mutex);
        if (is_shutdown)
            throw Exception(ErrorCodes::ABORTED, "Table is detached");
        previous_producer = std::exchange(shared_producer, producer);
    }

    cleanup_task->scheduleAfter(PRODUCER_CLEANUP_INTERVAL_MS, /* overwrite= */ false);
    return producer;
}

void KafkaProducerPool::closeIdleProducer()
{
    KafkaProducerHandlePtr producer;
    bool reusable = true;
    bool idle = false;
    {
        std::lock_guard lock(mutex);
        if (is_shutdown || !shared_producer)
            return;

        /// `acquire` copies the producer only under the mutex, so no `INSERT` uses it while the pool holds the only reference.
        if (shared_producer.use_count() == 1)
        {
            reusable = shared_producer->isReusable();
            idle = shared_producer->idleFor() >= idle_timeout;
        }

        if (!reusable || idle)
            producer = std::move(shared_producer);
        else
            producer = shared_producer;
    }

    if (!reusable)
    {
        LOG_TRACE(log, "Closing the Kafka producer after an error");
        return;
    }

    if (idle)
    {
        LOG_TRACE(log, "Closing the Kafka producer, which is not used for {} ms",
            std::chrono::duration_cast<std::chrono::milliseconds>(producer->idleFor()).count());
        return;
    }

    /// Serves what is queued while no `INSERT` polls the producer, e.g. the refresh of an OAuth token
    /// and the delivery reports of the messages of a cancelled `INSERT`.
    rd_kafka_poll(producer->getHandle(), 0);
    cleanup_task->scheduleAfter(PRODUCER_CLEANUP_INTERVAL_MS);
}

void KafkaProducerPool::shutdown()
{
    if (cleanup_task)
        cleanup_task->deactivate();

    KafkaProducerHandlePtr producer;
    {
        std::lock_guard lock(mutex);
        is_shutdown = true;
        producer = std::move(shared_producer);
    }
}

}
