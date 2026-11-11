package no.novari.kafka.starter.consumertracking

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.AbstractConsumerSeekAware
import org.springframework.kafka.listener.BatchInterceptor
import org.springframework.kafka.listener.BatchMessageListener
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback
import org.springframework.kafka.listener.FailedRecordProcessor
import org.springframework.kafka.listener.GenericMessageListener
import org.springframework.kafka.listener.ListenerExecutionFailedException
import org.springframework.kafka.listener.MessageListener
import org.springframework.kafka.listener.RecordInterceptor
import org.springframework.kafka.listener.RetryListener
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

// shortcut: events carry no topic/partition, since every test tracks one container on one single-partition topic.
// Add TopicPartition to the events when a test needs more than one.

data class KeyValue(
    val key: String?,
    val value: String?,
)

data class ExceptionReport(
    val type: KClass<out Throwable>,
    val message: String?,
)

sealed interface Event

data class PartitionsAssigned(
    val offset: Long,
) : Event

data object PartitionsRevoked : Event

data class RecordsPolled(
    val records: List<KeyValue>,
) : Event

data class OffsetsCommitted(
    val offset: Long,
) : Event

data class OffsetCommitFailed(
    val exception: ExceptionReport,
) : Event

data class ListenerInvokedWithRecord(
    val record: KeyValue,
) : Event

data class ListenerSuccessfullyProcessedRecord(
    val record: KeyValue,
) : Event

data class ListenerFailedToProcessRecord(
    val exception: ExceptionReport,
    val record: KeyValue,
) : Event

data class ListenerInvokedWithBatch(
    val records: List<KeyValue>,
) : Event

data class ListenerSuccessfullyProcessedBatch(
    val records: List<KeyValue>,
) : Event

data class ListenerFailedToProcessBatch(
    val exception: ExceptionReport,
    val records: List<KeyValue>,
) : Event

data class RecordDeliveryFailed(
    val exception: ExceptionReport,
    val record: KeyValue,
    val attempt: Int,
) : Event

data class RecordRecovered(
    val record: KeyValue,
) : Event

data class RecordRecoveryFailed(
    val exception: ExceptionReport,
    val record: KeyValue,
) : Event

data class BatchDeliveryFailed(
    val exception: ExceptionReport,
    val records: List<KeyValue>,
    val attempt: Int,
) : Event

data class BatchRecovered(
    val records: List<KeyValue>,
) : Event

data class BatchRecoveryFailed(
    val exception: ExceptionReport,
    val records: List<KeyValue>,
) : Event

data class CustomRecovererInvoked(
    val record: KeyValue,
) : Event

fun committed(offset: Long): (Event) -> Boolean = { it is OffsetsCommitted && it.offset >= offset }

inline fun <reified T : Event> first(): (Event) -> Boolean = { it is T }

// Values are written as JSON strings by the default JSON serializer, so "value1" is read back as value1.
fun ConsumerRecord<*, *>.toKeyValue() = KeyValue(key() as String?, (value() as String?)?.removeSurrounding("\""))

fun ConsumerRecords<*, *>.toKeyValues() = map { it.toKeyValue() }

fun Exception.toReport(): ExceptionReport {
    val exception = if (this is ListenerExecutionFailedException) cause ?: this else this
    return ExceptionReport(exception::class, exception.message)
}

/**
 * Records an ordered trace of what a listener container does, until [stopWhen] matches an event.
 * One instance per container, so tests do not share state.
 */
class ConsumerTracking(
    private val stopWhen: (Event) -> Boolean,
) {
    private val recorded = mutableListOf<Event>()
    private val stopped = CountDownLatch(1)

    val events: List<Event>
        @Synchronized get() = recorded.toList()

    @Synchronized
    fun add(event: Event) {
        if (stopped.count == 0L) return
        recorded += event
        if (stopWhen(event)) stopped.countDown()
    }

    fun await(timeout: Duration = Duration.ofSeconds(30)): Boolean =
        stopped.await(timeout.toMillis(), TimeUnit.MILLISECONDS)

    fun register(
        container: ConcurrentMessageListenerContainer<String, String>,
        errorHandler: CommonErrorHandler,
    ) {
        with(container.containerProperties) {
            kafkaConsumerProperties[ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG] =
                TrackingConsumerInterceptor::class.java.name
            kafkaConsumerProperties[TrackingConsumerInterceptor.TRACKING_CONFIG] = this@ConsumerTracking
            setCommitCallback { _, exception -> exception?.let { add(OffsetCommitFailed(it.toReport())) } }
        }
        container.setRecordInterceptor(recordInterceptor())
        container.setBatchInterceptor(batchInterceptor())
        (errorHandler as? FailedRecordProcessor)?.setRetryListeners(retryListener())
        container.commonErrorHandler = errorHandler
    }

    private fun recordInterceptor() =
        object : RecordInterceptor<String, String> {
            override fun intercept(
                record: ConsumerRecord<String, String>,
                consumer: Consumer<String, String>,
            ) = record.also { add(ListenerInvokedWithRecord(it.toKeyValue())) }

            override fun success(
                record: ConsumerRecord<String, String>,
                consumer: Consumer<String, String>,
            ) = add(ListenerSuccessfullyProcessedRecord(record.toKeyValue()))

            override fun failure(
                record: ConsumerRecord<String, String>,
                exception: Exception,
                consumer: Consumer<String, String>,
            ) = add(ListenerFailedToProcessRecord(exception.toReport(), record.toKeyValue()))
        }

    private fun batchInterceptor() =
        object : BatchInterceptor<String, String> {
            override fun intercept(
                records: ConsumerRecords<String, String>,
                consumer: Consumer<String, String>,
            ) = records.also { add(ListenerInvokedWithBatch(it.toKeyValues())) }

            override fun success(
                records: ConsumerRecords<String, String>,
                consumer: Consumer<String, String>,
            ) = add(ListenerSuccessfullyProcessedBatch(records.toKeyValues()))

            override fun failure(
                records: ConsumerRecords<String, String>,
                exception: Exception,
                consumer: Consumer<String, String>,
            ) = add(ListenerFailedToProcessBatch(exception.toReport(), records.toKeyValues()))
        }

    private fun retryListener() =
        object : RetryListener {
            override fun failedDelivery(
                record: ConsumerRecord<*, *>,
                ex: Exception?,
                deliveryAttempt: Int,
            ) = add(RecordDeliveryFailed(ex!!.toReport(), record.toKeyValue(), deliveryAttempt))

            override fun recovered(
                record: ConsumerRecord<*, *>,
                ex: Exception?,
            ) = add(RecordRecovered(record.toKeyValue()))

            override fun recoveryFailed(
                record: ConsumerRecord<*, *>,
                original: Exception?,
                failure: Exception,
            ) = add(RecordRecoveryFailed(failure.toReport(), record.toKeyValue()))

            override fun failedDelivery(
                records: ConsumerRecords<*, *>,
                ex: Exception,
                deliveryAttempt: Int,
            ) = add(BatchDeliveryFailed(ex.toReport(), records.toKeyValues(), deliveryAttempt))

            override fun recovered(
                records: ConsumerRecords<*, *>,
                ex: Exception,
            ) = add(BatchRecovered(records.toKeyValues()))

            override fun recoveryFailed(
                records: ConsumerRecords<*, *>,
                original: Exception,
                failure: Exception,
            ) = add(BatchRecoveryFailed(failure.toReport(), records.toKeyValues()))
        }
}

/** Gets its [ConsumerTracking] from the consumer config, so no static state is needed. */
class TrackingConsumerInterceptor : ConsumerInterceptor<String, String> {
    private lateinit var tracking: ConsumerTracking

    override fun configure(configs: Map<String, *>) {
        tracking = configs[TRACKING_CONFIG] as ConsumerTracking
    }

    override fun onConsume(records: ConsumerRecords<String, String>) =
        records.also { tracking.add(RecordsPolled(it.toKeyValues())) }

    override fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) =
        offsets.values.forEach { tracking.add(OffsetsCommitted(it.offset())) }

    override fun close() {}

    companion object {
        const val TRACKING_CONFIG = "test.consumer-tracking"
    }
}

/** Reports partition assignments, and lets tests seek on assignment like an app would with [AbstractConsumerSeekAware]. */
abstract class TrackingSeekAware(
    private val tracking: ConsumerTracking,
    private val onAssigned: (Map<TopicPartition, Long>, ConsumerSeekCallback) -> Unit,
) : AbstractConsumerSeekAware() {
    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekCallback,
    ) {
        assignments.values.forEach { tracking.add(PartitionsAssigned(it)) }
        super.onPartitionsAssigned(assignments, callback)
        onAssigned(assignments, callback)
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>?) {
        tracking.add(PartitionsRevoked)
        super.onPartitionsRevoked(partitions)
    }
}

class RecordListener(
    tracking: ConsumerTracking,
    onAssigned: (Map<TopicPartition, Long>, ConsumerSeekCallback) -> Unit = { _, _ -> },
    private val process: (ConsumerRecord<String, String>) -> Unit = {},
) : TrackingSeekAware(tracking, onAssigned),
    MessageListener<String, String> {
    override fun onMessage(record: ConsumerRecord<String, String>) = process(record)
}

class BatchListener(
    tracking: ConsumerTracking,
    private val process: (List<ConsumerRecord<String, String>>) -> Unit,
) : TrackingSeekAware(tracking, { _, _ -> }),
    BatchMessageListener<String, String> {
    override fun onMessage(records: List<ConsumerRecord<String, String>>) = process(records)
}

/** Creates a tracked container from Boot's listener container factory, so the application's Kafka properties apply. */
@Suppress("UNCHECKED_CAST")
fun ConcurrentKafkaListenerContainerFactory<*, *>.trackedContainer(
    topic: String,
    tracking: ConsumerTracking,
    listener: GenericMessageListener<*>,
    errorHandler: CommonErrorHandler,
    groupId: String = topic,
    consumerProperties: Map<String, String> = emptyMap(),
): ConcurrentMessageListenerContainer<String, String> =
    (createContainer(topic) as ConcurrentMessageListenerContainer<String, String>).apply {
        containerProperties.setGroupId(groupId)
        containerProperties.setMessageListener(listener)
        containerProperties.kafkaConsumerProperties.putAll(consumerProperties)
        tracking.register(this, errorHandler)
    }

fun kv(i: Int) = KeyValue("key$i", "value$i")

fun KafkaTemplate<String, String>.sendAll(
    topic: String,
    keys: IntRange,
) = keys.forEach { send(topic, "key$it", "value$it").get() }
