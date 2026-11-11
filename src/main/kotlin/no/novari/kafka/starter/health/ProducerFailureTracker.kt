package no.novari.kafka.starter.health

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.springframework.kafka.support.LoggingProducerListener
import org.springframework.kafka.support.ProducerListener
import java.time.Duration
import java.util.ArrayDeque

/**
 * Counts failed sends within a time window. Replaces Boot's [LoggingProducerListener] on the `KafkaTemplate`, so it
 * still logs every failure through it.
 *
 * Ported from `no.novari.kafka.health.ProducerFailureTracker` in fint-kafka 6.3.1 (commit `1ff1899`).
 */
internal class ProducerFailureTracker(
    private val failureThreshold: Int,
    failureWindow: Duration,
) : ProducerListener<Any, Any> {
    private val windowMillis: Long = failureWindow.toMillis()
    private val failureTimestamps = ArrayDeque<Long>()
    private val logging = LoggingProducerListener<Any, Any>()

    override fun onError(
        producerRecord: ProducerRecord<Any, Any>,
        recordMetadata: RecordMetadata?,
        exception: Exception,
    ) {
        synchronized(failureTimestamps) {
            failureTimestamps.addLast(System.currentTimeMillis())
            prune()
        }
        logging.onError(producerRecord, recordMetadata, exception)
    }

    fun isUnhealthy(): Boolean =
        synchronized(failureTimestamps) {
            prune()
            failureTimestamps.size >= failureThreshold
        }

    private fun prune() {
        val cutoff = System.currentTimeMillis() - windowMillis
        while (failureTimestamps.isNotEmpty() && failureTimestamps.peekFirst() < cutoff) {
            failureTimestamps.pollFirst()
        }
    }
}
