package no.novari.kafka.starter.consumer

import no.novari.kafka.starter.consumertracking.BatchDeliveryFailed
import no.novari.kafka.starter.consumertracking.BatchListener
import no.novari.kafka.starter.consumertracking.BatchRecovered
import no.novari.kafka.starter.consumertracking.ConsumerTracking
import no.novari.kafka.starter.consumertracking.Event
import no.novari.kafka.starter.consumertracking.ExceptionReport
import no.novari.kafka.starter.consumertracking.ListenerFailedToProcessBatch
import no.novari.kafka.starter.consumertracking.ListenerInvokedWithBatch
import no.novari.kafka.starter.consumertracking.ListenerSuccessfullyProcessedBatch
import no.novari.kafka.starter.consumertracking.OffsetsCommitted
import no.novari.kafka.starter.consumertracking.PartitionsAssigned
import no.novari.kafka.starter.consumertracking.RecordDeliveryFailed
import no.novari.kafka.starter.consumertracking.RecordRecovered
import no.novari.kafka.starter.consumertracking.RecordsPolled
import no.novari.kafka.starter.consumertracking.committed
import no.novari.kafka.starter.consumertracking.kv
import no.novari.kafka.starter.consumertracking.sendAll
import no.novari.kafka.starter.consumertracking.trackedContainer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.BatchListenerFailedException
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.util.backoff.FixedBackOff
import java.util.UUID

/** Commit granularity, retry and recovery for batch listeners with Spring's [DefaultErrorHandler]. */
@Tag("slow")
@SpringBootTest
@EmbeddedKafka(partitions = 1)
class BatchProcessingTest(
    @Autowired private val factory: ConcurrentKafkaListenerContainerFactory<*, *>,
    @Autowired private val template: KafkaTemplate<String, String>,
) {
    class Case(
        private val name: String,
        val expected: List<Event>,
        val failures: Map<Int, Int> = emptyMap(),
        val batchListenerFailedException: Boolean = true,
        val retries: Long = 1,
    ) {
        override fun toString() = name
    }

    @ParameterizedTest
    @MethodSource("cases")
    fun `processes batches`(case: Case) {
        val topic = UUID.randomUUID().toString()
        val tracking = ConsumerTracking(committed(3))
        val remainingFailures = case.failures.toMutableMap()
        val listener =
            BatchListener(tracking) { records ->
                records.forEachIndexed { index, record ->
                    val key = record.key().removePrefix("key").toInt()
                    val remaining = remainingFailures[key] ?: 0
                    if (remaining > 0) {
                        remainingFailures[key] = remaining - 1
                        throw if (case.batchListenerFailedException) {
                            BatchListenerFailedException(
                                "failed",
                                index,
                            )
                        } else {
                            RuntimeException()
                        }
                    }
                }
            }
        val container =
            factory.trackedContainer(
                topic,
                tracking,
                listener,
                DefaultErrorHandler(FixedBackOff(10, case.retries)),
                consumerProperties = mapOf(ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "3"),
            )
        template.sendAll(topic, 1..3)

        container.start()
        try {
            assertThat(tracking.await()).isTrue()
            assertThat(tracking.events).isEqualTo(case.expected)
        } finally {
            container.stop()
        }
    }

    companion object {
        private val runtime = ExceptionReport(RuntimeException::class, null)

        // Spring adds the index of the failed record to the message.
        private fun batchFailed(index: Int) = ExceptionReport(BatchListenerFailedException::class, "failed @-$index")

        private fun records(vararg keys: Int) = keys.map(::kv)

        private fun ok(vararg keys: Int) =
            listOf(
                RecordsPolled(records(*keys)),
                ListenerInvokedWithBatch(records(*keys)),
                ListenerSuccessfullyProcessedBatch(records(*keys)),
            )

        private fun failed(
            exception: ExceptionReport,
            vararg keys: Int,
        ) = listOf(ListenerInvokedWithBatch(records(*keys)), ListenerFailedToProcessBatch(exception, records(*keys)))

        @JvmStatic
        fun cases() =
            listOf(
                Case(
                    "commits the whole batch",
                    expected = listOf(PartitionsAssigned(0)) + ok(1, 2, 3) + OffsetsCommitted(3),
                ),
                Case(
                    "commits the records before a BatchListenerFailedException and retries from the failed record",
                    failures = mapOf(2 to 1),
                    expected =
                        listOf(
                            PartitionsAssigned(0),
                            RecordsPolled(records(1, 2, 3)),
                        ) + failed(batchFailed(1), 1, 2, 3) +
                            listOf(
                                BatchDeliveryFailed(batchFailed(1), records(1, 2, 3), 1),
                                OffsetsCommitted(1),
                                RecordDeliveryFailed(batchFailed(1), kv(2), 1),
                            ) + ok(2, 3) + OffsetsCommitted(3),
                ),
                Case(
                    "recovers the failed record when retries are exhausted, and continues with the rest",
                    failures = mapOf(2 to 10),
                    expected =
                        listOf(
                            PartitionsAssigned(0),
                            RecordsPolled(records(1, 2, 3)),
                        ) + failed(batchFailed(1), 1, 2, 3) +
                            listOf(
                                BatchDeliveryFailed(batchFailed(1), records(1, 2, 3), 1),
                                OffsetsCommitted(1),
                                RecordDeliveryFailed(batchFailed(1), kv(2), 1),
                                RecordsPolled(records(2, 3)),
                            ) + failed(batchFailed(0), 2, 3) +
                            listOf(
                                BatchDeliveryFailed(batchFailed(0), records(2, 3), 1),
                                RecordDeliveryFailed(batchFailed(0), kv(2), 2),
                                RecordRecovered(kv(2)),
                                OffsetsCommitted(2),
                            ) + ok(3) + OffsetsCommitted(3),
                ),
                Case(
                    "retries the whole batch on other exceptions",
                    failures = mapOf(2 to 1),
                    batchListenerFailedException = false,
                    expected =
                        listOf(PartitionsAssigned(0), RecordsPolled(records(1, 2, 3))) + failed(runtime, 1, 2, 3) +
                            BatchDeliveryFailed(runtime, records(1, 2, 3), 1) + ok(1, 2, 3).drop(1) +
                            OffsetsCommitted(3),
                ),
                Case(
                    "recovers the whole batch on other exceptions when retries are exhausted",
                    failures = mapOf(2 to 10),
                    batchListenerFailedException = false,
                    expected =
                        listOf(PartitionsAssigned(0), RecordsPolled(records(1, 2, 3))) + failed(runtime, 1, 2, 3) +
                            BatchDeliveryFailed(runtime, records(1, 2, 3), 1) + failed(runtime, 1, 2, 3) +
                            listOf(
                                BatchDeliveryFailed(runtime, records(1, 2, 3), 2),
                                BatchRecovered(records(1, 2, 3)),
                                OffsetsCommitted(3),
                            ),
                ),
            )
    }
}
