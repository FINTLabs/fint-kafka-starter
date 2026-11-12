package no.novari.kafka.starter.consumer

import no.novari.kafka.starter.consumertracking.ConsumerTracking
import no.novari.kafka.starter.consumertracking.CustomRecovererInvoked
import no.novari.kafka.starter.consumertracking.Event
import no.novari.kafka.starter.consumertracking.ExceptionReport
import no.novari.kafka.starter.consumertracking.ListenerFailedToProcessRecord
import no.novari.kafka.starter.consumertracking.ListenerInvokedWithRecord
import no.novari.kafka.starter.consumertracking.ListenerSuccessfullyProcessedRecord
import no.novari.kafka.starter.consumertracking.OffsetsCommitted
import no.novari.kafka.starter.consumertracking.PartitionsAssigned
import no.novari.kafka.starter.consumertracking.RecordDeliveryFailed
import no.novari.kafka.starter.consumertracking.RecordListener
import no.novari.kafka.starter.consumertracking.RecordRecovered
import no.novari.kafka.starter.consumertracking.RecordRecoveryFailed
import no.novari.kafka.starter.consumertracking.RecordsPolled
import no.novari.kafka.starter.consumertracking.committed
import no.novari.kafka.starter.consumertracking.kv
import no.novari.kafka.starter.consumertracking.sendAll
import no.novari.kafka.starter.consumertracking.toKeyValue
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
import org.springframework.kafka.listener.ConsumerRecordRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.messaging.converter.MessageConversionException
import org.springframework.util.backoff.FixedBackOff
import java.util.UUID

/** Commit granularity, retry and recovery for record listeners with Spring's [DefaultErrorHandler]. */
@Tag("slow")
@SpringBootTest
@EmbeddedKafka(partitions = 1)
class RecordProcessingTest(
    @Autowired private val factory: ConcurrentKafkaListenerContainerFactory<*, *>,
    @Autowired private val template: KafkaTemplate<String, String>,
) {
    class Case(
        private val name: String,
        val expected: List<Event>,
        val messages: Int = 1,
        val maxPollRecords: Int = 3,
        val failures: Map<Int, List<() -> Exception>> = emptyMap(),
        val errorHandler: (ConsumerTracking) -> DefaultErrorHandler,
    ) {
        override fun toString() = name
    }

    @ParameterizedTest
    @MethodSource("cases")
    fun `processes records`(case: Case) {
        val topic = UUID.randomUUID().toString()
        val tracking = ConsumerTracking(committed(case.messages.toLong()))
        val remainingFailures = case.failures.mapValues { ArrayDeque(it.value) }
        val listener =
            RecordListener(tracking) { record ->
                remainingFailures[record.key().removePrefix("key").toInt()]?.removeFirstOrNull()?.let { throw it() }
            }
        val container =
            factory.trackedContainer(
                topic,
                tracking,
                listener,
                case.errorHandler(tracking),
                consumerProperties = mapOf(ConsumerConfig.MAX_POLL_RECORDS_CONFIG to case.maxPollRecords.toString()),
            )
        template.sendAll(topic, 1..case.messages)

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
        private val illegalArgument = ExceptionReport(IllegalArgumentException::class, null)
        private val recoveryFailed = ExceptionReport(IllegalStateException::class, "recovery failed")

        private fun polled(vararg keys: Int) = RecordsPolled(keys.map(::kv))

        private fun ok(key: Int) =
            listOf(ListenerInvokedWithRecord(kv(key)), ListenerSuccessfullyProcessedRecord(kv(key)))

        private fun failed(
            key: Int,
            exception: ExceptionReport = runtime,
        ) = listOf(ListenerInvokedWithRecord(kv(key)), ListenerFailedToProcessRecord(exception, kv(key)))

        private fun deliveryFailed(
            key: Int,
            attempt: Int,
            exception: ExceptionReport = runtime,
        ) = RecordDeliveryFailed(exception, kv(key), attempt)

        private fun times(
            n: Int,
            exception: () -> Exception = ::RuntimeException,
        ) = List(n) { exception }

        private fun fixedBackOff(retries: Long) =
            { _: ConsumerTracking -> DefaultErrorHandler(FixedBackOff(10, retries)) }

        /** A recoverer that fails on its first call, and records every call. */
        private fun failingOnceRecoverer(tracking: ConsumerTracking): ConsumerRecordRecoverer {
            var calls = 0
            return ConsumerRecordRecoverer { record, _ ->
                tracking.add(CustomRecovererInvoked(record.toKeyValue()))
                if (calls++ == 0) throw IllegalStateException("recovery failed")
            }
        }

        @JvmStatic
        fun cases() =
            listOf(
                Case(
                    "commits once per poll when max.poll.records is 3",
                    messages = 3,
                    errorHandler = fixedBackOff(0),
                    expected =
                        listOf(PartitionsAssigned(0), polled(1, 2, 3)) + ok(1) + ok(2) + ok(3) + OffsetsCommitted(3),
                ),
                Case(
                    "commits per record when max.poll.records is 1",
                    messages = 3,
                    maxPollRecords = 1,
                    errorHandler = fixedBackOff(0),
                    expected =
                        listOf(PartitionsAssigned(0)) +
                            (1..3).flatMap { listOf(polled(it)) + ok(it) + OffsetsCommitted(it.toLong()) },
                ),
                Case(
                    "retries a record that fails once, after committing the records before it",
                    messages = 3,
                    failures = mapOf(2 to times(1)),
                    errorHandler = fixedBackOff(1),
                    expected =
                        listOf(PartitionsAssigned(0), polled(1, 2, 3)) + ok(1) + failed(2) +
                            listOf(OffsetsCommitted(1), deliveryFailed(2, 1), polled(2, 3)) + ok(2) + ok(3) +
                            OffsetsCommitted(3),
                ),
                Case(
                    "retries until the record succeeds when there are more retries than failures",
                    messages = 3,
                    failures = mapOf(2 to times(3)),
                    errorHandler = fixedBackOff(3),
                    expected =
                        listOf(PartitionsAssigned(0), polled(1, 2, 3)) + ok(1) + failed(2) +
                            listOf(OffsetsCommitted(1), deliveryFailed(2, 1), polled(2, 3)) + failed(2) +
                            listOf(deliveryFailed(2, 2), polled(2, 3)) + failed(2) +
                            listOf(deliveryFailed(2, 3), polled(2, 3)) + ok(2) + ok(3) + OffsetsCommitted(3),
                ),
                Case(
                    "recovers the record when retries are exhausted, and continues with the next",
                    messages = 3,
                    failures = mapOf(2 to times(3)),
                    errorHandler = fixedBackOff(2),
                    expected =
                        listOf(PartitionsAssigned(0), polled(1, 2, 3)) + ok(1) + failed(2) +
                            listOf(OffsetsCommitted(1), deliveryFailed(2, 1), polled(2, 3)) + failed(2) +
                            listOf(deliveryFailed(2, 2), polled(2, 3)) + failed(2) +
                            listOf(
                                deliveryFailed(2, 3),
                                RecordRecovered(kv(2)),
                                OffsetsCommitted(2),
                                polled(3),
                            ) + ok(3) +
                            OffsetsCommitted(3),
                ),
                Case(
                    "skips a failed record without retries with FixedBackOff(0, 0)",
                    messages = 3,
                    failures = mapOf(2 to times(1)),
                    errorHandler = { DefaultErrorHandler(FixedBackOff(0, 0)) },
                    // Without retries, Spring reports no failed delivery, only the recovery.
                    expected =
                        listOf(PartitionsAssigned(0), polled(1, 2, 3)) + ok(1) + failed(2) +
                            listOf(
                                OffsetsCommitted(1),
                                RecordRecovered(kv(2)),
                                OffsetsCommitted(2),
                                polled(3),
                            ) + ok(3) +
                            OffsetsCommitted(3),
                ),
                Case(
                    "tries 10 times and then skips with Spring's default error handler",
                    failures = mapOf(1 to times(10)),
                    errorHandler = { DefaultErrorHandler() },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) +
                            (1..9).flatMap { failed(1) + listOf(deliveryFailed(1, it), polled(1)) } +
                            failed(1) + listOf(deliveryFailed(1, 10), RecordRecovered(kv(1)), OffsetsCommitted(1)),
                ),
                Case(
                    "restarts the retry count when the exception type changes (Spring default)",
                    failures = mapOf(1 to times(2) + ::IllegalArgumentException),
                    errorHandler = fixedBackOff(2),
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) +
                            failed(1) + listOf(deliveryFailed(1, 1), polled(1)) +
                            failed(1) + listOf(deliveryFailed(1, 2), polled(1)) +
                            failed(1, illegalArgument) + listOf(deliveryFailed(1, 1, illegalArgument), polled(1)) +
                            ok(1) + OffsetsCommitted(1),
                ),
                Case(
                    "continues the retry count when the exception type changes and reset is turned off",
                    failures = mapOf(1 to times(2) + ::IllegalArgumentException),
                    errorHandler = {
                        DefaultErrorHandler(FixedBackOff(10, 2)).apply { setResetStateOnExceptionChange(false) }
                    },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) +
                            failed(1) + listOf(deliveryFailed(1, 1), polled(1)) +
                            failed(1) + listOf(deliveryFailed(1, 2), polled(1)) +
                            failed(1, illegalArgument) +
                            listOf(deliveryFailed(1, 3, illegalArgument), RecordRecovered(kv(1)), OffsetsCommitted(1)),
                ),
                Case(
                    "calls a custom recoverer",
                    messages = 3,
                    failures = mapOf(2 to times(1)),
                    errorHandler = { tracking ->
                        DefaultErrorHandler(
                            { record, _ -> tracking.add(CustomRecovererInvoked(record.toKeyValue())) },
                            FixedBackOff(0, 0),
                        )
                    },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1, 2, 3)) + ok(1) + failed(2) +
                            listOf(
                                OffsetsCommitted(1),
                                CustomRecovererInvoked(kv(2)),
                                RecordRecovered(kv(2)),
                                OffsetsCommitted(2),
                                polled(3),
                            ) + ok(3) + OffsetsCommitted(3),
                ),
                Case(
                    "reprocesses and retries again when the recoverer fails (Spring default)",
                    failures = mapOf(1 to times(10)),
                    errorHandler = { tracking ->
                        DefaultErrorHandler(failingOnceRecoverer(tracking), FixedBackOff(10, 1))
                    },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) +
                            failed(1) + listOf(deliveryFailed(1, 1), polled(1)) +
                            failed(1) +
                            listOf(
                                deliveryFailed(1, 2),
                                CustomRecovererInvoked(kv(1)),
                                RecordRecoveryFailed(recoveryFailed, kv(1)),
                                polled(1),
                            ) +
                            failed(1) + listOf(deliveryFailed(1, 1), polled(1)) +
                            failed(1) +
                            listOf(
                                deliveryFailed(1, 2),
                                CustomRecovererInvoked(kv(1)),
                                RecordRecovered(kv(1)),
                                OffsetsCommitted(1),
                            ),
                ),
                Case(
                    "reprocesses without new retries when the recoverer fails and reset is turned off",
                    failures = mapOf(1 to times(10)),
                    errorHandler = { tracking ->
                        DefaultErrorHandler(failingOnceRecoverer(tracking), FixedBackOff(10, 1))
                            .apply { setResetStateOnRecoveryFailure(false) }
                    },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) +
                            failed(1) + listOf(deliveryFailed(1, 1), polled(1)) +
                            failed(1) +
                            listOf(
                                deliveryFailed(1, 2),
                                CustomRecovererInvoked(kv(1)),
                                RecordRecoveryFailed(recoveryFailed, kv(1)),
                                polled(1),
                            ) +
                            failed(1) +
                            listOf(
                                deliveryFailed(1, 3),
                                CustomRecovererInvoked(kv(1)),
                                RecordRecovered(kv(1)),
                                OffsetsCommitted(1),
                            ),
                ),
                Case(
                    "does not retry an exception that is classified as not retryable",
                    failures = mapOf(1 to times(5, ::IllegalArgumentException)),
                    errorHandler = {
                        DefaultErrorHandler(FixedBackOff(10, 1)).apply {
                            addNotRetryableExceptions(IllegalArgumentException::class.java)
                        }
                    },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) + failed(1, illegalArgument) +
                            listOf(deliveryFailed(1, 1, illegalArgument), RecordRecovered(kv(1)), OffsetsCommitted(1)),
                ),
                Case(
                    "retries an exception that is classified as retryable when retry is opt-in",
                    failures = mapOf(1 to times(1, ::IllegalArgumentException)),
                    errorHandler = {
                        DefaultErrorHandler(FixedBackOff(10, 1)).apply {
                            defaultFalse()
                            addRetryableExceptions(IllegalArgumentException::class.java)
                        }
                    },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) + failed(1, illegalArgument) +
                            listOf(deliveryFailed(1, 1, illegalArgument), polled(1)) + ok(1) + OffsetsCommitted(1),
                ),
                Case(
                    "does not retry other exceptions when retry is opt-in",
                    failures = mapOf(1 to times(5)),
                    errorHandler = {
                        DefaultErrorHandler(FixedBackOff(10, 1)).apply {
                            defaultFalse()
                            addRetryableExceptions(IllegalArgumentException::class.java)
                        }
                    },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) + failed(1) +
                            listOf(deliveryFailed(1, 1), RecordRecovered(kv(1)), OffsetsCommitted(1)),
                ),
                // A value the message converter cannot read fails with MessageConversionException, which Spring
                // does not retry, so a poison pill is skipped at once instead of blocking the partition.
                Case(
                    "skips a poison pill without retries with Spring's default error handler",
                    failures = mapOf(1 to times(10) { MessageConversionException("poison pill") }),
                    errorHandler = { DefaultErrorHandler() },
                    expected =
                        listOf(PartitionsAssigned(0), polled(1)) +
                            failed(1, ExceptionReport(MessageConversionException::class, "poison pill")) +
                            listOf(
                                deliveryFailed(1, 1, ExceptionReport(MessageConversionException::class, "poison pill")),
                                RecordRecovered(kv(1)),
                                OffsetsCommitted(1),
                            ),
                ),
            )
    }
}
