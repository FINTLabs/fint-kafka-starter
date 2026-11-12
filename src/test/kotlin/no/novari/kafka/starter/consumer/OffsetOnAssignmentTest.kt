package no.novari.kafka.starter.consumer

import no.novari.kafka.starter.consumertracking.ConsumerTracking
import no.novari.kafka.starter.consumertracking.ListenerInvokedWithRecord
import no.novari.kafka.starter.consumertracking.ListenerSuccessfullyProcessedRecord
import no.novari.kafka.starter.consumertracking.OffsetsCommitted
import no.novari.kafka.starter.consumertracking.PartitionsAssigned
import no.novari.kafka.starter.consumertracking.RecordListener
import no.novari.kafka.starter.consumertracking.RecordsPolled
import no.novari.kafka.starter.consumertracking.committed
import no.novari.kafka.starter.consumertracking.first
import no.novari.kafka.starter.consumertracking.kv
import no.novari.kafka.starter.consumertracking.sendAll
import no.novari.kafka.starter.consumertracking.trackedContainer
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.test.context.EmbeddedKafka
import java.util.UUID

@SpringBootTest
@EmbeddedKafka(partitions = 1)
class OffsetOnAssignmentTest(
    @Autowired private val factory: ConcurrentKafkaListenerContainerFactory<*, *>,
    @Autowired private val template: KafkaTemplate<String, String>,
) {
    private val topic = UUID.randomUUID().toString()

    @Test
    fun `continues from committed offset in the same group`() {
        template.sendAll(topic, 1..2)
        val first = consume(committed(2))
        assertThat(first.events).isEqualTo(
            listOf(
                PartitionsAssigned(0),
                RecordsPolled(listOf(kv(1), kv(2))),
                ListenerInvokedWithRecord(kv(1)),
                ListenerSuccessfullyProcessedRecord(kv(1)),
                ListenerInvokedWithRecord(kv(2)),
                ListenerSuccessfullyProcessedRecord(kv(2)),
                OffsetsCommitted(2),
            ),
        )

        template.sendAll(topic, 3..4)
        assertThat(consume(first<RecordsPolled>()).events).isEqualTo(
            listOf(PartitionsAssigned(2), RecordsPolled(listOf(kv(3), kv(4)))),
        )
    }

    @Test
    fun `seeks to beginning on every assignment with AbstractConsumerSeekAware`() {
        val seekToBeginning = { assignments: Map<TopicPartition, Long>, callback: ConsumerSeekCallback ->
            callback.seekToBeginning(assignments.keys)
        }
        template.sendAll(topic, 1..2)
        consume(committed(2), seekToBeginning)

        assertThat(consume(first<RecordsPolled>(), seekToBeginning).events).isEqualTo(
            listOf(PartitionsAssigned(2), RecordsPolled(listOf(kv(1), kv(2)))),
        )
    }

    @Test
    fun `seeks to a custom offset on assignment`() {
        template.sendAll(topic, 1..4)

        val tracking = consume(first<RecordsPolled>()) { _, callback -> callback.seek(topic, 0, 2) }

        assertThat(tracking.events).isEqualTo(
            listOf(PartitionsAssigned(0), RecordsPolled(listOf(kv(3), kv(4)))),
        )
    }

    private fun consume(
        stopWhen: (no.novari.kafka.starter.consumertracking.Event) -> Boolean,
        onAssigned: (Map<TopicPartition, Long>, ConsumerSeekCallback) -> Unit = { _, _ -> },
    ): ConsumerTracking {
        val tracking = ConsumerTracking(stopWhen)
        val container =
            factory.trackedContainer(topic, tracking, RecordListener(tracking, onAssigned), DefaultErrorHandler())
        container.start()
        try {
            assertThat(tracking.await()).isTrue()
        } finally {
            container.stop()
        }
        return tracking
    }
}
