package no.novari.kafka.starter.topic

import no.novari.kafka.starter.consumertracking.ConsumerTracking
import no.novari.kafka.starter.consumertracking.Event
import no.novari.kafka.starter.consumertracking.KeyValue
import no.novari.kafka.starter.consumertracking.PartitionsAssigned
import no.novari.kafka.starter.consumertracking.RecordListener
import no.novari.kafka.starter.consumertracking.RecordsPolled
import no.novari.kafka.starter.consumertracking.first
import no.novari.kafka.starter.consumertracking.trackedContainer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.TopicConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import java.util.UUID

/**
 * Kafka rolls a segment when a new record's timestamp is more than `segment.ms` after the first record in the
 * segment, and only closed segments are cleaned. The tests therefore send a last record to roll the segment. It is
 * still in the active segment, so it is part of what the consumer reads.
 */
@Tag("slow")
@SpringBootTest(properties = ["logging.level.kafka.log.LogCleaner=DEBUG"])
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = [
        "log.cleaner.backoff.ms=1000",
        "log.retention.check.interval.ms=1000",
        "log.roll.ms=1000",
        "log.retention.ms=1000",
        "log.initial.task.delay.ms=0",
    ],
)
// The broker properties above give this class its own broker. Do not keep it in the context cache.
@DirtiesContext
class TopicCleanupTest(
    @Autowired private val kafkaAdmin: KafkaAdmin,
    @Autowired private val factory: ConcurrentKafkaListenerContainerFactory<*, *>,
    @Autowired private val template: KafkaTemplate<String, String>,
) {
    private val topic = UUID.randomUUID().toString()

    @Test
    fun `delete removes closed segments older than retention`() {
        create(TopicConfig.CLEANUP_POLICY_CONFIG to "delete", TopicConfig.RETENTION_MS_CONFIG to "1000")
        send("key1" to "value1", "key2" to "value2", "key3" to "value3", "key4" to "value4")
        Thread.sleep(2000)
        send("key5" to "value5", "key6" to "value6")

        assertThat(consume()).containsExactly(
            PartitionsAssigned(4),
            polled("key5" to "value5", "key6" to "value6"),
        )
    }

    @Test
    fun `compact keeps the last value per key in the original order`() {
        createCompacted(deleteRetentionMs = DAY_MS)
        send(
            "key1" to "value1-1",
            "key2" to "value2-1",
            "key3" to "value3",
            "key4" to "value4-1",
            "key4" to "value4-2",
            "key2" to "value2-2",
            "key5" to "value5",
            "key1" to "value1-2",
        )
        Thread.sleep(2000)
        send("key5" to "value5")
        Thread.sleep(2000)

        // Unlike delete, compaction keeps the log start offset at 0.
        assertThat(consume()).containsExactly(
            PartitionsAssigned(0),
            polled(
                "key3" to "value3",
                "key4" to "value4-2",
                "key2" to "value2-2",
                "key5" to "value5",
                "key1" to "value1-2",
            ),
        )
    }

    @Test
    fun `compact and delete removes old segments and compacts the rest`() {
        create(
            TopicConfig.CLEANUP_POLICY_CONFIG to "delete,compact",
            TopicConfig.SEGMENT_MS_CONFIG to "1000",
            TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG to "1000",
            TopicConfig.DELETE_RETENTION_MS_CONFIG to DAY_MS,
            TopicConfig.RETENTION_MS_CONFIG to "5500",
        )
        send("key1" to "value1", "key2" to "value2", "key3" to "value3-1")
        Thread.sleep(4000)
        send("key5" to "value5", "key3" to "value3-2", "key4" to "value4-1", "key4" to "value4-2")
        Thread.sleep(3000)
        send("key4" to "value4-3")
        Thread.sleep(1000)

        assertThat(consume()).containsExactly(
            PartitionsAssigned(3),
            polled("key5" to "value5", "key3" to "value3-2", "key4" to "value4-2"),
        )
    }

    @Test
    fun `compact keeps a tombstone until delete retention has passed`() {
        sendTombstoneAndCompactTwice(deleteRetentionMs = DAY_MS)

        assertThat(consumeUntilKey4()).isEqualTo(
            polled(
                "key2" to "value2",
                "key1" to null,
                "key3" to "value3",
                "key4" to "value4",
            ).records,
        )
    }

    @Test
    fun `compact removes a tombstone after delete retention`() {
        sendTombstoneAndCompactTwice(deleteRetentionMs = "1000")

        assertThat(
            consumeUntilKey4(),
        ).isEqualTo(polled("key2" to "value2", "key3" to "value3", "key4" to "value4").records)
    }

    /** The first compaction removes the value the tombstone replaces. The tombstone itself goes in a later one. */
    private fun sendTombstoneAndCompactTwice(deleteRetentionMs: String) {
        createCompacted(deleteRetentionMs)
        send("key1" to "value1", "key2" to "value2", "key1" to null)
        Thread.sleep(2000)
        send("key3" to "value3")
        Thread.sleep(2000)
        send("key4" to "value4")
        Thread.sleep(2000)
    }

    private fun createCompacted(deleteRetentionMs: String) =
        create(
            TopicConfig.CLEANUP_POLICY_CONFIG to "compact",
            TopicConfig.SEGMENT_MS_CONFIG to "1000",
            TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG to "1000",
            TopicConfig.DELETE_RETENTION_MS_CONFIG to deleteRetentionMs,
        )

    private fun create(vararg config: Pair<String, String>) =
        kafkaAdmin.createOrModifyTopics(
            TopicBuilder
                .name(topic)
                .partitions(1)
                .replicas(1)
                .configs(mapOf(TopicConfig.SEGMENT_MS_CONFIG to "1000") + config)
                .build(),
        )

    private fun send(vararg records: Pair<String, String?>) =
        records.forEach { (key, value) -> template.send(ProducerRecord(topic, key, value)).get() }

    private fun polled(vararg records: Pair<String, String?>) =
        RecordsPolled(records.map { KeyValue(it.first, it.second) })

    /** The last record is in the active segment, which the consumer may get in a separate poll. */
    private fun consumeUntilKey4(): List<KeyValue> {
        val events = consume { it is RecordsPolled && it.records.any { record -> record.key == "key4" } }
        assertThat(events.first()).isEqualTo(PartitionsAssigned(0))
        return events.filterIsInstance<RecordsPolled>().flatMap { it.records }
    }

    private fun consume(stopWhen: (Event) -> Boolean = first<RecordsPolled>()): List<Event> {
        val tracking = ConsumerTracking(stopWhen)
        val container = factory.trackedContainer(topic, tracking, RecordListener(tracking), DefaultErrorHandler())
        container.start()
        try {
            assertThat(tracking.await()).isTrue()
        } finally {
            container.stop()
        }
        return tracking.events
    }

    companion object {
        private const val DAY_MS = "86400000"
    }
}
