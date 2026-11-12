package no.novari.kafka.starter.consumer

import no.novari.kafka.starter.consumertracking.ConsumerTracking
import no.novari.kafka.starter.consumertracking.OffsetsCommitted
import no.novari.kafka.starter.consumertracking.PartitionsAssigned
import no.novari.kafka.starter.consumertracking.RecordListener
import no.novari.kafka.starter.consumertracking.RecordsPolled
import no.novari.kafka.starter.consumertracking.committed
import no.novari.kafka.starter.consumertracking.first
import no.novari.kafka.starter.consumertracking.sendAll
import no.novari.kafka.starter.consumertracking.trackedContainer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.test.context.EmbeddedKafka
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * `auto.offset.reset` only applies to a group without a committed offset. A committed offset belongs to one group,
 * so a new group never continues where another group stopped.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1)
class AutoOffsetResetTest(
    @Autowired private val factory: ConcurrentKafkaListenerContainerFactory<*, *>,
    @Autowired private val template: KafkaTemplate<String, String>,
) {
    private val topic = UUID.randomUUID().toString()

    @Test
    fun `new group reads from the beginning with the fint default earliest`() {
        template.sendAll(topic, 1..2)

        val tracking = ConsumerTracking(first<RecordsPolled>())
        start(tracking, "new-group", latest = false).stopAfter(tracking)

        assertThat(tracking.events.first()).isEqualTo(PartitionsAssigned(0))
        assertThat(keys(tracking)).containsExactly("key1", "key2")
    }

    @Test
    fun `new group starts at the end with latest`() {
        template.sendAll(topic, 1..2)

        val tracking = ConsumerTracking(first<PartitionsAssigned>())
        start(tracking, "new-group").stopAfter(tracking)

        // With latest, Spring commits the position on assignment (assignmentCommitOption LATEST_ONLY_NO_TX).
        assertThat(tracking.events).containsExactly(OffsetsCommitted(2), PartitionsAssigned(2))
    }

    @Test
    fun `restart continues from the committed offset, including records produced while down`() {
        template.sendAll(topic, 1..2)
        val assigned = CountDownLatch(1)
        val firstRun = ConsumerTracking(committed(4))
        val container = start(firstRun, "group") { assigned.countDown() }
        assertThat(assigned.await(30, TimeUnit.SECONDS)).isTrue()
        template.sendAll(topic, 3..4)
        container.stopAfter(firstRun)
        assertThat(firstRun.events.take(2)).containsExactly(OffsetsCommitted(2), PartitionsAssigned(2))
        assertThat(keys(firstRun)).containsExactly("key3", "key4")

        template.sendAll(topic, 5..6)
        val restart = ConsumerTracking(committed(6))
        start(restart, "group").stopAfter(restart)

        assertThat(restart.events.first()).isEqualTo(PartitionsAssigned(4))
        assertThat(keys(restart)).containsExactly("key5", "key6")
    }

    @Test
    fun `new group ignores another group's committed offset and skips what was produced in between`() {
        template.sendAll(topic, 1..2)
        val oldApplication = ConsumerTracking(committed(2))
        start(oldApplication, "old-application", latest = false).stopAfter(oldApplication)

        template.sendAll(topic, 3..4)
        val newApplication = ConsumerTracking(first<PartitionsAssigned>())
        start(newApplication, "new-application").stopAfter(newApplication)

        assertThat(newApplication.events).containsExactly(OffsetsCommitted(4), PartitionsAssigned(4))
    }

    private fun start(
        tracking: ConsumerTracking,
        groupId: String,
        latest: Boolean = true,
        onAssigned: () -> Unit = {},
    ) = factory
        .trackedContainer(
            topic,
            tracking,
            RecordListener(tracking, { _, _ -> onAssigned() }),
            DefaultErrorHandler(),
            groupId = groupId,
            consumerProperties = if (latest) mapOf(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest") else emptyMap(),
        ).apply { start() }

    private fun ConcurrentMessageListenerContainer<String, String>.stopAfter(tracking: ConsumerTracking) {
        try {
            assertThat(tracking.await()).isTrue()
        } finally {
            stop()
        }
    }

    private fun keys(tracking: ConsumerTracking) =
        tracking.events
            .filterIsInstance<RecordsPolled>()
            .flatMap { it.records }
            .map { it.key }
}
