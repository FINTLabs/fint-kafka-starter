package no.novari.kafka.starter.topic

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration
import java.util.UUID

/**
 * Topics from the [KafkaTopics] factories, created and updated through Boot's [KafkaAdmin] as at application startup,
 * with the fint default `spring.kafka.admin.modify-topic-configs=true`.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1)
class TopicConfigTest(
    @Autowired private val kafkaAdmin: KafkaAdmin,
    @Autowired private val topics: KafkaTopics,
) {
    @TestConfiguration
    class BeanTopic {
        @Bean
        fun beanTopic(topics: KafkaTopics): NewTopic =
            topics.newTopic(topics.event(BEAN_TOPIC), EventTopicConfig(Duration.ofDays(7)))
    }

    private val name = UUID.randomUUID().toString()
    private val eventConfig = EventTopicConfig(Duration.ofDays(7))
    private val entityConfig = EntityTopicConfig(Duration.ofDays(1), lastValueRetention = Duration.ofDays(7))

    @Test
    fun `creates a NewTopic bean at startup`() {
        assertThat(describe(topics.event(BEAN_TOPIC))).containsAllEntriesOf(
            mapOf(
                TopicConfig.CLEANUP_POLICY_CONFIG to "delete",
                TopicConfig.RETENTION_MS_CONFIG to days(7),
                TopicConfig.SEGMENT_MS_CONFIG to hours(12),
            ),
        )
    }

    @Test
    fun `creates an event topic with the given config`() {
        val topic = topics.newTopic(topics.event(name), eventConfig)

        kafkaAdmin.createOrModifyTopics(topic)

        assertThat(describe(topics.event(name))).containsAllEntriesOf(topic.configs())
    }

    @Test
    fun `updates the config of an existing topic on a new startup`() {
        kafkaAdmin.createOrModifyTopics(topics.newTopic(topics.entity(name), EntityTopicConfig(Duration.ofDays(1))))
        val topic = topics.newTopic(topics.entity(name), entityConfig)

        kafkaAdmin.createOrModifyTopics(topic)

        assertThat(describe(topics.entity(name)))
            .containsEntry(TopicConfig.CLEANUP_POLICY_CONFIG, "delete,compact")
            .containsAllEntriesOf(topic.configs())
    }

    /** Documents a migration risk: `retention.ms` stays on the topic, but `compact` alone does not use it. */
    @Test
    fun `keeps retention ms on the topic when the policy changes from delete,compact to compact`() {
        val threeDays = Duration.ofDays(3) // Not the broker default (7 days), so the value must come from the topic.
        kafkaAdmin.createOrModifyTopics(
            topics.newTopic(topics.entity(name), EntityTopicConfig(Duration.ofDays(1), lastValueRetention = threeDays)),
        )

        kafkaAdmin.createOrModifyTopics(topics.newTopic(topics.entity(name), EntityTopicConfig(Duration.ofDays(1))))

        val entries = describeEntries(topics.entity(name)).associateBy { it.name() }
        assertThat(entries.getValue(TopicConfig.CLEANUP_POLICY_CONFIG).value()).isEqualTo("compact")
        val retention = entries.getValue(TopicConfig.RETENTION_MS_CONFIG)
        assertThat(retention.value()).isEqualTo(threeDays.toMillis().toString())
        assertThat(retention.source()).isEqualTo(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
    }

    @Test
    fun `leaves the topic unchanged on a new startup with the same config`() {
        val topic = topics.newTopic(topics.entity(name), entityConfig)
        kafkaAdmin.createOrModifyTopics(topic)
        val before = describeEntries(topics.entity(name))

        kafkaAdmin.createOrModifyTopics(topic)

        assertThat(describeEntries(topics.entity(name))).isEqualTo(before)
    }

    private fun describe(topic: TopicName) = describeEntries(topic).associate { it.name() to it.value() }

    private fun describeEntries(topic: TopicName): List<ConfigEntry> =
        AdminClient.create(kafkaAdmin.configurationProperties).use { admin ->
            val resource = ConfigResource(ConfigResource.Type.TOPIC, topic.name)
            admin
                .describeConfigs(listOf(resource))
                .all()
                .get()
                .getValue(resource)
                .entries()
                .sortedBy { it.name() }
        }

    private fun days(n: Long) = Duration.ofDays(n).toMillis().toString()

    private fun hours(n: Long) = Duration.ofHours(n).toMillis().toString()

    companion object {
        private const val BEAN_TOPIC = "created-by-bean"
    }
}
