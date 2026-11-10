package no.novari.kafka.starter.topic

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration
import java.util.UUID

/**
 * Topics created and updated through Boot's [KafkaAdmin], as at application startup, with the fint default
 * `spring.kafka.admin.modify-topic-configs=true`.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1)
class TopicConfigTest(
    @Autowired private val kafkaAdmin: KafkaAdmin,
) {
    private val topic = UUID.randomUUID().toString()

    // fint-kafka 6 config for an event topic and an entity topic with cleanup frequency NORMAL.
    private val eventConfig =
        mapOf(
            TopicConfig.CLEANUP_POLICY_CONFIG to "delete",
            TopicConfig.RETENTION_MS_CONFIG to days(7),
            TopicConfig.SEGMENT_MS_CONFIG to hours(12),
        )
    private val entityConfig =
        mapOf(
            TopicConfig.CLEANUP_POLICY_CONFIG to "delete,compact",
            TopicConfig.RETENTION_MS_CONFIG to days(7),
            TopicConfig.SEGMENT_MS_CONFIG to hours(12),
            TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG to hours(24),
            TopicConfig.DELETE_RETENTION_MS_CONFIG to days(1),
        )

    @Test
    fun `creates a topic with the given config`() {
        createOrModify(eventConfig)

        assertThat(describe()).containsAllEntriesOf(eventConfig)
    }

    @Test
    fun `updates the config of an existing topic on a new startup`() {
        createOrModify(eventConfig)

        createOrModify(entityConfig)

        assertThat(describe()).containsAllEntriesOf(entityConfig)
    }

    @Test
    fun `leaves the topic unchanged on a new startup with the same config`() {
        createOrModify(entityConfig)
        val before = describeEntries()

        createOrModify(entityConfig)

        assertThat(describeEntries()).isEqualTo(before)
    }

    private fun createOrModify(config: Map<String, String>) =
        kafkaAdmin.createOrModifyTopics(
            TopicBuilder
                .name(topic)
                .partitions(1)
                .replicas(1)
                .configs(config)
                .build(),
        )

    private fun describe() = describeEntries().associate { it.name() to it.value() }

    private fun describeEntries(): List<ConfigEntry> =
        AdminClient.create(kafkaAdmin.configurationProperties).use { admin ->
            val resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
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
}
