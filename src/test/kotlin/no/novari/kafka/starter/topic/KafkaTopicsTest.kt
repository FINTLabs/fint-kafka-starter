package no.novari.kafka.starter.topic

import org.apache.kafka.common.config.TopicConfig
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import java.time.Duration

class KafkaTopicsTest {
    private val topics = KafkaTopics("test-org-id", "test-domain-context", replicas = 2)

    class GoldenRow(
        val columns: List<String>,
    ) {
        override fun toString() = columns.joinToString(" ")
    }

    @ParameterizedTest
    @MethodSource("goldenNames")
    fun `topic name equals fint-kafka 6`(row: GoldenRow) {
        val (kind, orgId, domainContext, name, expected) = row.columns
        val topic =
            when (kind) {
                "entity" -> topics.entity(name, orgId.orDefault(), domainContext.orDefault())
                "event" -> topics.event(name, orgId.orDefault(), domainContext.orDefault())
                else -> topics.errorEvent(name, orgId.orDefault(), domainContext.orDefault())
            }
        assertThat(topic.name).isEqualTo(expected)
    }

    @ParameterizedTest
    @MethodSource("goldenPatterns")
    fun `topic pattern equals fint-kafka 6`(row: GoldenRow) {
        val (kind, orgId, domainContext, name, expected) = row.columns
        val pattern =
            when (kind) {
                "entity" -> topics.entityPattern(name.toPattern()!!, orgId.toPattern(), domainContext.toPattern())
                "event" -> topics.eventPattern(name.toPattern()!!, orgId.toPattern(), domainContext.toPattern())
                else -> topics.errorEventPattern(name.toPattern()!!, orgId.toPattern(), domainContext.toPattern())
            }
        assertThat(pattern.regex).isEqualTo(expected)
    }

    @Test
    fun `regex-quotes pattern values`() {
        assertThat(topics.eventPattern(NamePattern.anyOf("a+b", "c")).regex)
            .isEqualTo("""^(test-org-id)\.(test-domain-context)\.(event)\.(a\+b|c)$""")
    }

    // The golden rows for anyOf on org-id also use startingWith, so they are filtered out.
    @Test
    fun `anyOf on org-id`() {
        assertThat(topics.entityPattern(orgId = NamePattern.anyOf("afk-no", "bfk-no")).regex)
            .isEqualTo("""^(afk-no|bfk-no)\.(test-domain-context)\.(entity)\.[^.]+$""")
    }

    @Test
    fun `the name is SpEL-friendly and the pattern matches the name`() {
        val topic = topics.errorEvent("instance-receival-error")

        assertThat(Regex(topics.errorEventPattern().regex).matches(topic.name)).isTrue()
        assertThat(Regex(topics.eventPattern().regex).matches(topic.name)).isFalse()
    }

    @Test
    fun `rejects a dot, uppercase letters and blank parts`() {
        assertThatThrownBy { topics.event("instance.received") }
            .hasMessage(
                "Topic name part 'name' ('instance.received') contains '.'. " +
                    "Topic name parts cannot contain '.' or uppercase letters, so use 'afk-no', not 'afk.no'.",
            )
        assertThatThrownBy { topics.event("x", orgId = "AFK-no") }.hasMessageStartingWith(
            "Topic name part 'orgId' ('AFK-no') contains uppercase letters.",
        )
        assertThatThrownBy { topics.event(" ") }.hasMessage("Topic name part 'name' is blank.")
        assertThatThrownBy { NamePattern.anyOf("a.b") }.hasMessageStartingWith(
            "Topic name part 'pattern value' ('a.b') contains '.'.",
        )
        assertThatThrownBy { NamePattern.anyOf() }.hasMessage("A topic name pattern needs at least one value.")
    }

    @Test
    fun `fails with the property name when a default is missing or invalid`() {
        val withoutDefaults = KafkaTopics(null, null, replicas = 2)

        assertThat(withoutDefaults.event("x", "afk-no", "flyt").name).isEqualTo("afk-no.flyt.event.x")
        assertThatThrownBy { withoutDefaults.event("x", orgId = "afk-no") }
            .hasMessage(
                "Topic name part 'domainContext' is not set. Set novari.kafka.topic.domain-context, or pass domainContext.",
            )
        assertThatThrownBy { withoutDefaults.entityPattern() }
            .hasMessage("Topic name part 'orgId' is not set. Set novari.kafka.topic.org-id, or pass orgId.")
        assertThatThrownBy { KafkaTopics("afk.no", null, replicas = 2) }
            .hasMessageStartingWith("Topic name part 'novari.kafka.topic.org-id' ('afk.no') contains '.'.")
    }

    @ParameterizedTest
    @EnumSource
    fun `event topic config`(frequency: EventCleanupFrequency) {
        val topic =
            topics.newTopic(
                topics.event("x"),
                EventTopicConfig(Duration.ofDays(7), frequency, partitions = 3),
            )

        assertThat(topic.name()).isEqualTo("test-org-id.test-domain-context.event.x")
        assertThat(topic.numPartitions()).isEqualTo(3)
        assertThat(topic.replicationFactor()).isEqualTo(2.toShort())
        assertThat(topic.configs()).isEqualTo(
            mapOf(
                TopicConfig.CLEANUP_POLICY_CONFIG to "delete",
                TopicConfig.RETENTION_MS_CONFIG to days(7),
                TopicConfig.SEGMENT_MS_CONFIG to EVENT_SEGMENT_HOURS.getValue(frequency).hours(),
            ),
        )
    }

    @ParameterizedTest
    @EnumSource
    fun `entity topic config with last value retention`(frequency: EntityCleanupFrequency) {
        val topic =
            topics.newTopic(
                topics.entity("x"),
                EntityTopicConfig(Duration.ofDays(1), lastValueRetention = Duration.ofDays(7), frequency),
            )

        val (segment, maxCompactionLag) = ENTITY_HOURS.getValue(frequency)
        assertThat(topic.numPartitions()).isEqualTo(1)
        assertThat(topic.configs()).isEqualTo(
            mapOf(
                TopicConfig.CLEANUP_POLICY_CONFIG to "delete,compact",
                TopicConfig.RETENTION_MS_CONFIG to days(7),
                TopicConfig.SEGMENT_MS_CONFIG to segment.hours(),
                TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG to maxCompactionLag.hours(),
                TopicConfig.DELETE_RETENTION_MS_CONFIG to days(1),
            ),
        )
    }

    @Test
    fun `entity topic config keeps the last value forever by default`() {
        val topic = topics.newTopic(topics.entity("x"), EntityTopicConfig(Duration.ofDays(1)))

        assertThat(topic.configs()).isEqualTo(
            mapOf(
                TopicConfig.CLEANUP_POLICY_CONFIG to "compact",
                TopicConfig.SEGMENT_MS_CONFIG to 12L.hours(),
                TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG to 24L.hours(),
                TopicConfig.DELETE_RETENTION_MS_CONFIG to days(1),
            ),
        )
    }

    companion object {
        // fint-kafka 6: EventCleanupFrequency.java and EntityCleanupFrequency.java
        private val EVENT_SEGMENT_HOURS =
            mapOf(
                EventCleanupFrequency.FREQUENT to 6L,
                EventCleanupFrequency.NORMAL to 12L,
                EventCleanupFrequency.RARE to 24L,
            )
        private val ENTITY_HOURS =
            mapOf(
                EntityCleanupFrequency.FREQUENT to (3L to 6L),
                EntityCleanupFrequency.NORMAL to (12L to 24L),
                EntityCleanupFrequency.RARE to (24L to 48L),
            )

        @JvmStatic
        fun goldenNames() = golden("topic-names.tsv")

        // startingWith, endingWith and custom(regex) are not in fint-kafka-starter, as no app uses them.
        @JvmStatic
        fun goldenPatterns() =
            golden("topic-name-patterns.tsv").filter { row ->
                row.columns.subList(1, 4).all { it.substringBefore("(") in SUPPORTED_PATTERNS }
            }

        private val SUPPORTED_PATTERNS = setOf("<default>", "any", "exactly", "anyOf")

        private fun golden(file: String) =
            KafkaTopicsTest::class.java
                .getResource("/golden/$file")!!
                .readText()
                .lines()
                .filter { it.isNotBlank() && !it.startsWith("#") }
                .drop(1)
                .map { GoldenRow(it.split("\t")) }

        private fun String.orDefault() = takeUnless { it == "<default>" }

        /** Parses `<default>`, `any`, `exactly(a)` and `anyOf(a,b)`. */
        private fun String.toPattern(): NamePattern? {
            if (this == "<default>") return null
            if (this == "any") return NamePattern.any()
            val function = substringBefore("(")
            val values = substringAfter("(").removeSuffix(")").split(",").toTypedArray()
            return when (function) {
                "exactly", "anyOf" -> NamePattern.anyOf(*values)
                else -> error("Unknown pattern $this")
            }
        }

        private fun days(n: Long) = Duration.ofDays(n).toMillis().toString()

        private fun Long.hours() = Duration.ofHours(this).toMillis().toString()
    }
}
