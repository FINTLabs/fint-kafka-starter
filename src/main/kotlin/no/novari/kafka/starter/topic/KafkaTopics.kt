package no.novari.kafka.starter.topic

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT
import org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG
import org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE
import org.apache.kafka.common.config.TopicConfig.DELETE_RETENTION_MS_CONFIG
import org.apache.kafka.common.config.TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG
import org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG
import org.apache.kafka.common.config.TopicConfig.SEGMENT_MS_CONFIG
import org.springframework.kafka.config.TopicBuilder
import java.time.Duration

/**
 * Topic names on the form `<org-id>.<domain-context>.<entity|event>[.error].<name>`, patterns for them, and
 * [NewTopic]s with the Novari topic config. `orgId` and `domainContext` default to `novari.kafka.topic.org-id` and
 * `novari.kafka.topic.domain-context`.
 *
 * Name and pattern logic ported from `TopicNameMappingService.kt`, `TopicNamePatternRegexUtils.kt` and
 * `TopicNameParameterCharacterValidationService.kt` in fint-kafka 6.3.1 (commit `1ff1899`).
 */
public class KafkaTopics internal constructor(
    private val orgId: String?,
    private val domainContext: String?,
    private val replicas: Int,
) {
    init {
        orgId?.let { validatePart(ORG_ID_PROPERTY, it) }
        domainContext?.let { validatePart(DOMAIN_CONTEXT_PROPERTY, it) }
    }

    public fun entity(
        name: String,
        orgId: String? = null,
        domainContext: String? = null,
    ): EntityTopicName = EntityTopicName(topicName(orgId, domainContext, listOf(ENTITY), name))

    public fun event(
        name: String,
        orgId: String? = null,
        domainContext: String? = null,
    ): EventTopicName = EventTopicName(topicName(orgId, domainContext, listOf(EVENT), name))

    public fun errorEvent(
        name: String,
        orgId: String? = null,
        domainContext: String? = null,
    ): EventTopicName = EventTopicName(topicName(orgId, domainContext, listOf(EVENT, ERROR), name))

    public fun entityPattern(
        name: NamePattern = NamePattern.any(),
        orgId: NamePattern? = null,
        domainContext: NamePattern? = null,
    ): TopicPattern = topicPattern(orgId, domainContext, listOf(ENTITY), name)

    public fun eventPattern(
        name: NamePattern = NamePattern.any(),
        orgId: NamePattern? = null,
        domainContext: NamePattern? = null,
    ): TopicPattern = topicPattern(orgId, domainContext, listOf(EVENT), name)

    public fun errorEventPattern(
        name: NamePattern = NamePattern.any(),
        orgId: NamePattern? = null,
        domainContext: NamePattern? = null,
    ): TopicPattern = topicPattern(orgId, domainContext, listOf(EVENT, ERROR), name)

    /** Return it from a `@Bean` method, or pass it to `KafkaAdmin.createOrModifyTopics`. */
    public fun newTopic(
        topic: EventTopicName,
        config: EventTopicConfig,
    ): NewTopic =
        newTopic(
            topic,
            config.partitions,
            mapOf(
                CLEANUP_POLICY_CONFIG to CLEANUP_POLICY_DELETE,
                RETENTION_MS_CONFIG to config.retention.ms(),
                SEGMENT_MS_CONFIG to config.cleanupFrequency.segment.ms(),
            ),
        )

    /** Return it from a `@Bean` method, or pass it to `KafkaAdmin.createOrModifyTopics`. */
    public fun newTopic(
        topic: EntityTopicName,
        config: EntityTopicConfig,
    ): NewTopic =
        newTopic(
            topic,
            config.partitions,
            buildMap {
                if (config.lastValueRetention == null) {
                    put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT)
                } else {
                    put(CLEANUP_POLICY_CONFIG, "$CLEANUP_POLICY_DELETE,$CLEANUP_POLICY_COMPACT")
                    put(RETENTION_MS_CONFIG, config.lastValueRetention.ms())
                }
                put(MAX_COMPACTION_LAG_MS_CONFIG, config.cleanupFrequency.maxCompactionLag.ms())
                put(DELETE_RETENTION_MS_CONFIG, config.nullValueRetention.ms())
                put(SEGMENT_MS_CONFIG, config.cleanupFrequency.segment.ms())
            },
        )

    private fun newTopic(
        topic: TopicName,
        partitions: Int,
        configs: Map<String, String>,
    ) = TopicBuilder
        .name(topic.name)
        .partitions(partitions)
        .replicas(replicas)
        .configs(configs)
        .build()

    private fun topicName(
        orgId: String?,
        domainContext: String?,
        messageType: List<String>,
        name: String,
    ): String {
        val org = orgId ?: defaultOrgId
        val domain = domainContext ?: defaultDomainContext
        validatePart("orgId", org)
        validatePart("domainContext", domain)
        validatePart("name", name)
        return (listOf(org, domain) + messageType + name).joinToString(".")
    }

    private fun topicPattern(
        orgId: NamePattern?,
        domainContext: NamePattern?,
        messageType: List<String>,
        name: NamePattern,
    ): TopicPattern {
        val org = orgId ?: NamePattern.anyOf(defaultOrgId)
        val domain = domainContext ?: NamePattern.anyOf(defaultDomainContext)
        val parts = listOf(org, domain) + messageType.map { NamePattern.anyOf(it) } + name
        return TopicPattern(parts.joinToString("\\.", "^", "$") { it.regex })
    }

    private val defaultOrgId get() = orgId ?: throw missingDefault("orgId", ORG_ID_PROPERTY)

    private val defaultDomainContext
        get() = domainContext ?: throw missingDefault("domainContext", DOMAIN_CONTEXT_PROPERTY)

    private fun missingDefault(
        part: String,
        property: String,
    ) = IllegalStateException("Topic name part '$part' is not set. Set $property, or pass $part.")

    private fun Duration.ms() = toMillis().toString()
}

internal fun validatePart(
    part: String,
    value: String,
) {
    require(value.isNotBlank()) { "Topic name part '$part' is blank." }
    require('.' !in value) { "Topic name part '$part' ('$value') contains '.'. $ILLEGAL_CHARACTERS" }
    require(value.none { it.isUpperCase() }) {
        "Topic name part '$part' ('$value') contains uppercase letters. $ILLEGAL_CHARACTERS"
    }
}

private const val ENTITY = "entity"
private const val EVENT = "event"
private const val ERROR = "error"
private const val ORG_ID_PROPERTY = "novari.kafka.topic.org-id"
private const val DOMAIN_CONTEXT_PROPERTY = "novari.kafka.topic.domain-context"
private const val ILLEGAL_CHARACTERS =
    "Topic name parts cannot contain '.' or uppercase letters, so use 'afk-no', not 'afk.no'."
