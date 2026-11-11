package no.novari.kafka.starter.consumer

import java.util.UUID

/**
 * Group ids built from `spring.kafka.consumer.group-id`, as in fint-kafka 6
 * (`ConsumerFactoryService.java`, commit `1ff1899`). Use it in SpEL, for example
 * `@KafkaListener(groupId = "#{@kafkaGroupIds.withSuffix('-cache')}")`. A `@KafkaListener` without `id` and `groupId`
 * uses the base group id.
 */
public class KafkaGroupIds internal constructor(
    internal val base: String?,
) {
    /** The base group id and [suffix], without a separator: `withSuffix("-cache")` gives `<base>-cache`. */
    public fun withSuffix(suffix: String): String = requiredBase() + suffix

    /** The base group id and a random UUID, without a separator. Gives a new consumer group on every start. */
    public fun unique(): String = withSuffix(UUID.randomUUID().toString())

    private fun requiredBase() = base ?: throw IllegalStateException(MISSING_BASE)
}

internal const val MISSING_BASE =
    "spring.kafka.consumer.group-id is not set. Set it to the group id the application used with fint-kafka, " +
        "so the consumer groups do not change."
