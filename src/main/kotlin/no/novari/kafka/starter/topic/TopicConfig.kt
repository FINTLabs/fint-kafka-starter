package no.novari.kafka.starter.topic

import java.time.Duration

/** How often old records are cleaned up, through `segment.ms`. */
public enum class EventCleanupFrequency(
    internal val segment: Duration,
) {
    FREQUENT(Duration.ofHours(6)),
    NORMAL(Duration.ofHours(12)),
    RARE(Duration.ofHours(24)),
}

/** How often old records are cleaned up and compacted, through `segment.ms` and `max.compaction.lag.ms`. */
public enum class EntityCleanupFrequency(
    internal val segment: Duration,
    internal val maxCompactionLag: Duration,
) {
    FREQUENT(Duration.ofHours(3), Duration.ofHours(6)),
    NORMAL(Duration.ofHours(12), Duration.ofHours(24)),
    RARE(Duration.ofHours(24), Duration.ofHours(48)),
}

/** Config for an event or error-event topic, with cleanup policy `delete`. */
public class EventTopicConfig(
    /** `retention.ms` */
    public val retention: Duration,
    public val cleanupFrequency: EventCleanupFrequency = EventCleanupFrequency.NORMAL,
    public val partitions: Int = 1,
)

/** Config for an entity topic, with cleanup policy `compact`, or `delete,compact` when [lastValueRetention] is set. */
public class EntityTopicConfig(
    /** How long a tombstone is kept after compaction (`delete.retention.ms`). */
    public val nullValueRetention: Duration,
    /**
     * `retention.ms`. The default `null` gives cleanup policy `compact`, which keeps the last value per key **forever**.
     * Set it to also delete old records (`delete,compact`), as `lastValueRetentionTime(...)` did in fint-kafka 6.
     *
     * **Warning when migrating from fint-kafka 6:** a topic created with `lastValueRetentionTime(...)` changes from
     * `delete,compact` to `compact` at the next startup if this is left out, and old records are no longer deleted.
     */
    public val lastValueRetention: Duration? = null,
    public val cleanupFrequency: EntityCleanupFrequency = EntityCleanupFrequency.NORMAL,
    public val partitions: Int = 1,
)
