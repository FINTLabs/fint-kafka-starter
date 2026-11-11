package no.novari.kafka.starter.topic

/** A topic name from [KafkaTopics]. Use [name] in SpEL: `@KafkaListener(topics = ["#{@myTopic.name}"])`. */
public sealed class TopicName(
    public val name: String,
) {
    override fun toString(): String = name
}

public class EntityTopicName internal constructor(
    name: String,
) : TopicName(name)

/** Name of an event or error-event topic. */
public class EventTopicName internal constructor(
    name: String,
) : TopicName(name)

/** A topic name pattern from [KafkaTopics]. Use [regex] in SpEL: `@KafkaListener(topicPattern = "#{@myPattern.regex}")`. */
public class TopicPattern internal constructor(
    public val regex: String,
) {
    override fun toString(): String = regex
}
