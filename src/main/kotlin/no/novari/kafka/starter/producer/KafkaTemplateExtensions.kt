package no.novari.kafka.starter.producer

import no.novari.kafka.starter.topic.TopicName
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.util.concurrent.CompletableFuture

/** Sends [value] with [key] to [topic]. A `null` value is a tombstone. */
public fun <V : Any> KafkaTemplate<String, V>.send(
    topic: TopicName,
    key: String,
    value: V?,
): CompletableFuture<SendResult<String, V>> = send(topic.name, key, value)
