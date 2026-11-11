package no.novari.kafka.starter.producer

import no.novari.kafka.starter.topic.EventTopicName
import no.novari.kafka.starter.topic.KafkaTopics
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Producer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.messaging.handler.annotation.Header
import org.springframework.util.unit.DataSize
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/** Boot's [KafkaTemplate] with the fint defaults: JSON values, the origin header and one shared producer. */
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = [ProducerTest.TOPIC],
    // The broker default is 1 MiB. Raising it is the broker's or topic's job, not the starter's.
    brokerProperties = ["message.max.bytes=3145728"],
)
class ProducerTest(
    @Autowired private val template: KafkaTemplate<String, Dto>,
    @Autowired private val producersCreated: AtomicInteger,
    @Autowired private val producerTestTopic: EventTopicName,
    @Autowired private val listener: Listener,
) {
    data class Dto(
        val name: String,
    )

    class Listener {
        val values = LinkedBlockingQueue<Pair<String, Dto>>()
        val records = LinkedBlockingQueue<ConsumerRecord<String, String>>()

        @KafkaListener(topics = ["#{@producerTestTopic.name}"], groupId = "producer-test-dto")
        fun onDto(
            value: Dto,
            @Header(KafkaHeaders.RECEIVED_KEY) key: String,
        ) = values.put(key to value)

        @KafkaListener(topics = ["#{@producerTestTopic.name}"], groupId = "producer-test-record")
        fun onRecord(record: ConsumerRecord<String, String>) = records.put(record)
    }

    @TestConfiguration
    class Config {
        @Bean
        fun producerTestTopic(topics: KafkaTopics) = topics.event("producer-test")

        @Bean
        fun listener() = Listener()

        /** Counts every producer the context creates, also those created by other tests in the class. */
        @Bean
        fun producersCreated() = AtomicInteger()

        @Bean
        fun producerCounter(producersCreated: AtomicInteger) =
            DefaultKafkaProducerFactoryCustomizer { factory ->
                @Suppress("UNCHECKED_CAST")
                (factory as DefaultKafkaProducerFactory<Any, Any>).addListener(
                    object : ProducerFactory.Listener<Any, Any> {
                        override fun producerAdded(
                            id: String,
                            producer: Producer<Any, Any>,
                        ) {
                            producersCreated.incrementAndGet()
                        }
                    },
                )
            }
    }

    @Test
    fun `sends JSON with one origin header through one shared producer`() {
        (1..4)
            .map { i -> thread { template.send(producerTestTopic, "shared$i", Dto("name$i")).get() } }
            .forEach { it.join() }

        assertThat(producersCreated.get()).isEqualTo(1)
        assertThat((1..4).map { listener.values.await("shared") })
            .containsExactlyInAnyOrderElementsOf((1..4).map { "shared$it" to Dto("name$it") })
        val record = listener.records.await { it.key().startsWith("shared") }
        val originHeaders =
            record.headers().headers(
                OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_RECORD_HEADER,
            )
        assertThat(originHeaders.map { String(it.value()) }).containsExactly("test-application")
    }

    @Test
    fun `sends a 2 MiB message`() {
        val name = "x".repeat(DataSize.ofMegabytes(2).toBytes().toInt())

        template.send(producerTestTopic, "large", Dto(name)).get()

        assertThat(listener.values.await("large")).isEqualTo("large" to Dto(name))
    }

    /** Skips records from other tests in the class. */
    private fun LinkedBlockingQueue<Pair<String, Dto>>.await(keyPrefix: String) =
        await { it.first.startsWith(keyPrefix) }

    private fun <T : Any> LinkedBlockingQueue<T>.await(predicate: (T) -> Boolean): T {
        while (true) {
            val element = poll(30, TimeUnit.SECONDS) ?: error("Timed out")
            if (predicate(element)) return element
        }
    }

    companion object {
        const val TOPIC = "test-org-id.test-domain-context.event.producer-test"
    }
}
