package no.novari.kafka.starter.producer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Producer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.test.context.EmbeddedKafka
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/** Boot's [KafkaTemplate] with the fint defaults: JSON values, the origin header and one shared producer. */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = [ProducerTest.TOPIC])
class ProducerTest(
    @Autowired private val template: KafkaTemplate<String, Dto>,
    @Autowired private val producerFactory: DefaultKafkaProducerFactory<*, *>,
    @Autowired private val listener: Listener,
) {
    data class Dto(
        val name: String,
    )

    class Listener {
        val values = LinkedBlockingQueue<Dto>()
        val records = LinkedBlockingQueue<ConsumerRecord<String, String>>()

        @KafkaListener(topics = [TOPIC], groupId = "producer-test-dto")
        fun onDto(value: Dto) = values.put(value)

        @KafkaListener(topics = [TOPIC], groupId = "producer-test-record")
        fun onRecord(record: ConsumerRecord<String, String>) = records.put(record)
    }

    @TestConfiguration
    class Config {
        @Bean
        fun listener() = Listener()
    }

    @Test
    fun `sends JSON with one origin header through one shared producer`() {
        val producersCreated = AtomicInteger()
        @Suppress("UNCHECKED_CAST")
        (producerFactory as DefaultKafkaProducerFactory<Any, Any>).addListener(
            object : ProducerFactory.Listener<Any, Any> {
                override fun producerAdded(
                    id: String,
                    producer: Producer<Any, Any>,
                ) {
                    producersCreated.incrementAndGet()
                }
            },
        )

        (1..4).map { i -> thread { template.send(TOPIC, "key$i", Dto("name$i")).get() } }.forEach { it.join() }

        assertThat(producersCreated.get()).isEqualTo(1)
        assertThat((1..4).map { listener.values.poll(30, TimeUnit.SECONDS) })
            .containsExactlyInAnyOrderElementsOf((1..4).map { Dto("name$it") })
        val record = listener.records.poll(30, TimeUnit.SECONDS)!!
        val originHeaders =
            record.headers().headers(
                OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_RECORD_HEADER,
            )
        assertThat(originHeaders.map { String(it.value()) }).containsExactly("test-application")
    }

    companion object {
        const val TOPIC = "producer-test"
    }
}
