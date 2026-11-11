package no.novari.kafka.starter.health

import no.novari.kafka.starter.producer.send
import no.novari.kafka.starter.topic.EventTopicName
import no.novari.kafka.starter.topic.KafkaTopics
import org.apache.kafka.common.errors.RecordTooLargeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.event.ConsumerStoppedEvent
import org.springframework.kafka.event.ConsumerStoppedEvent.Reason
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.util.unit.DataSize
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/** The Kafka indicators through `/actuator/health/readiness` and `/actuator/health/liveness`. */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "management.endpoint.health.show-components=always",
        "management.endpoint.health.show-details=always",
        "novari.kafka.health.producer.failure-threshold=1",
        "novari.kafka.health.producer.failure-window=500ms",
    ],
)
@EmbeddedKafka(partitions = 1)
@ExtendWith(OutputCaptureExtension::class)
class HealthEndpointTest(
    @LocalServerPort private val port: Int,
    @Autowired private val events: ApplicationEventPublisher,
    @Autowired private val registry: KafkaListenerEndpointRegistry,
    @Autowired private val template: KafkaTemplate<String, String>,
    @Autowired private val healthTestTopic: EventTopicName,
) {
    @TestConfiguration
    class Config {
        @Bean
        fun healthTestTopic(topics: KafkaTopics) = topics.event("health-test")

        @Bean
        fun healthTestListener() = Listener()
    }

    class Listener {
        @KafkaListener(id = LISTENER_ID, idIsGroup = false, topics = ["#{@healthTestTopic.name}"])
        fun on(value: String) = Unit
    }

    @Test
    fun `readiness and liveness are up with the Kafka indicators`() {
        val readiness = health("readiness")
        val liveness = health("liveness")

        assertThat(readiness.status).isEqualTo(200)
        assertThat(readiness.components()).containsExactlyInAnyOrder("readinessState", "kafkaConnectivity")
        assertThat(readiness.status("kafkaConnectivity")).isEqualTo("UP")
        assertThat(liveness.status).isEqualTo(200)
        assertThat(liveness.components()).containsExactlyInAnyOrder("livenessState", "kafkaConsumers")
        assertThat(liveness.status("kafkaConsumers")).isEqualTo("UP")
    }

    @Test
    fun `liveness is down while a consumer is stopped with a fatal reason`() {
        val consumer = Any()

        events.publishEvent(ConsumerStoppedEvent(consumer, Any(), Reason.AUTH))
        try {
            assertThat(health("liveness").status).isEqualTo(503)
            assertThat(health("liveness").status("kafkaConsumers")).isEqualTo("DOWN")
            assertThat(health("readiness").status).isEqualTo(200)
        } finally {
            events.publishEvent(ConsumerStoppedEvent(consumer, Any(), Reason.NORMAL))
        }

        assertThat(health("liveness").status).isEqualTo(200)
    }

    @Test
    fun `liveness is down while a KafkaListener container is not running`() {
        val container = registry.getListenerContainer(LISTENER_ID)!!

        container.stop()
        try {
            val liveness = health("liveness")
            assertThat(liveness.status).isEqualTo(503)
            val stoppedContainers = liveness.body["components"]["kafkaConsumers"]["details"]["stoppedContainers"]
            assertThat(stoppedContainers.toList().map { it.asString() }).containsExactly(LISTENER_ID)
        } finally {
            container.start()
        }

        assertThat(health("liveness").status).isEqualTo(200)
    }

    @Test
    fun `readiness is down after a failed send from KafkaTemplate`(output: CapturedOutput) {
        val tooLarge = "x".repeat(DataSize.ofMegabytes(4).toBytes().toInt())

        // The producer rejects the record before it is sent, so send() throws instead of returning a failed future.
        assertThatThrownBy { template.send(healthTestTopic, "too-large", tooLarge) }
            .hasRootCauseInstanceOf(RecordTooLargeException::class.java)
        try {
            val readiness = health("readiness")
            assertThat(readiness.status).isEqualTo(503)
            assertThat(readiness.status("kafkaConnectivity")).isEqualTo("DOWN")
            assertThat(health("liveness").status).isEqualTo(200)
            assertThat(output).contains("Exception thrown when sending a message with key='too-large'")
        } finally {
            // The failure leaves the 500 ms window, so the other tests in the context start from UP.
            Thread.sleep(600)
        }

        assertThat(health("readiness").status).isEqualTo(200)
    }

    private class Response(
        val status: Int,
        val body: JsonNode,
    ) {
        fun components() = body["components"].propertyNames()

        fun status(component: String) = body["components"][component]["status"].asString()
    }

    private fun health(group: String): Response {
        val request = HttpRequest.newBuilder(URI("http://localhost:$port/actuator/health/$group")).build()
        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
        return Response(response.statusCode(), JsonMapper.builder().build().readTree(response.body()))
    }

    private companion object {
        const val LISTENER_ID = "health-test"
    }
}
