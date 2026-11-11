package no.novari.kafka.starter.health

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.springframework.boot.SpringApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.health.contributor.Status
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.event.ConsumerStoppedEvent
import org.springframework.kafka.event.ConsumerStoppedEvent.Reason
import org.springframework.kafka.listener.MessageListenerContainer
import java.time.Duration

class KafkaConsumersHealthIndicatorTest {
    private val consumer = Any()
    private val indicator = KafkaConsumersHealthIndicator(KafkaListenerEndpointRegistry())

    @Test
    fun `is up when no consumer has stopped`() {
        assertThat(indicator.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `is down when a consumer stops with a fatal reason`() {
        indicator.onConsumerStopped(stopped(Reason.AUTH))

        assertThat(indicator.health().status).isEqualTo(Status.DOWN)
    }

    @Test
    fun `stays up when a consumer stops with the normal reason`() {
        indicator.onConsumerStopped(stopped(Reason.NORMAL))

        assertThat(indicator.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `recovers to up once the same consumer later stops with the normal reason`() {
        indicator.onConsumerStopped(stopped(Reason.ERROR))
        assertThat(indicator.health().status).isEqualTo(Status.DOWN)

        indicator.onConsumerStopped(stopped(Reason.NORMAL))
        assertThat(indicator.health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `ignores a stopped container until the application is ready`() {
        val container = mock(MessageListenerContainer::class.java)
        `when`(container.isAutoStartup).thenReturn(true)
        `when`(container.isRunning).thenReturn(false)
        val registry = mock(KafkaListenerEndpointRegistry::class.java)
        `when`(registry.allListenerContainers).thenReturn(listOf(container))
        val indicator = KafkaConsumersHealthIndicator(registry)

        assertThat(indicator.health().status).isEqualTo(Status.UP)

        indicator.onApplicationReady(applicationReadyEvent())

        assertThat(indicator.health().status).isEqualTo(Status.DOWN)
    }

    private fun stopped(reason: Reason) = ConsumerStoppedEvent(consumer, Any(), reason)

    private fun applicationReadyEvent(): ApplicationReadyEvent =
        ApplicationReadyEvent(
            SpringApplication(),
            arrayOf(),
            mock(ConfigurableApplicationContext::class.java),
            Duration.ZERO,
        )
}
