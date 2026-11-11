package no.novari.kafka.starter.health

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.kafka.support.LoggingProducerListener
import org.springframework.kafka.support.ProducerListener

class NovariKafkaHealthAutoConfigurationTest {
    private val runner =
        ApplicationContextRunner().withConfiguration(
            AutoConfigurations.of(KafkaAutoConfiguration::class.java, NovariKafkaHealthAutoConfiguration::class.java),
        )

    @Test
    fun `replaces Boot's LoggingProducerListener with the tracker`() {
        runner.run {
            assertThat(it).getBean(ProducerListener::class.java).isInstanceOf(ProducerFailureTracker::class.java)
            assertThat(it).doesNotHaveBean(LoggingProducerListener::class.java)
            assertThat(it).hasSingleBean(KafkaConnectivityHealthIndicator::class.java)
            assertThat(it).hasSingleBean(KafkaConsumersHealthIndicator::class.java)
        }
    }

    @Test
    fun `backs off for the application's own ProducerListener`() {
        runner
            .withBean(ProducerListener::class.java, { object : ProducerListener<Any, Any> {} })
            .run {
                assertThat(it).hasNotFailed()
                assertThat(it).doesNotHaveBean(ProducerFailureTracker::class.java)
                assertThat(it).hasSingleBean(KafkaConnectivityHealthIndicator::class.java)
            }
    }

    @Test
    fun `registers nothing when health is disabled`() {
        runner.withPropertyValues("novari.kafka.health.enabled=false").run {
            assertThat(it).doesNotHaveBean(KafkaConnectivityHealthIndicator::class.java)
            assertThat(it).doesNotHaveBean(KafkaConsumersHealthIndicator::class.java)
            assertThat(it).getBean(ProducerListener::class.java).isInstanceOf(LoggingProducerListener::class.java)
        }
    }
}
