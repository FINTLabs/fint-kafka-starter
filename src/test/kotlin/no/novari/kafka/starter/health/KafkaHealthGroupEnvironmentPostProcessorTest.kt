package no.novari.kafka.starter.health

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.SpringApplication
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment

class KafkaHealthGroupEnvironmentPostProcessorTest {
    private val postProcessor = KafkaHealthGroupEnvironmentPostProcessor()

    @Test
    fun `adds the Kafka indicators to the probe groups`() {
        val environment = postProcess()

        assertThat(environment.getProperty(READINESS_INCLUDE)).isEqualTo("readinessState,kafkaConnectivity")
        assertThat(environment.getProperty(LIVENESS_INCLUDE)).isEqualTo("livenessState,kafkaConsumers")
        assertThat(environment.getProperty(VALIDATE_GROUP_MEMBERSHIP)).isNull()
    }

    @Test
    fun `does not override an explicit application config`() {
        val environment = postProcess(READINESS_INCLUDE to "readinessState")

        assertThat(environment.getProperty(READINESS_INCLUDE)).isEqualTo("readinessState")
        assertThat(environment.getProperty(LIVENESS_INCLUDE)).isEqualTo("livenessState,kafkaConsumers")
    }

    @Test
    fun `is skipped when health is disabled`() {
        val environment = postProcess("novari.kafka.health.enabled" to "false")

        assertThat(environment.getProperty(READINESS_INCLUDE)).isNull()
        assertThat(environment.getProperty(LIVENESS_INCLUDE)).isNull()
    }

    private fun postProcess(vararg properties: Pair<String, String>) =
        StandardEnvironment().also {
            it.propertySources.addFirst(MapPropertySource("app", mapOf(*properties)))
            postProcessor.postProcessEnvironment(it, SpringApplication())
        }

    private companion object {
        const val READINESS_INCLUDE = "management.endpoint.health.group.readiness.include"
        const val LIVENESS_INCLUDE = "management.endpoint.health.group.liveness.include"
        const val VALIDATE_GROUP_MEMBERSHIP = "management.endpoint.health.validate-group-membership"
    }
}
