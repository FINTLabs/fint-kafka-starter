package no.novari.kafka.starter.health

import org.springframework.boot.EnvironmentPostProcessor
import org.springframework.boot.SpringApplication
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource

/**
 * Adds `kafkaConnectivity` to the readiness group and `kafkaConsumers` to the liveness group, unless the application
 * sets the group's `include` itself. The names are the bean names without `HealthIndicator`, as Boot registers them.
 *
 * Ported from `no.novari.kafka.health.KafkaHealthGroupEnvironmentPostProcessor` in fint-kafka 6.3.1 (commit
 * `1ff1899`), which used the bean names and turned off `validate-group-membership`.
 */
internal class KafkaHealthGroupEnvironmentPostProcessor : EnvironmentPostProcessor {
    override fun postProcessEnvironment(
        environment: ConfigurableEnvironment,
        application: SpringApplication,
    ) {
        if (!environment.getProperty("novari.kafka.health.enabled", Boolean::class.java, true)) {
            return
        }

        val defaults =
            buildMap<String, Any> {
                if (environment.getProperty(READINESS_INCLUDE) == null) {
                    put(READINESS_INCLUDE, "readinessState,$CONNECTIVITY")
                }
                if (environment.getProperty(LIVENESS_INCLUDE) == null) {
                    put(LIVENESS_INCLUDE, "livenessState,$CONSUMERS")
                }
            }
        if (defaults.isNotEmpty()) {
            environment.propertySources.addLast(MapPropertySource(PROPERTY_SOURCE_NAME, defaults))
        }
    }

    companion object {
        const val CONNECTIVITY = "kafkaConnectivity"
        const val CONSUMERS = "kafkaConsumers"
        private const val PROPERTY_SOURCE_NAME = "novariKafkaHealthGroupDefaults"
        private const val READINESS_INCLUDE = "management.endpoint.health.group.readiness.include"
        private const val LIVENESS_INCLUDE = "management.endpoint.health.group.liveness.include"
    }
}
