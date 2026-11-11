package no.novari.kafka.starter.health

import no.novari.kafka.starter.NovariKafkaProperties
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.Admin
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.support.ProducerListener

/**
 * Before [KafkaAutoConfiguration], so Boot's `KafkaTemplate` gets the [ProducerFailureTracker] as its
 * [ProducerListener] instead of `LoggingProducerListener`.
 */
@AutoConfiguration(before = [KafkaAutoConfiguration::class])
@ConditionalOnClass(HealthIndicator::class)
@ConditionalOnBooleanProperty("novari.kafka.health.enabled", matchIfMissing = true)
@EnableConfigurationProperties(NovariKafkaProperties::class)
internal class NovariKafkaHealthAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(ProducerListener::class)
    fun producerFailureTracker(properties: NovariKafkaProperties) =
        ProducerFailureTracker(properties.health.producer.failureThreshold, properties.health.producer.failureWindow)

    @Bean
    fun kafkaConsumersHealthIndicator(
        // Registered by @EnableKafka in Boot's KafkaAnnotationDrivenConfiguration, which IntelliJ does not see.
        @Suppress("SpringJavaInjectionPointsAutowiringInspection")
        registry: KafkaListenerEndpointRegistry,
    ) = KafkaConsumersHealthIndicator(registry)

    /** Its own admin client, with `-health` added to `client.id`, so it does not clash with [KafkaAdmin]'s. */
    @Bean
    fun kafkaConnectivityHealthIndicator(
        kafkaAdmin: KafkaAdmin,
        producerFailureTracker: ProducerFailureTracker?,
        properties: NovariKafkaProperties,
    ): KafkaConnectivityHealthIndicator {
        val config = kafkaAdmin.configurationProperties.toMutableMap()
        config.computeIfPresent(CommonClientConfigs.CLIENT_ID_CONFIG) { _, clientId -> "$clientId-health" }
        return KafkaConnectivityHealthIndicator(
            Admin.create(config),
            producerFailureTracker,
            properties.health.connectivity,
        )
    }
}
