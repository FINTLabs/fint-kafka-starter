package no.novari.kafka.starter.autoconfigure

import no.novari.kafka.starter.producer.OriginHeaderProducerInterceptor
import org.springframework.boot.EnvironmentPostProcessor
import org.springframework.boot.SpringApplication
import org.springframework.core.Ordered
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource
import org.springframework.kafka.support.serializer.JacksonJsonSerializer
import org.springframework.util.unit.DataSize

/**
 * Adds the fint defaults as the property source with the lowest precedence, so they show in `/actuator/env` and any
 * `spring.kafka.*` key set by the application wins.
 */
internal class NovariKafkaDefaultsEnvironmentPostProcessor :
    EnvironmentPostProcessor,
    Ordered {
    override fun getOrder() = Ordered.LOWEST_PRECEDENCE

    override fun postProcessEnvironment(
        environment: ConfigurableEnvironment,
        application: SpringApplication,
    ) {
        val applicationId =
            environment.getProperty("novari.kafka.application-id") ?: environment.getProperty("spring.application.name")
        val orgId = environment.getProperty("novari.kafka.topic.org-id")
        val defaults =
            buildMap<String, Any> {
                put("spring.kafka.consumer.auto-offset-reset", "earliest")
                put("spring.kafka.consumer.properties[max.partition.fetch.bytes]", THREE_MIB.toBytes())
                put("spring.kafka.producer.properties[max.request.size]", THREE_MIB.toBytes())
                put("spring.kafka.producer.value-serializer", JacksonJsonSerializer::class.java.name)
                put(
                    "spring.kafka.producer.properties[interceptor.classes]",
                    OriginHeaderProducerInterceptor::class.java.name,
                )
                put("spring.kafka.admin.modify-topic-configs", "true")
                put("spring.kafka.listener.observation-enabled", "true")
                put("spring.kafka.template.observation-enabled", "true")
                if (applicationId != null) {
                    put("novari.kafka.application-id", applicationId)
                    put(
                        "spring.kafka.producer.properties[${OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG}]",
                        applicationId,
                    )
                    put("spring.kafka.client-id", listOfNotNull(applicationId, orgId).joinToString("-"))
                }
            }
        environment.propertySources.addLast(MapPropertySource(PROPERTY_SOURCE_NAME, defaults))
    }

    private companion object {
        const val PROPERTY_SOURCE_NAME = "novariKafkaDefaults"
        val THREE_MIB = DataSize.ofMegabytes(3)
    }
}
