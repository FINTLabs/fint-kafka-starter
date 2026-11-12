package no.novari.kafka.starter.autoconfigure

import no.novari.kafka.starter.NovariKafkaProperties
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.converter.JacksonJsonMessageConverter
import org.springframework.kafka.support.converter.RecordMessageConverter
import org.springframework.kafka.support.serializer.JacksonJsonSerializer
import tools.jackson.databind.json.JsonMapper

@AutoConfiguration(after = [JacksonAutoConfiguration::class, KafkaAutoConfiguration::class])
@EnableConfigurationProperties(NovariKafkaProperties::class)
internal class NovariKafkaAutoConfiguration {
    /**
     * Gives the JSON value serializer the application's [JsonMapper], unless another value serializer is configured.
     * Conditional on the bean name, so the application's own customizers do not turn this one off.
     */
    @Bean
    @ConditionalOnMissingBean(name = ["novariKafkaProducerFactoryCustomizer"])
    fun novariKafkaProducerFactoryCustomizer(jsonMapper: JsonMapper) =
        DefaultKafkaProducerFactoryCustomizer { factory ->
            val valueSerializer = factory.configurationProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG]
            if (valueSerializer == JacksonJsonSerializer::class.java) {
                @Suppress("UNCHECKED_CAST")
                (factory as DefaultKafkaProducerFactory<Any, Any>).valueSerializer = JacksonJsonSerializer(jsonMapper)
            }
        }

    @Bean
    @ConditionalOnMissingBean
    fun kafkaRecordMessageConverter(jsonMapper: JsonMapper): RecordMessageConverter =
        JacksonJsonMessageConverter(jsonMapper)
}
