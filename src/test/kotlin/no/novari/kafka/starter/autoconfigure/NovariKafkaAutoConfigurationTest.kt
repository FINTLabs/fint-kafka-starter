package no.novari.kafka.starter.autoconfigure

import no.novari.kafka.starter.NovariKafkaProperties
import no.novari.kafka.starter.producer.OriginHeaderProducerInterceptor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.internals.RecordHeaders
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.converter.JacksonJsonMessageConverter
import org.springframework.kafka.support.converter.MessagingMessageConverter
import org.springframework.kafka.support.converter.RecordMessageConverter
import org.springframework.kafka.support.serializer.JacksonJsonSerializer
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.json.JsonMapper

class NovariKafkaAutoConfigurationTest {
    // ApplicationContextRunner does not run EnvironmentPostProcessors, so it runs as an initializer after the properties.
    private val runnerWithoutApplicationId =
        ApplicationContextRunner()
            .withInitializer {
                NovariKafkaDefaultsEnvironmentPostProcessor().postProcessEnvironment(
                    it.environment,
                    SpringApplication(),
                )
            }.withConfiguration(
                AutoConfigurations.of(
                    JacksonAutoConfiguration::class.java,
                    KafkaAutoConfiguration::class.java,
                    NovariKafkaAutoConfiguration::class.java,
                ),
            )

    private val runner = runnerWithoutApplicationId.withPropertyValues("novari.kafka.application-id=app")

    class Default(
        val property: String,
        val value: String,
        val override: String,
        val actual: (AssertableApplicationContext) -> Any?,
    ) {
        override fun toString() = property
    }

    @ParameterizedTest
    @MethodSource("defaults")
    fun `sets the fint default`(default: Default) {
        runner.run { assertThat(default.actual(it).toString()).isEqualTo(default.value) }
    }

    @ParameterizedTest
    @MethodSource("defaults")
    fun `the application overrides the fint default`(default: Default) {
        runner
            .withPropertyValues("${default.property}=${default.override}")
            .run { assertThat(default.actual(it).toString()).isEqualTo(default.override) }
    }

    @Test
    fun `leaves fetch max bytes at the Kafka default`() {
        runner.run { assertThat(it.consumerConfig()).doesNotContainKey(ConsumerConfig.FETCH_MAX_BYTES_CONFIG) }
    }

    @Test
    fun `client-id is application-id and org-id`() {
        runner
            .withPropertyValues("novari.kafka.topic.org-id=afk-no")
            .run { assertThat(it.consumerConfig()[ConsumerConfig.CLIENT_ID_CONFIG]).isEqualTo("app-afk-no") }
    }

    @Test
    fun `application-id falls back to spring application name`() {
        runnerWithoutApplicationId.withPropertyValues("spring.application.name=name").run {
            assertThat(it.getBean(NovariKafkaProperties::class.java).applicationId).isEqualTo("name")
            assertThat(it.consumerConfig()[ConsumerConfig.CLIENT_ID_CONFIG]).isEqualTo("name")
            assertThat(it.producerConfig()[OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG])
                .isEqualTo("name")
        }
    }

    @Test
    fun `serializes values with the application's JsonMapper and writes the type header`() {
        runner
            .withBean(JsonMapper::class.java, {
                JsonMapper.builder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build()
            })
            .run {
                val headers = RecordHeaders()
                val bytes = it.producerFactory().valueSerializer!!.serialize("topic", headers, Dto("x"))

                assertThat(String(bytes)).isEqualTo("""{"some_value":"x"}""")
                assertThat(String(headers.lastHeader("__TypeId__").value())).isEqualTo(Dto::class.java.name)
            }
    }

    @Test
    fun `registers the JSON message converter`() {
        runner.run {
            assertThat(
                it,
            ).getBean(RecordMessageConverter::class.java).isInstanceOf(JacksonJsonMessageConverter::class.java)
        }
    }

    @Test
    fun `backs off for the application's own message converter`() {
        runner
            .withBean(RecordMessageConverter::class.java, { MessagingMessageConverter() })
            .run {
                assertThat(it)
                    .getBean(RecordMessageConverter::class.java)
                    .isExactlyInstanceOf(MessagingMessageConverter::class.java)
            }
    }

    @Test
    fun `backs off for the application's own producer factory customizer`() {
        runner
            .withBean("novariKafkaProducerFactoryCustomizer", DefaultKafkaProducerFactoryCustomizer::class.java, {
                DefaultKafkaProducerFactoryCustomizer {}
            })
            .run { assertThat(it.producerFactory().valueSerializer).isNull() }
    }

    @Test
    fun `ignores the fint-kafka 6 key fint kafka enable-ssl`() {
        runner
            .withPropertyValues("fint.kafka.enable-ssl=true")
            .run { assertThat(it.consumerConfig()).doesNotContainKey("security.protocol") }
    }

    data class Dto(
        val someValue: String,
    )

    companion object {
        private fun AssertableApplicationContext.consumerConfig() =
            getBean(DefaultKafkaConsumerFactory::class.java).configurationProperties

        @Suppress("UNCHECKED_CAST")
        private fun AssertableApplicationContext.producerFactory() =
            getBean(DefaultKafkaProducerFactory::class.java) as DefaultKafkaProducerFactory<Any, Any>

        private fun AssertableApplicationContext.producerConfig() = producerFactory().configurationProperties

        private fun AssertableApplicationContext.kafkaProperties() = getBean(KafkaProperties::class.java)

        @JvmStatic
        fun defaults() =
            listOf(
                Default("spring.kafka.consumer.auto-offset-reset", "earliest", "latest") {
                    it.consumerConfig()[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG]
                },
                Default("spring.kafka.consumer.properties.max.partition.fetch.bytes", "3145728", "1048576") {
                    it.consumerConfig()[ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG]
                },
                Default("spring.kafka.producer.properties.max.request.size", "3145728", "1048576") {
                    it.producerConfig()[ProducerConfig.MAX_REQUEST_SIZE_CONFIG]
                },
                Default(
                    "spring.kafka.producer.value-serializer",
                    JacksonJsonSerializer::class.java.name,
                    "org.apache.kafka.common.serialization.StringSerializer",
                ) {
                    val factory = it.producerFactory()
                    (
                        factory.valueSerializer?.javaClass
                            ?: factory.configurationProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] as Class<*>
                    ).name
                },
                Default(
                    "spring.kafka.producer.properties.interceptor.classes",
                    OriginHeaderProducerInterceptor::class.java.name,
                    "com.example.OtherInterceptor",
                ) { it.producerConfig()[ProducerConfig.INTERCEPTOR_CLASSES_CONFIG] },
                Default("spring.kafka.producer.properties.origin.application.id", "app", "other") {
                    it.producerConfig()[OriginHeaderProducerInterceptor.ORIGIN_APPLICATION_ID_PRODUCER_CONFIG]
                },
                Default("spring.kafka.client-id", "app", "other") {
                    it.consumerConfig()[ConsumerConfig.CLIENT_ID_CONFIG]
                },
                Default("spring.kafka.admin.modify-topic-configs", "true", "false") {
                    it.kafkaProperties().admin.isModifyTopicConfigs
                },
                Default("spring.kafka.listener.observation-enabled", "true", "false") {
                    it
                        .getBean(
                            ConcurrentKafkaListenerContainerFactory::class.java,
                        ).containerProperties.isObservationEnabled
                },
                Default("spring.kafka.template.observation-enabled", "true", "false") {
                    it.kafkaProperties().template.isObservationEnabled
                },
            )
    }
}
