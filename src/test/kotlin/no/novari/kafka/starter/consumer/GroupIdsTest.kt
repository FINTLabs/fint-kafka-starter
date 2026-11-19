package no.novari.kafka.starter.consumer

import no.novari.kafka.starter.autoconfigure.NovariKafkaAutoConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.boot.test.context.assertj.AssertableApplicationContext
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListener

/** Group ids must equal fint-kafka 6, so no consumer group changes when an application moves to this starter. */
@ExtendWith(OutputCaptureExtension::class)
class GroupIdsTest {
    // The containers are not started, so no broker is needed.
    private val runnerWithoutBase =
        ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    JacksonAutoConfiguration::class.java,
                    KafkaAutoConfiguration::class.java,
                    NovariKafkaAutoConfiguration::class.java,
                ),
            ).withPropertyValues("spring.kafka.listener.auto-startup=false")

    private val runner = runnerWithoutBase.withPropertyValues("spring.kafka.consumer.group-id=test")

    class GoldenRow(
        val case: String,
        val suffix: String,
        val groupId: String,
    ) {
        override fun toString() = "$case $suffix"
    }

    @ParameterizedTest
    @MethodSource("golden")
    fun `group id equals fint-kafka 6`(row: GoldenRow) {
        val groupIds = KafkaGroupIds("test")
        when (row.case) {
            "default" -> {
                runner.withBean(DefaultListener::class.java).run {
                    assertThat(it.groupIds()).containsExactly(row.groupId)
                }
            }

            "suffix" -> {
                assertThat(groupIds.withSuffix(row.suffix)).isEqualTo(row.groupId)
            }

            else -> {
                assertThat(row.groupId).matches(UNIQUE)
                assertThat(groupIds.unique()).matches(UNIQUE)
            }
        }
    }

    @Test
    fun `unique gives a new group id every time`() {
        val groupIds = KafkaGroupIds("test")
        assertThat(groupIds.unique()).isNotEqualTo(groupIds.unique())
    }

    @Test
    fun `KafkaListener resolves group ids from the kafkaGroupIds bean`(output: CapturedOutput) {
        runner.withBean(SpelListener::class.java).run {
            assertThat(it).hasNotFailed()
            val (cache, unique) = it.groupIds().partition { groupId -> groupId == "test-cache" }
            assertThat(cache).hasSize(1)
            assertThat(unique.single()).matches(UNIQUE)
            assertThat(output).contains("uses group.id test-cache").doesNotContain("does not start with")
        }
    }

    @Test
    fun `listener id outside the base group id gives a warning by default`(output: CapturedOutput) {
        runner.withBean(IdListener::class.java).run {
            assertThat(it).hasNotFailed()
            assertThat(it.groupIds()).containsExactly("x")
            assertThat(output).contains(
                "WARN",
                "Kafka listener x uses group.id x",
                "Kafka listeners with a group.id that does not start with spring.kafka.consumer.group-id 'test': x → x.",
            )
        }
    }

    @Test
    fun `group id check FAIL stops startup`() {
        runner
            .withBean(IdListener::class.java)
            .withPropertyValues("novari.kafka.consumer.group-id-check=FAIL")
            .run {
                assertThat(it).hasFailed()
                assertThat(it.startupFailure).hasMessageStartingWith(
                    "Kafka listeners with a group.id that does not start with spring.kafka.consumer.group-id 'test': x → x.",
                )
            }
    }

    @Test
    fun `group id check OFF only logs the group ids`(output: CapturedOutput) {
        runner
            .withBean(IdListener::class.java)
            .withPropertyValues("novari.kafka.consumer.group-id-check=OFF")
            .run {
                assertThat(it).hasNotFailed()
                assertThat(output).contains("Kafka listener x uses group.id x").doesNotContain("does not start with")
            }
    }

    @Test
    fun `checks listener containers declared as beans`() {
        runner
            .withUserConfiguration(BeanContainerConfig::class.java)
            .withPropertyValues("novari.kafka.consumer.group-id-check=FAIL")
            .run {
                assertThat(it).hasFailed()
                assertThat(it.startupFailure).hasMessageContaining("beanContainer → other")
            }
    }

    @Test
    fun `fails at startup without a base group id when there are listeners`() {
        runnerWithoutBase
            .withBean(IdListener::class.java)
            .run {
                assertThat(it).hasFailed()
                assertThat(it.startupFailure).hasMessageStartingWith(
                    "spring.kafka.consumer.group-id is not set.",
                )
            }
    }

    @Test
    fun `starts without a base group id when there are no listeners`() {
        runnerWithoutBase.run { assertThat(it).hasNotFailed() }
    }

    class DefaultListener {
        @KafkaListener(topics = ["t"])
        fun on(value: String) {}
    }

    class SpelListener {
        @KafkaListener(topics = ["t"], groupId = "#{@kafkaGroupIds.withSuffix('-cache')}")
        fun cache(value: String) {}

        @KafkaListener(topics = ["t"], groupId = "#{@kafkaGroupIds.unique()}")
        fun unique(value: String) {}
    }

    @Configuration
    class BeanContainerConfig {
        @Bean
        fun beanContainer(factory: ConsumerFactory<String, String>) =
            KafkaMessageListenerContainer(
                factory,
                ContainerProperties("t").apply {
                    setGroupId("other")
                    setMessageListener(MessageListener<String, String> {})
                },
            ).apply { isAutoStartup = false }
    }

    class IdListener {
        @KafkaListener(id = "x", topics = ["t"])
        fun on(value: String) {}
    }

    companion object {
        private val UNIQUE = Regex("test[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").toPattern()

        private fun AssertableApplicationContext.groupIds() =
            getBean(KafkaListenerEndpointRegistry::class.java).listenerContainers.map { it.groupId }

        @JvmStatic
        fun golden() =
            GroupIdsTest::class.java
                .getResource("/golden/group-ids.tsv")!!
                .readText()
                .lines()
                .filter { it.isNotBlank() && !it.startsWith("#") }
                .drop(1)
                .map { it.split("\t").let { (case, suffix, groupId) -> GoldenRow(case, suffix, groupId) } }
    }
}
