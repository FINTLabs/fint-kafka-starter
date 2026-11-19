package no.novari.kafka.starter.consumer

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import java.time.Duration
import java.time.Instant

/**
 * A closed application leaves its consumer group at once. Without the LeaveGroup request, a new instance would wait
 * up to `session.timeout.ms` (45 s) for its assignment, as in a rolling deployment.
 */
@Tag("slow")
@EmbeddedKafka(partitions = 1, topics = [LeaveGroupTest.TOPIC])
class LeaveGroupTest {
    class Listener {
        @KafkaListener(topics = [TOPIC])
        fun on(value: String) {}
    }

    @Test
    fun `closed application leaves the group, so a new instance does not wait for the session timeout`(
        broker: EmbeddedKafkaBroker,
    ) {
        val runner =
            ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration::class.java))
                .withBean(Listener::class.java)
                .withPropertyValues(
                    "spring.kafka.bootstrap-servers=${broker.brokersAsString}",
                    "spring.kafka.consumer.group-id=$GROUP",
                    "spring.kafka.consumer.properties.session.timeout.ms=45000",
                )

        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to broker.brokersAsString)).use { admin ->
            runner.run {
                assertThat(admin.waitForGroup(Duration.ofSeconds(30)) { it.hasAssignedMember() }).isTrue()
            }
            assertThat(admin.waitForGroup(Duration.ofSeconds(30)) { it.members().isEmpty() }).isTrue()

            runner.run {
                assertThat(admin.waitForGroup(Duration.ofSeconds(10)) { it.hasAssignedMember() }).isTrue()
            }
        }
    }

    private fun ConsumerGroupDescription.hasAssignedMember() =
        members().size == 1 &&
            members()
                .single()
                .assignment()
                .topicPartitions()
                .isNotEmpty()

    private fun AdminClient.waitForGroup(
        timeout: Duration,
        condition: (ConsumerGroupDescription) -> Boolean,
    ): Boolean {
        val deadline = Instant.now() + timeout
        while (Instant.now() < deadline) {
            val group =
                describeConsumerGroups(listOf(GROUP))
                    .describedGroups()
                    .getValue(GROUP)
                    .get()
            if (condition(group)) return true
            Thread.sleep(100)
        }
        return false
    }

    companion object {
        const val TOPIC = "leave-group"
        const val GROUP = "leave-group-test"
    }
}
