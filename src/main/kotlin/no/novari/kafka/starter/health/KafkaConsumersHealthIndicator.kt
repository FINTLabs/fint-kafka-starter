package no.novari.kafka.starter.health

import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.context.event.EventListener
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.event.ConsumerStoppedEvent
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * DOWN when a consumer has stopped with a reason other than NORMAL, or, after [ApplicationReadyEvent], when an
 * auto-startup container is not running. In the liveness group.
 *
 * Reads [KafkaListenerEndpointRegistry.getAllListenerContainers], which has both the `@KafkaListener` containers and
 * the containers declared as beans. fint-kafka 6 only read the beans.
 *
 * Ported from `no.novari.kafka.health.KafkaConsumersHealthIndicator` in fint-kafka 6.3.1 (commit `1ff1899`).
 */
internal class KafkaConsumersHealthIndicator(
    private val registry: KafkaListenerEndpointRegistry,
) : HealthIndicator {
    private val fatalStops = ConcurrentHashMap<String, String>()
    private val applicationReady = AtomicBoolean(false)

    @EventListener
    fun onApplicationReady(event: ApplicationReadyEvent) {
        applicationReady.set(true)
    }

    @EventListener
    fun onConsumerStopped(event: ConsumerStoppedEvent) {
        val source = event.source.toString()
        val reason = event.reason
        if (reason == ConsumerStoppedEvent.Reason.NORMAL) {
            fatalStops.remove(source)
        } else {
            log.error("Kafka consumer stopped with fatal reason {} ({}); marking liveness DOWN", reason, source)
            fatalStops[source] = reason.name
        }
    }

    override fun health(): Health {
        if (fatalStops.isNotEmpty()) {
            return Health
                .down()
                .withDetail("reason", "Consumer container(s) fatally stopped")
                .withDetail("fatalStops", fatalStops.toMap())
                .build()
        }

        val stoppedContainers = stoppedAutoStartupContainers()
        if (stoppedContainers.isNotEmpty()) {
            return Health
                .down()
                .withDetail("reason", "Consumer container(s) not running")
                .withDetail("stoppedContainers", stoppedContainers)
                .build()
        }

        return Health.up().build()
    }

    private fun stoppedAutoStartupContainers(): List<String> {
        if (!applicationReady.get()) {
            return emptyList()
        }
        return registry.allListenerContainers
            .filter { it.isAutoStartup && !it.isRunning }
            .map { it.listenerId ?: it.toString() }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(KafkaConsumersHealthIndicator::class.java)
    }
}
