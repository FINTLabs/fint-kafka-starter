package no.novari.kafka.starter.health

import no.novari.kafka.starter.NovariKafkaProperties
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.slf4j.LoggerFactory
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * DOWN when the cluster has not answered `describeCluster` [NovariKafkaProperties.Health.Connectivity.failureThreshold]
 * times in a row, or when the [producerFailureTracker] is unhealthy. In the readiness group.
 *
 * Ported from `no.novari.kafka.health.KafkaConnectivityHealthIndicator` in fint-kafka 6.3.1 (commit `1ff1899`).
 */
internal class KafkaConnectivityHealthIndicator(
    private val admin: Admin,
    /** Null when the application has its own `ProducerListener`. */
    private val producerFailureTracker: ProducerFailureTracker?,
    properties: NovariKafkaProperties.Health.Connectivity,
) : HealthIndicator,
    AutoCloseable {
    private val timeoutMillis: Long = properties.timeout.toMillis()
    private val cacheTtlMillis: Long = properties.cacheTtl.toMillis()
    private val failureThreshold: Int = properties.failureThreshold

    private var lastConnectivityHealth: Health? = null
    private var lastProbeAt: Long = 0
    private var consecutiveFailures: Int = 0

    override fun health(): Health {
        val connectivity = connectivityHealth()
        if (producerFailureTracker?.isUnhealthy() != true) {
            return connectivity
        }
        return Health
            .down()
            .withDetail("producerFailure", "Producer send failures exceeded threshold")
            .withDetails(connectivity.details)
            .build()
    }

    override fun close() = admin.close()

    @Synchronized
    private fun connectivityHealth(): Health {
        val now = System.currentTimeMillis()
        lastConnectivityHealth?.let {
            if (now - lastProbeAt < cacheTtlMillis) {
                return it
            }
        }
        lastProbeAt = now

        val health =
            try {
                val clusterId =
                    admin
                        .describeCluster(DescribeClusterOptions().timeoutMs(timeoutMillis.toInt()))
                        .clusterId()
                        .get(timeoutMillis, TimeUnit.MILLISECONDS)
                consecutiveFailures = 0
                Health.up().withDetail("clusterId", clusterId ?: "unknown").build()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                onProbeFailure(e)
            } catch (e: ExecutionException) {
                onProbeFailure(e)
            } catch (e: TimeoutException) {
                onProbeFailure(e)
            } catch (e: RuntimeException) {
                onProbeFailure(e)
            }

        lastConnectivityHealth = health
        return health
    }

    private fun onProbeFailure(e: Exception): Health {
        consecutiveFailures++
        log.warn("Kafka cluster not reachable ({} consecutive failure(s)): {}", consecutiveFailures, e.message)
        return if (consecutiveFailures >= failureThreshold) {
            Health
                .down()
                .withDetail("reason", "Kafka cluster not reachable")
                .withDetail("consecutiveFailures", consecutiveFailures)
                .withDetail("error", e.message.toString())
                .build()
        } else {
            Health
                .up()
                .withDetail("degraded", "Transient connectivity failure below threshold")
                .withDetail("consecutiveFailures", consecutiveFailures)
                .build()
        }
    }

    private companion object {
        private val log = LoggerFactory.getLogger(KafkaConnectivityHealthIndicator::class.java)
    }
}
