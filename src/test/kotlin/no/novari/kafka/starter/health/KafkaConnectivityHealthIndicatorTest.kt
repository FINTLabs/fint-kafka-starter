package no.novari.kafka.starter.health

import no.novari.kafka.starter.NovariKafkaProperties
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.DescribeClusterOptions
import org.apache.kafka.clients.admin.DescribeClusterResult
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.springframework.boot.health.contributor.Status
import java.time.Duration

class KafkaConnectivityHealthIndicatorTest {
    private val reachable = KafkaFuture.completedFuture("cluster-1")
    private val unreachable =
        KafkaFutureImpl<String>().apply { completeExceptionally(RuntimeException("no route to broker")) }

    @Test
    fun `is up when the cluster is reachable`() {
        assertThat(indicator(reachable).health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `goes down only after consecutive failures reach the threshold`() {
        val indicator = indicator(unreachable)

        assertThat(indicator.health().status).isEqualTo(Status.UP)
        assertThat(indicator.health().status).isEqualTo(Status.DOWN)
    }

    @Test
    fun `is down when the producer is unhealthy`() {
        assertThat(indicator(reachable, unhealthyTracker()).health().status).isEqualTo(Status.DOWN)
    }

    @Test
    fun `ignores producer failures without a tracker`() {
        assertThat(indicator(reachable, tracker = null).health().status).isEqualTo(Status.UP)
    }

    @Test
    fun `merges producer and connectivity details when both are unhealthy`() {
        val indicator = indicator(unreachable, unhealthyTracker())
        indicator.health()
        val health = indicator.health()

        assertThat(health.status).isEqualTo(Status.DOWN)
        assertThat(health.details)
            .containsEntry("producerFailure", "Producer send failures exceeded threshold")
            .containsEntry("reason", "Kafka cluster not reachable")
            .containsKey("consecutiveFailures")
            .containsKey("error")
    }

    private fun indicator(
        clusterId: KafkaFuture<String>,
        tracker: ProducerFailureTracker? = ProducerFailureTracker(1, Duration.ofMinutes(1)),
    ): KafkaConnectivityHealthIndicator {
        val result = mock(DescribeClusterResult::class.java)
        `when`(result.clusterId()).thenReturn(clusterId)
        val admin = mock(Admin::class.java)
        `when`(admin.describeCluster(any(DescribeClusterOptions::class.java))).thenReturn(result)
        val properties =
            NovariKafkaProperties.Health.Connectivity(
                timeout = Duration.ofMillis(500),
                cacheTtl = Duration.ZERO,
                failureThreshold = 2,
            )
        return KafkaConnectivityHealthIndicator(admin, tracker, properties)
    }

    private fun unhealthyTracker() =
        ProducerFailureTracker(1, Duration.ofMinutes(1)).apply {
            onError(ProducerRecord("topic", "key", "value"), null, RuntimeException("boom"))
        }
}
