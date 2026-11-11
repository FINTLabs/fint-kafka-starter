package no.novari.kafka.starter.health

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

/** Readiness goes DOWN when the cluster cannot be reached. Liveness stays UP, so the pod is not restarted. */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "spring.kafka.bootstrap-servers=localhost:1",
        "management.endpoint.health.show-components=always",
        "novari.kafka.health.connectivity.timeout=200ms",
        "novari.kafka.health.connectivity.cache-ttl=0s",
        "novari.kafka.health.connectivity.failure-threshold=2",
    ],
)
class HealthEndpointWithoutBrokerTest(
    @LocalServerPort private val port: Int,
) {
    @Test
    fun `readiness goes down after consecutive failed probes and liveness stays up`() {
        assertThat(status("readiness")).isEqualTo(200)
        assertThat(status("readiness")).isEqualTo(503)
        assertThat(status("liveness")).isEqualTo(200)
    }

    private fun status(group: String): Int {
        val request = HttpRequest.newBuilder(URI("http://localhost:$port/actuator/health/$group")).build()
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding()).statusCode()
    }
}
