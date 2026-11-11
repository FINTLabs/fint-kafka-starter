package no.novari.kafka.starter.health

import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import java.time.Duration

@ExtendWith(OutputCaptureExtension::class)
class ProducerFailureTrackerTest {
    private val record = ProducerRecord<Any, Any>("topic", "key", "value")

    @Test
    fun `is healthy when failure count is below threshold`() {
        val tracker = ProducerFailureTracker(3, Duration.ofMinutes(1))

        repeat(2) { tracker.onError(record, null, RuntimeException("boom")) }

        assertThat(tracker.isUnhealthy()).isFalse()
    }

    @Test
    fun `is unhealthy when failure count reaches threshold`() {
        val tracker = ProducerFailureTracker(3, Duration.ofMinutes(1))

        repeat(3) { tracker.onError(record, null, RuntimeException("boom")) }

        assertThat(tracker.isUnhealthy()).isTrue()
    }

    @Test
    fun `forgets failures once they fall outside the failure window`() {
        val tracker = ProducerFailureTracker(2, Duration.ofMillis(20))

        repeat(2) { tracker.onError(record, null, RuntimeException("boom")) }
        assertThat(tracker.isUnhealthy()).isTrue()

        Thread.sleep(40)

        assertThat(tracker.isUnhealthy()).isFalse()
    }

    @Test
    fun `logs the failure like Boot's LoggingProducerListener`(output: CapturedOutput) {
        ProducerFailureTracker(3, Duration.ofMinutes(1)).onError(record, null, RuntimeException("boom"))

        assertThat(output).contains("Exception thrown when sending a message with key='key'", "to topic topic", "boom")
    }
}
