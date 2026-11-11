package no.novari.kafka.starter.producer

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.internals.RecordHeader

/**
 * Adds the header [ORIGIN_APPLICATION_ID_RECORD_HEADER] to every record, with the value of the producer config
 * [ORIGIN_APPLICATION_ID_PRODUCER_CONFIG] in UTF-8.
 *
 * Ported from `no.novari.kafka.OriginHeaderProducerInterceptor` in fint-kafka 6.3.1 (commit `1ff1899`).
 */
public class OriginHeaderProducerInterceptor : ProducerInterceptor<Any?, Any?> {
    private lateinit var originApplicationIdHeader: RecordHeader

    override fun configure(configs: Map<String, *>) {
        val applicationId = configs[ORIGIN_APPLICATION_ID_PRODUCER_CONFIG] as String?
        originApplicationIdHeader = RecordHeader(ORIGIN_APPLICATION_ID_RECORD_HEADER, applicationId?.toByteArray())
    }

    override fun onSend(record: ProducerRecord<Any?, Any?>): ProducerRecord<Any?, Any?> =
        record.apply { headers().add(originApplicationIdHeader) }

    override fun onAcknowledgement(
        metadata: RecordMetadata?,
        exception: Exception?,
    ) {}

    override fun close() {}

    public companion object {
        public const val ORIGIN_APPLICATION_ID_PRODUCER_CONFIG: String = "origin.application.id"
        public const val ORIGIN_APPLICATION_ID_RECORD_HEADER: String = "origin.application.id"
    }
}
