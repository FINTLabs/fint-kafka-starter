package no.novari.kafka.starter

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("novari.kafka")
public class NovariKafkaProperties(
    /** Written to the origin header and used in `spring.kafka.client-id`. Defaults to `spring.application.name`. */
    public val applicationId: String? = null,
    public val topic: Topic = Topic(),
    public val consumer: Consumer = Consumer(),
    public val health: Health = Health(),
) {
    public class Topic(
        /** Default org-id in topic names, with hyphens (`afk-no`). Also used in `spring.kafka.client-id`. */
        public val orgId: String? = null,
        public val domainContext: String? = null,
        public val defaultReplicas: Int = 2,
    )

    public class Consumer(
        public val groupIdCheck: GroupIdCheck = GroupIdCheck.WARN,
    )

    public enum class GroupIdCheck { WARN, FAIL, OFF }

    public class Health(
        /** Adds `kafkaConnectivity` to the readiness group and `kafkaConsumers` to the liveness group. */
        public val enabled: Boolean = true,
        public val connectivity: Connectivity = Connectivity(),
        public val producer: Producer = Producer(),
    ) {
        public class Connectivity(
            public val timeout: Duration = Duration.ofSeconds(5),
            public val cacheTtl: Duration = Duration.ofSeconds(10),
            /** Consecutive failed probes before `kafkaConnectivity` is DOWN. */
            public val failureThreshold: Int = 4,
        )

        public class Producer(
            /** Failed sends within [failureWindow] before `kafkaConnectivity` is DOWN. */
            public val failureThreshold: Int = 5,
            public val failureWindow: Duration = Duration.ofMinutes(2),
        )
    }
}
