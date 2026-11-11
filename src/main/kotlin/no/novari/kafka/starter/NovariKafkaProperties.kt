package no.novari.kafka.starter

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("novari.kafka")
public class NovariKafkaProperties(
    /** Written to the origin header and used in `spring.kafka.client-id`. Defaults to `spring.application.name`. */
    public val applicationId: String? = null,
    public val topic: Topic = Topic(),
    public val consumer: Consumer = Consumer(),
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
}
