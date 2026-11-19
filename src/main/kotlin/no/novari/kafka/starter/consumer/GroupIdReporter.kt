package no.novari.kafka.starter.consumer

import no.novari.kafka.starter.NovariKafkaProperties.GroupIdCheck
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.kafka.config.KafkaListenerEndpointRegistry

/**
 * Logs the group id of every listener container at startup, and checks that it is the base group id or starts with
 * it. A group id outside the base, such as from `@KafkaListener(id = …)`, often means a new consumer group.
 * The check does not catch a changed suffix.
 */
internal class GroupIdReporter(
    private val registry: KafkaListenerEndpointRegistry,
    private val groupIds: KafkaGroupIds,
    private val check: GroupIdCheck,
) : SmartInitializingSingleton {
    override fun afterSingletonsInstantiated() {
        val containers = registry.allListenerContainers
        if (containers.isEmpty()) return
        val base = groupIds.base ?: throw IllegalStateException(MISSING_BASE)

        val deviations =
            containers.mapNotNull { container ->
                val listenerId = container.listenerId
                val groupId = container.groupId
                log.info("Kafka listener {} uses group.id {}", listenerId, groupId)
                "$listenerId → $groupId".takeUnless { groupId != null && groupId.startsWith(base) }
            }
        if (deviations.isEmpty() || check == GroupIdCheck.OFF) return

        val message =
            "Kafka listeners with a group.id that does not start with spring.kafka.consumer.group-id '$base': " +
                "${deviations.joinToString()}. A changed group.id gives a new consumer group. " +
                "@KafkaListener(id = …) also sets the group.id, unless idIsGroup = false. " +
                "Set novari.kafka.consumer.group-id-check to WARN, FAIL or OFF."
        if (check == GroupIdCheck.FAIL) throw IllegalStateException(message)
        log.warn(message)
    }

    private companion object {
        val log = LoggerFactory.getLogger(GroupIdReporter::class.java)
    }
}
