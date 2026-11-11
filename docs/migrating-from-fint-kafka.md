# Migrating from fint-kafka

This guide moves an app from fint-kafka 6 (`no.novari:kafka`) to fint-kafka-starter (`no.novari:kafka-starter`). The
fint-kafka API is replaced by standard Spring Kafka, so all Kafka code in the app is rewritten. Consumer groups, topic
names and topic config must not change. The guide shows how to check that.

## Contents

1. [Before you start](#before-you-start)
2. [Dependency](#dependency)
3. [Properties](#properties)
4. [API translation](#api-translation)
5. [Topics](#topics)
6. [Producers](#producers)
7. [Consumers](#consumers)
8. [Group ids](#group-ids)
9. [Error handling](#error-handling)
10. [Seek to the beginning](#seek-to-the-beginning)
11. [Several or dynamic containers](#several-or-dynamic-containers)
12. [Kafka health is gone](#kafka-health-is-gone)
13. [SSL and kafkarator](#ssl-and-kafkarator)
14. [Wrapper libraries](#wrapper-libraries)
15. [Request/reply is not included](#requestreply-is-not-included)
16. [Java apps](#java-apps)
17. [Checklist](#checklist)

## Before you start

- The app runs Spring Boot 4.1+, Jackson 3 and Java 21+. fint-kafka 6 does not run on Spring Boot 4, so this is done in
  the same change as the Spring Boot upgrade.
- The app does not use request/reply. See [Request/reply is not included](#requestreply-is-not-included).
- Libraries the app uses Kafka through, such as `fint-flyt-kafka`, are migrated first. See
  [Wrapper libraries](#wrapper-libraries).
- kafkarator sets `spring.kafka.security.protocol=SSL`. See [SSL and kafkarator](#ssl-and-kafkarator).
- Before you deploy to staging, write down the app's consumer groups. See [Group ids](#group-ids).

## Dependency

```kotlin
// before
implementation("no.novari:kafka:6.x")

// after
implementation("no.novari:kafka-starter:<version>")
```

The starter brings `spring-boot-starter-kafka` and `spring-boot-starter-jackson`, and is auto-configured.

fint-kafka-starter is auto-configured and needs no component scan. fint-kafka 6 did, so apps have
`scanBasePackages = ["no.novari"]` or a `@ComponentScan` of `no.novari` or `no.novari.kafka`:

- A scan of `no.novari.kafka` only served fint-kafka. Remove it.
- A scan of `no.novari` may also pick up other internal libraries in that package. Keep it if the app needs them.
  `scanBasePackages` on `@SpringBootApplication` skips auto-configurations, so it does not affect fint-kafka-starter.
  A separate `@ComponentScan` does not, so narrow it to the packages the app needs, or add
  `excludeFilters = [ComponentScan.Filter(type = FilterType.CUSTOM, classes = [AutoConfigurationExcludeFilter::class])]`.

## Properties

| fint-kafka 6                        | fint-kafka-starter                                         |
|-------------------------------------|------------------------------------------------------------|
| `spring.kafka.consumer.group-id`    | unchanged, and required when the app has listeners         |
| `spring.kafka.bootstrap-servers`    | unchanged                                                  |
| `spring.kafka.ssl.*`                | unchanged, from kafkarator                                 |
| `fint.kafka.enable-ssl`             | not read; SSL comes from `spring.kafka.security.protocol`  |
| `novari.kafka.application-id`       | unchanged; falls back to `spring.application.name`         |
| `novari.kafka.topic.org-id`         | unchanged                                                  |
| `novari.kafka.topic.domain-context` | unchanged                                                  |
| `novari.kafka.default-replicas`     | `novari.kafka.topic.default-replicas`                      |
| `fint.kafka.health.*`               | removed, see [Kafka health is gone](#kafka-health-is-gone) |
| other `spring.kafka.*`              | ignored by fint-kafka 6, **used now**                      |

fint-kafka 6 only read `bootstrap-servers`, `consumer.group-id` and `ssl.*` from `spring.kafka.*`. Check the app's
config and Kustomize overlays for other `spring.kafka.*` keys: they take effect now.

New: `novari.kafka.consumer.group-id-check` (`WARN`, `FAIL` or `OFF`), see [Group ids](#group-ids).

## API translation

| fint-kafka 6                                                                                                  | fint-kafka-starter                                                                     |
|---------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `ParameterizedTemplateFactory.createTemplate(X.class)`                                                        | inject `KafkaTemplate<String, X>`                                                      |
| `TemplateFactory.createTemplate(X.class)`                                                                     | inject `KafkaTemplate<String, X>`                                                      |
| `ParameterizedTemplate.send(ParameterizedProducerRecord)`                                                     | `kafkaTemplate.send(topicName, key, value)`                                            |
| `ParameterizedListenerContainerFactoryService.createRecordListenerContainerFactory(...).createContainer(...)` | `@KafkaListener(topics = ["#{@xTopic.name}"])`                                         |
| `ListenerContainerFactoryService.createRecordListenerContainerFactory(...).createContainer("topic")`          | `@KafkaListener(topics = ["topic"])`                                                   |
| `createBatchListenerContainerFactory(...)`                                                                    | `@KafkaListener(..., batch = "true")`                                                  |
| `createContainer(topicNamePatternParameters)`                                                                 | `@KafkaListener(topicPattern = "#{@xPattern.regex}")`                                  |
| `ListenerConfiguration.groupIdApplicationDefault()`                                                           | nothing                                                                                |
| `groupIdApplicationDefaultWithSuffix("-x")`                                                                   | `groupId = "#{@kafkaGroupIds.withSuffix('-x')}"`                                       |
| `groupIdApplicationDefaultWithUniqueSuffix()`                                                                 | `groupId = "#{@kafkaGroupIds.unique()}"`                                               |
| `maxPollRecords(n)`                                                                                           | `properties = ["max.poll.records=n"]`                                                  |
| `maxPollInterval(d)`                                                                                          | `properties = ["max.poll.interval.ms=<ms>"]`                                           |
| `maxPollRecordsKafkaDefault()`, `maxPollIntervalKafkaDefault()`                                               | nothing                                                                                |
| `continueFromPreviousOffsetOnAssignment()`                                                                    | nothing                                                                                |
| `seekToBeginningOnAssignment()`                                                                               | [`AbstractConsumerSeekAware`](#seek-to-the-beginning)                                  |
| `onAssignment(...)`, `onRevocation(...)`                                                                      | override `onPartitionsAssigned` / `onPartitionsRevoked` in `AbstractConsumerSeekAware` |
| `OffsetSeekingTrigger.seekToBeginning()`                                                                      | `AbstractConsumerSeekAware.seekToBeginning()`                                          |
| container customizer, e.g. `setConcurrency(n)`                                                                | `@KafkaListener(concurrency = "n")`                                                    |
| `ErrorHandlerFactory.createErrorHandler(ErrorHandlerConfiguration)`                                           | a `CommonErrorHandler` bean, see [Error handling](#error-handling)                     |
| `EntityTopicService.createOrModifyTopic(params, config)`                                                      | `@Bean` returning `topics.newTopic(topics.entity("x"), EntityTopicConfig(...))`        |
| `EventTopicService.createOrModifyTopic(params, config)`                                                       | `@Bean` returning `topics.newTopic(topics.event("x"), EventTopicConfig(...))`          |
| `ErrorEventTopicService.createOrModifyTopic(params, config)`                                                  | `@Bean` returning `topics.newTopic(topics.errorEvent("x"), EventTopicConfig(...))`     |
| `TopicService.createOrModifyTopic(name, TopicConfiguration)`                                                  | `@Bean` returning a `NewTopic` from Spring's `TopicBuilder`                            |
| `EntityTopicNameParameters` with `resourceName("x")`                                                          | `topics.entity("x")`                                                                   |
| `EventTopicNameParameters` with `eventName("x")`                                                              | `topics.event("x")`                                                                    |
| `ErrorEventTopicNameParameters` with `errorEventName("x")`                                                    | `topics.errorEvent("x")`                                                               |
| `orgIdApplicationDefault()`, `domainContextApplicationDefault()`                                              | nothing                                                                                |
| `orgId("y")`, `domainContext("z")`                                                                            | `topics.entity("x", orgId = "y", domainContext = "z")`                                 |
| `*TopicNamePatternParameters`                                                                                 | `topics.entityPattern(...)`, `eventPattern(...)`, `errorEventPattern(...)`             |
| `TopicNamePatternParameterPattern.any()`                                                                      | `NamePattern.any()`                                                                    |
| `anyOf(...)`, `exactly(x)`                                                                                    | `NamePattern.anyOf(...)`, `NamePattern.anyOf(x)`                                       |
| `startingWith`, `endingWith`, `custom`, `orgIdCommon()`                                                       | not included; write the regex in `topicPattern` yourself                               |
| request/reply                                                                                                 | not included                                                                           |
| `fint.kafka.health.*`, Kafka health indicators                                                                | not included                                                                           |
| testFixtures (`ConsumerTrackingService`)                                                                      | not included; use `spring-boot-starter-kafka-test` and `@EmbeddedKafka`                |

`KafkaTopics`, `EntityTopicName`, `EventTopicName`, `EntityTopicConfig` and `EventTopicConfig` are in
`no.novari.kafka.starter.topic`, `KafkaGroupIds` in `no.novari.kafka.starter.consumer` and the `send` extension in
`no.novari.kafka.starter.producer`.

## Topics

### Names

Topic names are unchanged: `<org-id>.<domain-context>.<entity|event>[.error].<name>`. Declare each name as a bean, so
listeners can use it in SpEL and producers can inject it:

```kotlin
@Configuration
class TopicConfiguration {
    @Bean
    fun userTopic(topics: KafkaTopics) = topics.entity("user")

    @Bean
    fun userTopicCreation(topics: KafkaTopics, userTopic: EntityTopicName) =
        topics.newTopic(userTopic, EntityTopicConfig(nullValueRetention = Duration.ofDays(1)))

    @Bean
    fun userCreatedTopic(topics: KafkaTopics) = topics.event("user-created")

    @Bean
    fun userCreatedTopicCreation(topics: KafkaTopics, userCreatedTopic: EventTopicName) =
        topics.newTopic(userCreatedTopic, EventTopicConfig(retention = Duration.ofDays(7)))
}
```

### Creation

fint-kafka 6 apps called `createOrModifyTopic(...)`, often in a constructor. Now a `NewTopic` bean is enough: Spring
Boot's `KafkaAdmin` creates missing topics and updates the config of existing ones at startup, before the listeners
start. To create topics at runtime, for example per org, call `kafkaAdmin.createOrModifyTopics(topics.newTopic(...))`.

Partitions can only be increased. A `NewTopic` with fewer partitions than the topic has does not reduce them.

### Config

| fint-kafka 6                             | fint-kafka-starter                          | Default when left out |
|------------------------------------------|---------------------------------------------|-----------------------|
| `EventTopicConfiguration.partitions(n)`  | `EventTopicConfig(partitions = n)`          | 1                     |
| `.retentionTime(d)`                      | `EventTopicConfig(retention = d)`           | required              |
| `.cleanupFrequency(f)`                   | `EventTopicConfig(cleanupFrequency = f)`    | `NORMAL`              |
| `EntityTopicConfiguration.partitions(n)` | `EntityTopicConfig(partitions = n)`         | 1                     |
| `.lastValueRetainedForever()`            | leave out `lastValueRetention`              | `null` = forever      |
| `.lastValueRetentionTime(d)`             | `EntityTopicConfig(lastValueRetention = d)` | **`null` = forever**  |
| `.nullValueRetentionTime(d)`             | `EntityTopicConfig(nullValueRetention = d)` | required              |
| `.cleanupFrequency(f)`                   | `EntityTopicConfig(cleanupFrequency = f)`   | `NORMAL`              |

The resulting topic config is the same as in fint-kafka 6, except that `cleanup.policy` is `delete,compact` without
the space. See [Topic config](fint-kafka-starter-vs-spring-kafka.md#topic-config) for the exact values.

> [!WARNING]
> **`lastValueRetention` has a default, `lastValueRetentionTime(...)` had none.** If you leave out
> `lastValueRetention` for a topic that had `lastValueRetentionTime(...)`, `KafkaAdmin` changes its `cleanup.policy`
> from `delete,compact` to `compact` at the next startup. Old records are then never deleted. `retention.ms` stays on
> the topic, but `compact` alone does not use it, so the change is easy to miss.

Checklist for every app:

- [ ] Search the fint-kafka 6 code for `lastValueRetentionTime(`.
- [ ] For each hit, set `lastValueRetention` to the same duration in the new `EntityTopicConfig`.
- [ ] For each `lastValueRetainedForever()`, leave out `lastValueRetention`.
- [ ] Check the remaining `EntityTopicConfig(...)` calls: no `lastValueRetention` must mean forever.
- [ ] After the deploy to staging, open each entity topic in the Aiven console and check that `cleanup.policy` and
      `retention.ms` are as before.

## Producers

```kotlin
@Service
class UserPublisher(
    private val kafkaTemplate: KafkaTemplate<String, UserDto>,
    private val userCreatedTopic: EventTopicName,
) {
    fun publish(id: String, user: UserDto) {
        kafkaTemplate.send(userCreatedTopic, id, user)
    }
}
```

- `send(TopicName, key, value)` is an extension in `no.novari.kafka.starter.producer`. A `null` value is a tombstone.
  `kafkaTemplate.send(topic.name, key, value)` does the same.
- All templates share one producer. fint-kafka 6 made one per template.
- Values are JSON, written with the app's Jackson 3 `JsonMapper`. Move Jackson 2 `ObjectMapper` customization to
  Jackson 3, or the JSON on the topic changes.
- Every record still gets the header `origin.application.id`.
- `send` is asynchronous, as before. Failed sends are logged by Spring's `LoggingProducerListener`.

## Consumers

Replace each container bean with a `@KafkaListener` method on a bean:

```kotlin
@Component
class UserListener(
    private val users: UserService,
) {
    @KafkaListener(topics = ["#{@userTopic.name}"])
    fun on(
        @Payload(required = false) user: UserDto?,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
    ) {
        if (user == null) users.delete(key) else users.save(key, user)
    }
}
```

- **Take the value as a parameter, not `ConsumerRecord<String, UserDto>`.** The message converter turns the JSON into
  the parameter type. With a `ConsumerRecord` parameter the value is the raw JSON `String`, and the app fails with
  `ClassCastException` when it reads it.
- Use `@Payload(required = false)` and a nullable type on entity topics, where `null` is a tombstone. Leave both out on
  event topics.
- `@Header(KafkaHeaders.RECEIVED_TOPIC)`, `RECEIVED_PARTITION`, `OFFSET` and `@Headers` give the rest of the record.
- A listener starts with the app and stops when it shuts down. Nothing calls `start()`.

### Batch

```kotlin
@KafkaListener(topics = ["#{@userTopic.name}"], batch = "true")
fun on(
    users: List<UserDto>,
    @Header(KafkaHeaders.RECEIVED_KEY) keys: List<String>,
)
```

Throw `BatchListenerFailedException(message, index)` to commit the records before `index` and retry from it, as in
fint-kafka 6.

### Patterns

```kotlin
@Bean
fun userEventsPattern(topics: KafkaTopics) =
    topics.eventPattern(NamePattern.anyOf("user-created", "user-deleted"), orgId = NamePattern.any())

@KafkaListener(topicPattern = "#{@userEventsPattern.regex}")
fun on(event: UserEvent) { }
```

Left-out `orgId` and `domainContext` match the defaults from `novari.kafka.topic.*`.

### Poll settings and concurrency

```kotlin
@KafkaListener(
    topics = ["#{@userTopic.name}"],
    concurrency = "3",
    properties = ["max.poll.records=100", "max.poll.interval.ms=600000"],
)
```

For all listeners: `spring.kafka.consumer.max-poll-records`, `spring.kafka.consumer.properties.[max.poll.interval.ms]`
and `spring.kafka.listener.concurrency`.

## Group ids

**The group ids must not change.** A new group id is a new consumer group: with `auto.offset.reset=earliest` it
reprocesses the whole topic, with `latest` it skips records.

| fint-kafka 6                                  | fint-kafka-starter                               | Group id                         |
|-----------------------------------------------|--------------------------------------------------|----------------------------------|
| `groupIdApplicationDefault()`                 | `@KafkaListener` without `groupId` and `id`      | `spring.kafka.consumer.group-id` |
| `groupIdApplicationDefaultWithSuffix("-x")`   | `groupId = "#{@kafkaGroupIds.withSuffix('-x')}"` | base + `-x`, no separator added  |
| `groupIdApplicationDefaultWithUniqueSuffix()` | `groupId = "#{@kafkaGroupIds.unique()}"`         | base + random UUID               |

- Keep `spring.kafka.consumer.group-id` and its Kustomize overrides as they are.
- Copy suffixes exactly. fint-kafka 6 added no separator, so `withSuffix("cache")` and `withSuffix("-cache")` are
  different groups.
- The app fails at startup if it has listeners and `spring.kafka.consumer.group-id` is not set. fint-kafka 6 used the
  string `"null"` as base.

### The `@KafkaListener(id)` pitfall

`@KafkaListener(id = "user-listener")` sets the group id to `user-listener`, because `idIsGroup` is `true` by default.
The app silently gets a new consumer group. Leave out `id`, or set `idIsGroup = false` when you need the id, for
example to look the container up in `KafkaListenerEndpointRegistry`:

```kotlin
@KafkaListener(id = "user-listener", idIsGroup = false, topics = ["#{@userTopic.name}"])
```

### Startup log and check

At startup the starter logs every listener container and its group id:

```
Kafka listener org.springframework.kafka.KafkaListenerEndpointContainer#0 uses group.id afk-no-fint-kontroll-user-catalog
```

A group id that does not start with `spring.kafka.consumer.group-id` gives a warning. Set
`novari.kafka.consumer.group-id-check: FAIL` while migrating, so such a listener stops the app. The check catches
`id` and hard-coded `groupId`, but not a changed suffix. Only the comparison below catches that.

### Check in staging

Use the Aiven console. Select a topic the app consumes, and open its consumer groups. Aiven shows partition, group,
offset and lag.

1. Before the deploy, note the app's groups and their offsets on each topic the app consumes.
2. Deploy, and compare the `uses group.id` lines in the log with the noted groups. Every group id must be among them,
   except `unique()` groups.
3. Check the topics again. There must be no new groups for the app, except `unique()` groups.
4. The offsets of the app's groups continue from before, without going back to 0, and the lag goes down.

The runbook for consumer groups ([FINT-113](https://novari-iks.atlassian.net/browse/FINT-113)) shows the same check
with `kafka-consumer-groups.sh`.

## Error handling

The starter has no error handling API. It uses Spring's `DefaultErrorHandler`, which tries a record 10 times without
pause, then logs and skips it. A `CommonErrorHandler` bean replaces it for all listeners on Boot's container factory.

### `noRetries().skipFailedRecords()`

Most fint-kafka 6 apps use this. Same behaviour, one attempt and then log and skip:

```kotlin
@Bean
fun kafkaErrorHandler(): CommonErrorHandler = DefaultErrorHandler(FixedBackOff(0, 0))
```

### Other choices

| `ErrorHandlerConfiguration`                        | `DefaultErrorHandler`                                                                     |
|----------------------------------------------------|-------------------------------------------------------------------------------------------|
| `noRetries()`                                      | `FixedBackOff(0, 0)`                                                                      |
| `retryWithFixedInterval(interval, maxRetries)`     | `FixedBackOff(interval.toMillis(), maxRetries)`                                           |
| `retryWithExponentialInterval(...)`                | `ExponentialBackOffWithMaxRetries(maxRetries)` with its setters                           |
| `retryWithBackoffFunction(...)`                    | `setBackOffFunction { record, exception -> ... }`                                         |
| `excludeExceptionsFromRetry(...)`                  | `addNotRetryableExceptions(...)`                                                          |
| `retryOnly(...)`                                   | `defaultFalse()`, then `addRetryableExceptions(...)`                                      |
| `useDefaultRetryClassification()`                  | nothing                                                                                   |
| `restartRetryOnExceptionChange()`                  | nothing (Spring's default)                                                                |
| `continueRetryOnExceptionChange()`                 | `setResetStateOnExceptionChange(false)`                                                   |
| `skipFailedRecords()`                              | nothing (Spring's default recoverer logs and skips)                                       |
| `recoverFailedRecords((record, consumer, e) -> …)` | `DefaultErrorHandler(ConsumerAwareRecordRecoverer { record, consumer, e -> … }, backOff)` |
| `reprocessAndRetryRecordOnRecoveryFailure()`       | nothing (Spring's default)                                                                |
| `reprocessRecordOnRecoveryFailure()`               | `setResetStateOnRecoveryFailure(false)`                                                   |
| `skipRecordOnRecoveryFailure()`                    | catch and log in the recoverer                                                            |

Conversion errors, such as invalid JSON, are never retried.

### Different handling per listener

Set the error handler in a `ContainerPostProcessor` bean and name it on the listener:

```kotlin
@Bean
fun retryFiveTimes() =
    ContainerPostProcessor<String, String, AbstractMessageListenerContainer<String, String>> {
        it.commonErrorHandler = DefaultErrorHandler(FixedBackOff(1000, 5))
    }

@KafkaListener(topics = ["#{@userTopic.name}"], containerPostProcessor = "retryFiveTimes")
```

## Seek to the beginning

Replaces `seekToBeginningOnAssignment()`, typically for listeners that fill a cache from an entity topic. Extend Spring's
`AbstractConsumerSeekAware` in the listener class:

```kotlin
@Component
class UserCacheListener(
    private val cache: UserCache,
) : AbstractConsumerSeekAware() {
    @KafkaListener(topics = ["#{@userTopic.name}"])
    fun on(
        @Payload(required = false) user: UserDto?,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
    ) {
        if (user == null) cache.remove(key) else cache.put(key, user)
    }

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback,
    ) {
        super.onPartitionsAssigned(assignments, callback)
        callback.seekToBeginning(assignments.keys)
    }
}
```

- The seek applies to every `@KafkaListener` in the class. Keep listeners that should not seek in another class.
- Keep the group id from fint-kafka 6, also here.
- Replaces `OffsetSeekingTrigger`: inject the listener and call `seekToBeginning()`, for example from a REST endpoint.
  This needs the `super.onPartitionsAssigned(...)` call above.
- For other offsets, use `callback.seek(topic, partition, offset)` or `seekToTimestamp(...)`.

## Several or dynamic containers

Most apps need only `@KafkaListener`. When the topics are not known at startup, or the app needs a container object,
create the container from Boot's factory. It gets the same config and error handler as `@KafkaListener`:

```kotlin
@Bean
fun userContainer(
    factory: ConcurrentKafkaListenerContainerFactory<*, *>,
    userTopic: EntityTopicName,
    jsonMapper: JsonMapper,
    users: UserService,
) = factory.createContainer(userTopic.name).apply {
    containerProperties.setMessageListener(
        MessageListener<String, String> { record ->
            // record.value() is null for a tombstone
            users.save(record.key(), record.value()?.let { jsonMapper.readValue(it, UserDto::class.java) })
        },
    )
}
```

- The value is the JSON `String`. Convert it with the app's `JsonMapper`, as above.
- The group id is `spring.kafka.consumer.group-id`. For a suffix, set
  `containerProperties.setGroupId(kafkaGroupIds.withSuffix("-x"))`.
- A container returned from a `@Bean` is started and stopped with the app, and is included in the group id log.
- A container made at runtime, outside a `@Bean`, must be started and stopped by the app, and is not in the group id
  log. `factory.createContainer(pattern)` takes a `java.util.regex.Pattern`, for example `Pattern.compile(topicPattern.regex)`.

## Kafka health is gone

fint-kafka 6 had the health indicators `kafkaConnectivity` and `kafkaConsumers`. fint-kafka-starter has none, as plain
Spring Boot. Kafka 4 clients reconnect through `bootstrap.servers` when all known brokers are gone.

- Probes on `/actuator/health/liveness` and `/readiness`: no change. The fint-kafka 6 groups pointed at the wrong
  indicator names, so they never included Kafka.
- Probes on `/actuator/health` (root): the Kafka check is gone.
- Remove `kafkaConnectivity` and `kafkaConsumers` from `management.endpoint.health.group.*.include`. Otherwise the
  app fails at startup, because Spring Boot checks that group members exist.
- Remove `fint.kafka.health.*`.

## SSL and kafkarator

kafkarator puts the Kafka credentials in the secret `<app>-kafka`. fint-kafka 6 turned SSL on with
`fint.kafka.enable-ssl`. fint-kafka-starter does not read that key and uses Spring Boot's
`spring.kafka.security.protocol` and `spring.kafka.ssl.*`.

kafkarator sets `spring.kafka.security.protocol=SSL` from [FINT-112](https://novari-iks.atlassian.net/browse/FINT-112)
([kafkarator#16](https://github.com/FINTLabs/kafkarator/pull/16)). Check that the app's secret has it:

```shell
kubectl get secret <app>-kafka -n <namespace> -o jsonpath='{.data.spring\.kafka\.security\.protocol}' | base64 -d
```

It must print `SSL`. Without it the app connects with `PLAINTEXT` and fails against Aiven. Until kafkarator is
updated, set `spring.kafka.security.protocol: SSL` in the app.

## Wrapper libraries

Some libraries wrap fint-kafka, for example `fint-flyt-kafka`, `fint-flyt-gateway-starter` and
`fint-flyt-(web-)resource-server`. An app cannot mix fint-kafka 6 and fint-kafka-starter, so the wrapper is migrated
first.

For a wrapper library:

- Depend on `no.novari:kafka-starter` with `api`, so apps get it through the wrapper.
- Build on Boot's `KafkaTemplate`, container factory and `KafkaAdmin`. Do not create producer or consumer factories.
- Leave the `CommonErrorHandler` bean to the app, or declare it with `@ConditionalOnMissingBean`.
- Copies of fint-kafka 6 code, such as `InstanceFlowErrorHandlerConfigurationStepBuilder` in `fint-flyt-kafka`, are
  deleted.
- The starter's public API is checked for binary compatibility, so a wrapper built against an older minor version
  works with a newer one.

## Request/reply is not included

fint-kafka 6's request/reply (`RequestTemplateFactory`, `RequestListenerContainerFactory`, request and reply topics) is
not in fint-kafka-starter. Only flyt used it, and flyt replaces it with HTTP in
[FFS-2389](https://novari-iks.atlassian.net/browse/FFS-2389). Apps that use request/reply move to HTTP before they
migrate.

## Java apps

The starter is written in Kotlin, but works from Java. Parameters with defaults must be passed explicitly:

```java
@Bean
EntityTopicName userTopic(KafkaTopics topics) {
    return topics.entity("user", null, null);
}

@Bean
NewTopic userTopicCreation(KafkaTopics topics, EntityTopicName userTopic) {
    // nullValueRetention, lastValueRetention, cleanupFrequency, partitions
    return topics.newTopic(userTopic, new EntityTopicConfig(Duration.ofDays(1), null, EntityCleanupFrequency.NORMAL, 1));
}

@KafkaListener(topics = "#{@userTopic.name}", groupId = "#{@kafkaGroupIds.withSuffix('-cache')}")
void on(@Payload(required = false) UserDto user, @Header(KafkaHeaders.RECEIVED_KEY) String key) { }

kafkaTemplate.send(userTopic.getName(), key, user);
```

## Checklist

- [ ] Spring Boot 4.1+, Jackson 3, Java 21+.
- [ ] Wrapper libraries migrated, no request/reply.
- [ ] `no.novari:kafka` replaced by `no.novari:kafka-starter`; component scans that only served fint-kafka removed.
- [ ] Properties updated, and other `spring.kafka.*` keys checked.
- [ ] Topic names declared with `KafkaTopics`, topic creation as `NewTopic` beans.
- [ ] The `lastValueRetention` checklist under [Config](#config) is done.
- [ ] Producers use `KafkaTemplate`.
- [ ] Consumers use `@KafkaListener` with the value as parameter, no `id` without `idIsGroup = false`.
- [ ] Group ids and suffixes as in fint-kafka 6; `novari.kafka.consumer.group-id-check: FAIL` while migrating.
- [ ] `CommonErrorHandler` bean for `noRetries().skipFailedRecords()`.
- [ ] Seek to the beginning with `AbstractConsumerSeekAware`.
- [ ] Kafka indicators removed from health groups.
- [ ] `spring.kafka.security.protocol=SSL` in the kafkarator secret.
- [ ] Consumer groups and offsets checked in staging.
