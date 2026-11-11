# fint-kafka-starter

Spring Boot starter with Novari conventions for Kafka. It is a thin layer on top of Spring Kafka and replaces
[fint-kafka](https://github.com/FINTLabs/fint-kafka). Apps write standard `@KafkaListener`, `KafkaTemplate`, `NewTopic`
and `CommonErrorHandler`. On top of Spring Kafka, the starter gives:

- `KafkaTopics`: topic names on the form `<org-id>.<domain-context>.<entity|event>[.error].<name>`, patterns for them,
  and `NewTopic`s with the Novari topic config
- `KafkaGroupIds`: group ids built from `spring.kafka.consumer.group-id`, the same strings as fint-kafka 6
- a log line per listener with its group id at startup, and a check that it starts with `spring.kafka.consumer.group-id`
- JSON values with the app's Jackson `JsonMapper`, and the header `origin.application.id` on every record
- defaults that differ from Spring Kafka, see [fint-kafka-starter vs Spring Kafka](docs/fint-kafka-starter-vs-spring-kafka.md)

**Status:** under development. Not ready for use yet.

- Requires Spring Boot 4.1+, Jackson 3 and Java 21+
- Migrating from fint-kafka: see [Migrating from fint-kafka](docs/migrating-from-fint-kafka.md)

## Usage

```kotlin
repositories {
    maven("https://repo.fintlabs.no/releases")
}

dependencies {
    implementation("no.novari:kafka-starter:<version>")
}
```

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: my-app
novari:
  kafka:
    application-id: my-app
    topic:
      org-id: afk-no
      domain-context: flyt
```

```kotlin
@Configuration
class KafkaConfig {
    @Bean
    fun userReceivedTopic(topics: KafkaTopics) = topics.event("user-received")

    @Bean
    fun userProcessedTopic(topics: KafkaTopics) = topics.event("user-processed")

    // Created or updated by Spring Boot's KafkaAdmin at startup
    @Bean
    fun userProcessedTopicCreation(topics: KafkaTopics, userProcessedTopic: EventTopicName) =
        topics.newTopic(userProcessedTopic, EventTopicConfig(retention = Duration.ofDays(7)))
}

@Component
class UserReceivedListener(
    private val kafkaTemplate: KafkaTemplate<String, UserDto>,
    private val userProcessedTopic: EventTopicName,
) {
    @KafkaListener(topics = ["#{@userReceivedTopic.name}"])
    fun on(
        user: UserDto,
        @Header(KafkaHeaders.RECEIVED_KEY) key: String,
    ) {
        kafkaTemplate.send(userProcessedTopic, key, user) // no.novari.kafka.starter.producer.send
    }
}
```

The listener above uses `spring.kafka.consumer.group-id`. For a separate consumer group, use
`groupId = "#{@kafkaGroupIds.withSuffix('-cache')}"`. Do not set `id` on `@KafkaListener`: it becomes the group id.

| Property                               | Default                   | Purpose                                                                  |
|----------------------------------------|---------------------------|--------------------------------------------------------------------------|
| `novari.kafka.application-id`          | `spring.application.name` | Value of the `origin.application.id` header, and part of `client.id`     |
| `novari.kafka.topic.org-id`            |                           | Default org id in topic names, with hyphens (`afk-no`)                   |
| `novari.kafka.topic.domain-context`    |                           | Default domain context in topic names                                    |
| `novari.kafka.topic.default-replicas`  | `2`                       | Replication factor in `KafkaTopics.newTopic(...)`                        |
| `novari.kafka.consumer.group-id-check` | `WARN`                    | `WARN`, `FAIL` or `OFF` for a group id that does not start with the base |

## Docs

- [fint-kafka-starter vs Spring Kafka](docs/fint-kafka-starter-vs-spring-kafka.md): defaults that differ, and how to
  override them
- [Migrating from fint-kafka](docs/migrating-from-fint-kafka.md)

## Build

```shell
./gradlew check
```

## Public API

The public API is locked with an ABI (binary interface) dump in `api/kafka-starter.api`, made by
[binary-compatibility-validator](https://github.com/Kotlin/binary-compatibility-validator). Libraries such as
`fint-flyt-kafka` are compiled against one version of this starter, while the app may run with a newer one. A changed
signature then fails at runtime with `NoSuchMethodError`, not at compile time. Many such changes look safe in Kotlin
source, for example adding a parameter with a default value.

- `./gradlew check` fails when the compiled public API differs from the dump.
- After an intended API change, run `./gradlew apiDump` and commit the dump in the same pull request, so the change is
  visible in review.
- On a rebase conflict in the dump, do not merge it by hand. Run `./gradlew apiDump` again.
- From 1.0.0: a removed or changed line in the dump needs a major version. New lines need a minor version.
- `explicitApi()` is on, so public declarations need an explicit `public`. Keep everything apps do not use `internal`.

## Release

Publishing a GitHub release publishes the artifact to `https://repo.fintlabs.no/releases`. The version is the tag without
a leading `v`, so `v1.2.0` becomes `1.2.0`.

To publish a release candidate, create a pre-release from `main` with a tag such as `v1.2.0-rc-0`:

```shell
gh release create v1.2.0-rc-0 --prerelease --target main --generate-notes
```

Follow the run under **Actions → Publish to Reposilite**.
