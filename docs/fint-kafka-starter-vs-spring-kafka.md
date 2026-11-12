# fint-kafka-starter vs Spring Kafka

fint-kafka-starter uses Spring Boot's Kafka beans (`KafkaTemplate`, the default listener container factory and
`KafkaAdmin`), but changes some defaults. Most of them keep the behaviour of fint-kafka 6.

The `spring.kafka.*` defaults are added as the property source with the lowest precedence, named `novariKafkaDefaults`.
They show in `/actuator/env`, and any value the app sets wins.

## Defaults

| Setting                                     | fint-kafka-starter                                  | Spring Boot / Kafka                    |
|---------------------------------------------|-----------------------------------------------------|----------------------------------------|
| `auto.offset.reset`                         | `earliest`                                          | `latest`                               |
| `max.partition.fetch.bytes`                 | 3 MiB                                               | 1 MiB                                  |
| `max.request.size` (producer)               | 3 MiB                                               | 1 MiB                                  |
| Value serializer                            | `JacksonJsonSerializer` with the app's `JsonMapper` | `StringSerializer`                     |
| Value conversion in `@KafkaListener`        | JSON to the type of the listener parameter          | none, the value is a `String`          |
| Producer interceptor                        | `OriginHeaderProducerInterceptor`                   | none                                   |
| `client.id`                                 | `<application-id>-<org-id>`                         | `consumer-<group>-<n>`, `producer-<n>` |
| `spring.kafka.admin.modify-topic-configs`   | `true`                                              | `false`                                |
| `spring.kafka.listener.observation-enabled` | `true`                                              | `false`                                |
| `spring.kafka.template.observation-enabled` | `true`                                              | `false`                                |
| Topic replicas in `KafkaTopics.newTopic`    | 2                                                   | broker default                         |

Everything else is Spring Boot's and Kafka's default, for example `linger.ms` = 5, one shared producer, and
`DefaultErrorHandler` with 10 attempts without pause, then log and skip.

## Overriding a default

### `auto.offset.reset`

A new consumer group reads from the start of the topic. To read only new records:

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: latest
```

For one listener: `@KafkaListener(properties = ["auto.offset.reset=latest"])`.

### Message sizes

```yaml
spring:
  kafka:
    consumer:
      properties:
        "[max.partition.fetch.bytes]": 1048576
    producer:
      properties:
        "[max.request.size]": 1048576
```

`max.request.size` must not be larger than the topic's `max.message.bytes`.

### Value serializer and conversion

Values are written as JSON with the `__TypeId__` header. The app's `JsonMapper` bean is used, so Jackson config such as
`spring.jackson.*` and `JsonMapperBuilderCustomizer` applies to Kafka too.

- Another serializer: set `spring.kafka.producer.value-serializer`. The starter then leaves the serializer alone.
- Another converter for `@KafkaListener`: define a `RecordMessageConverter` bean.

The consumer's value deserializer is Boot's `StringDeserializer`. The message converter turns the JSON into the type of
the listener's payload parameter, and ignores `__TypeId__`. A listener that takes a `ConsumerRecord` gets the raw JSON
`String` as value.

### Origin header

Every record gets the header `origin.application.id` with the value of `novari.kafka.application-id`, added by
`OriginHeaderProducerInterceptor` through `spring.kafka.producer.properties[interceptor.classes]`. Setting that key
replaces the interceptor, so list it there too if the app adds its own interceptors.

### `client.id`

`<application-id>-<org-id>` from `novari.kafka.application-id` and `novari.kafka.topic.org-id`, or only
`<application-id>` without an org id. Override with `spring.kafka.client-id`.

### Topic config changes at startup

`spring.kafka.admin.modify-topic-configs: true` makes `KafkaAdmin` update the config of existing topics to match the
`NewTopic` beans at startup, as fint-kafka 6 did. Set it to `false` to only create missing topics.

### Observation

Listener and template observation is on, so Micrometer traces and metrics cover Kafka. Turn it off with
`spring.kafka.listener.observation-enabled: false` and `spring.kafka.template.observation-enabled: false`.

### Replicas

`novari.kafka.topic.default-replicas` sets the replication factor for `KafkaTopics.newTopic(...)`. Use 1 against a
single local broker.

## Topic config

`KafkaTopics.newTopic(...)` sets this config:

| Type               | `cleanup.policy`                                         | Other config                                                                                                               |
|--------------------|----------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| event, error-event | `delete`                                                 | `retention.ms` = `retention`, `segment.ms`                                                                                 |
| entity             | `compact`, or `delete,compact` with `lastValueRetention` | `retention.ms` = `lastValueRetention`, `delete.retention.ms` = `nullValueRetention`, `segment.ms`, `max.compaction.lag.ms` |

| Cleanup frequency | Event `segment.ms` | Entity `segment.ms` | Entity `max.compaction.lag.ms` |
|-------------------|--------------------|---------------------|--------------------------------|
| `FREQUENT`        | 6 h                | 3 h                 | 6 h                            |
| `NORMAL`          | 12 h               | 12 h                | 24 h                           |
| `RARE`            | 24 h               | 24 h                | 48 h                           |

Defaults in `EventTopicConfig` and `EntityTopicConfig`: `cleanupFrequency = NORMAL` and `partitions = 1`.
`lastValueRetention` defaults to `null`, which gives `compact` and keeps the last value per key forever.
`retention` and `nullValueRetention` have no default.

Need other config? Build the `NewTopic` with Spring's `TopicBuilder` and the name from `KafkaTopics`.
