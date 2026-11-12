# Architecture

C4 level 3 for fint-kafka-starter. The starter is a library, so it has no system context or containers of its own: it
runs inside each Spring Boot app that depends on it. The diagram shows that app with the starter's components. Blue is
the starter, turquoise is the app, Spring Boot and Kafka.

## C4 level 3: Components

The starter does not send or receive records itself. It adjusts Spring Boot's Kafka beans, and gives the app topic names
and group ids to use with them.

```mermaid
C4Component
    title fint-kafka-starter in a Spring Boot app

    Component_Ext(config, "Configuration", "application.yaml, kafkarator secret", "spring.kafka.*, novari.kafka.*")
    Component(auto, "Auto-configuration", "Spring Boot", "Kafka defaults, JSON, origin header")
    ComponentQueue_Ext(kafka, "Kafka", "Aiven", "Topics and consumer groups")
    Component_Ext(boot, "Spring Boot Kafka", "Spring Kafka", "KafkaTemplate, listener containers, KafkaAdmin")
    Component_Ext(app, "App code", "Kotlin or Java", "@KafkaListener, KafkaTemplate, NewTopic beans")
    Component(reporter, "GroupIdReporter", "Startup", "Logs and checks group ids")
    Component(topics, "KafkaTopics", "Bean", "Topic names, patterns, NewTopic")
    Component(groupIds, "KafkaGroupIds", "Bean", "Group ids from the base")

    Rel(config, auto, "Reads")
    Rel(auto, boot, "Customizes")
    BiRel(boot, kafka, "Records, topics", "Kafka")
    Rel(app, boot, "Sends, listens")
    Rel(reporter, boot, "Reads containers")
    Rel(app, topics, "Names, NewTopic")
    Rel(app, groupIds, "Group ids", "SpEL")
    Rel(reporter, groupIds, "Base")

    UpdateRelStyle(config, auto, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="-35", $offsetY="-30")
    UpdateRelStyle(auto, boot, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="10")
    UpdateRelStyle(boot, kafka, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="-35", $offsetY="-30")
    UpdateRelStyle(app, boot, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="20")
    UpdateRelStyle(reporter, boot, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="10")
    UpdateRelStyle(app, topics, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="10")
    UpdateRelStyle(app, groupIds, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="20")
    UpdateRelStyle(reporter, groupIds, $textColor="#3B82F6", $lineColor="#3B82F6", $offsetX="10")
    UpdateElementStyle(auto, $bgColor="#93C5FD", $fontColor="#1F2937", $borderColor="#3B82F6")
    UpdateElementStyle(reporter, $bgColor="#93C5FD", $fontColor="#1F2937", $borderColor="#3B82F6")
    UpdateElementStyle(topics, $bgColor="#93C5FD", $fontColor="#1F2937", $borderColor="#3B82F6")
    UpdateElementStyle(groupIds, $bgColor="#93C5FD", $fontColor="#1F2937", $borderColor="#3B82F6")
    UpdateElementStyle(config, $bgColor="#A5F3FC", $fontColor="#1F2937", $borderColor="#06B6D4")
    UpdateElementStyle(kafka, $bgColor="#A5F3FC", $fontColor="#1F2937", $borderColor="#06B6D4")
    UpdateElementStyle(boot, $bgColor="#A5F3FC", $fontColor="#1F2937", $borderColor="#06B6D4")
    UpdateElementStyle(app, $bgColor="#A5F3FC", $fontColor="#1F2937", $borderColor="#06B6D4")
    UpdateLayoutConfig($c4ShapeInRow="2", $c4BoundaryInRow="1")
```

- **Auto-configuration** has two parts:
  - `NovariKafkaDefaultsEnvironmentPostProcessor` adds the `spring.kafka.*` defaults as the property source with the
    lowest precedence. It also sets `client.id` and the origin application id from `novari.kafka.application-id` and
    `novari.kafka.topic.org-id`. See [fint-kafka-starter vs Spring Kafka](fint-kafka-starter-vs-spring-kafka.md).
  - `NovariKafkaAutoConfiguration` runs after Boot's `KafkaAutoConfiguration`. It gives the JSON serializer the app's
    `JsonMapper`, registers the JSON message converter for `@KafkaListener`, and creates `KafkaTopics`, `KafkaGroupIds`
    and `GroupIdReporter`. Every bean the app may replace is `@ConditionalOnMissingBean`.
- **`OriginHeaderProducerInterceptor`** runs inside the Kafka producer, set through `interceptor.classes`, and adds the
  header `origin.application.id` to every record.
- **`KafkaTopics`** builds names from `novari.kafka.topic.org-id` and `domain-context`, and `NewTopic`s with the
  Novari topic config. Boot's `KafkaAdmin` creates them at startup.
- **`KafkaGroupIds`** builds group ids from `spring.kafka.consumer.group-id`, as fint-kafka 6 did. Listeners use it
  through SpEL: `#{@kafkaGroupIds.withSuffix('-cache')}`.
- **`GroupIdReporter`** logs the group id of every container in `KafkaListenerEndpointRegistry` at startup, and checks
  that it starts with `spring.kafka.consumer.group-id` (`novari.kafka.consumer.group-id-check`).

Not in the starter: own producers, consumers or containers, an error handling API, request/reply and Kafka health.
Those are Spring Kafka's, or left out on purpose.
