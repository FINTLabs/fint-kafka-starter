# fint-kafka-starter

Spring Boot starter with Novari conventions for Kafka: topic names, consumer group ids, producer defaults and health.
It is a thin layer on top of Spring Kafka and replaces [fint-kafka](https://github.com/FINTLabs/fint-kafka).

**Status:** under development. Not ready for use yet.

- Artifact: `no.novari:kafka-starter`
- Requires Spring Boot 4.1+ and Java 21+

## Build

```shell
./gradlew check
```

The public API is locked with an ABI dump in `api/`. After an intended API change, run `./gradlew apiDump` and commit the result.
