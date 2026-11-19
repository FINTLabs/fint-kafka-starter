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

## Release

Publishing a GitHub release publishes the artifact to `https://repo.fintlabs.no/releases`. The version is the tag without
a leading `v`, so `v1.2.0` becomes `1.2.0`.

To publish a release candidate, create a pre-release from `main` with a tag such as `v1.2.0-rc-0`:

```shell
gh release create v1.2.0-rc-0 --prerelease --target main --generate-notes
```

Follow the run under **Actions → Publish to Reposilite**.
