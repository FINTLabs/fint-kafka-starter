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
