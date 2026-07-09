# Forkless PoC modules

These modules demonstrate that a provider can plug private implementations behind Apache Polaris
seams with no long-lived source fork. Contracts and data carriers are framework-agnostic and live in
`polaris-core`; the implementations here are peers behind those contracts.

- `default/` (`:polaris-forkless-default`) — OSS-default implementations, sited in `extensions/` as
  peers, framework-agnostic plain Java.
- `provider/` (`:polaris-forkless-provider`) — provider-analog ("Snowflake-private") implementations.
  Its build depends on **only** `polaris-core` plus Apache Iceberg, with no framework dependency at
  all. If it compiles, it provably references no framework type: that is the forkless proof.
- `wiring/` (`:polaris-forkless-wiring`) — stands in for the runtime/app layer. The CDI annotations
  that select among implementations live here, never on an implementation.

Demo seams remaining here: durable layer. (Entity resolution, authorization decision, error model,
operation metadata, and storage IO have all since been migrated to real production wiring in
`runtime/service` and no longer live as demo scaffolding in this module.) See
`docs/forkless-poc-implementation-report.md` (in the forkless-effort workspace) for the full
rationale, the credential-vending call chain, the dependability tiers, and the migration list.

Build and test, with Java 21:

```
JAVA_HOME=/path/to/jdk-21 ./gradlew \
  :polaris-forkless-default:test :polaris-forkless-provider:test :polaris-forkless-wiring:compileJava
```
