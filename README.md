# kafka-streams-multipart-demo

A worked example of [`kafka-streams-multipart-test-utils`](https://github.com/SouquieresAdam/kafka-streams-multipart-test-utils),
showing the partitioning bugs that the stock `TopologyTestDriver` lets through and that
the multi-partition driver catches at test time.

## The problem this solves

Kafka Streams' standard `TopologyTestDriver` runs every record through a single fake task
with partition `0`. Tests pass with that fiction in place. Production then deploys the
same topology against topics with `N>1` partitions, and three classes of bug surface for
the first time:

1. **Routing fidelity** — a missing `selectKey`, a changed key serializer, a custom
   partitioner regression. Stock `TestRecord` has no partition field, so the test cannot
   assert routing. The bug appears in production.
2. **Silent state split** — two sources merged onto a shared store with mismatched
   partition counts collapse into one task under stock TTD, giving a misleading green.
   In production with `N>1`, the same key from each source side hashes to <i>different</i>
   store partitions and the per-key state silently splits.
3. **Partition count drift** — `Repartitioned.withNumberOfPartitions(N)` is invisible to
   stock TTD. A change from `4` to `8` (or removal) drifts undetected until the migration.

`MultiPartitionTopologyTestDriver` (KIP-1238 preview) closes that gap. It materialises
one `StreamTask` per `(subtopologyId, partition)`, routes by the production
`BuiltInPartitioner`, and exposes per-partition state-store accessors and
`partitionsOf(...)`. Multi-partition issues fail your CI build, not your prod migration.

## How the tests prove it

`PartitioningRiskExposureTest` runs the *same* topology under both drivers and contrasts
their behaviour. Each test names the gap it closes:

| Test | Gap surfaced |
|---|---|
| `outputPartitionRoutingIsObservableOnlyWithMultiPartitionDriver` | Stock `TestRecord` exposes no partition. MP-TTD's `MultiPartitionTestRecord.partitionOf(rec)` reports the production `murmur2` hash and lets the test assert routing per key. |
| `coPartitionMismatchIsSilentUnderStockDriverButCaughtByMultiPartitionDriver` | A buggy 2-vs-3-partition merge that aggregates a shared store. Stock TTD reports `count=2` in one bucket and passes. MP-TTD shows the increment from each side living on a different store partition — the production state split is now an assertion failure. |
| `storePartitionCountIsAssertableOnlyWithMultiPartitionDriver` | `partitionsOf("tenant-sku-totals") == 4` pins the `Repartitioned.withNumberOfPartitions(4)` contract at unit-test time. Drift fails CI, not the migration. |

`EcommerceTopologyTest` then exercises the multi-partition driver against a realistic
3-sub-topology stateful application, demonstrating that each capability above scales to a
real flow:

| Test | What it proves |
|---|---|
| `cartActionsAreCountedPerUserAcrossPartitions` | DSL `groupByKey().count()` materialises a partitioned store; per-user state lives in exactly one of 4 task instances and never leaks. Asserting that requires per-partition store access — which only MP-TTD provides. |
| `tenantSkuTotalsAggregateAfterRepartition` | `selectKey + Repartitioned.withNumberOfPartitions(4)` pins the downstream sub-topology and the aggregate store at 4 partitions. Same logical key from different source partitions converges to one store partition — the very contract you cannot test single-partition. |
| `inventoryProcessorMaintainsStockPerSku` | A custom PAPI `Processor` with a partitioned store ends up with one store instance per `(sub-topology, partition)`. Each instance only contains the skus that hash to its partition. |
| `inventoryProcessorEmitsLowStockAlertsBelowThreshold` | The processor's conditional `forward` reaches the sink, and emitted records preserve the source-side partition (no repartition between processor and sink). Strict less-than threshold semantics are pinned. |
| `multiSubTopologyIndependenceAndPartitionCounts` | Three sub-topologies coexist at heterogeneous partition counts (4, 4, 3). Drift on any of these counts fails CI. |
| `allThreeFlowsCoexistUnderASingleDriver` | A single driver instance hosts a heterogeneous topology end-to-end: a record on each input flows independently and lands in the right store and/or output. |

## Topology under test

```
                      cart-events (4 part.)
                              │
                              ▼
   ┌──────── Sub-topology A ───────────┐
   │  groupByKey().count()             │
   │      → cart-actions-per-user (4)  │
   └───────────────────────────────────┘

                      orders (3 part.)
                              │
                              ▼
   ┌──────── Sub-topology B (pre)  ────┐
   │  selectKey(tenant#sku)            │
   │      → repartition topic (4)      │
   └───────────────────────────────────┘
                              │
                              ▼
   ┌──────── Sub-topology B (post) ────┐
   │  groupByKey().aggregate(sum)      │
   │      → tenant-sku-totals (4)      │
   └───────────────────────────────────┘

                  inventory-updates (3 part.)
                              │
                              ▼
   ┌──────── Sub-topology C ───────────┐
   │  .process(InventoryProcessor)     │
   │      → inventory-store (3)        │
   │      → low-stock-alerts (3)       │
   └───────────────────────────────────┘
```

The PAPI processor (`InventoryProcessor`) keeps a per-sku stock counter in a partitioned
key-value store and emits a record on `low-stock-alerts` whenever the post-update stock
falls strictly below a configurable threshold.

## Build & run

The demo depends on `consulting.stream:kafka-streams-multipart-test-utils:3.9.2-0.1.0`,
resolved from Maven Central:

```sh
./gradlew test
```

Expected output: `9 tests, 0 failures` (6 functional + 3 risk-exposure).
Requires JDK 25 (toolchain).

## Project layout

```
src/
  main/java/consulting/stream/demo/
    processor/InventoryProcessor.java       # custom PAPI processor with partitioned store
    topology/EcommerceTopology.java         # 3-sub-topology factory (DSL + .process)
  test/java/consulting/stream/demo/
    EcommerceTopologyTest.java              # 6 functional tests
    PartitioningRiskExposureTest.java       # 3 paired contrast tests (stock TTD vs MP-TTD)
```

## Picking a different Kafka version

Edit `gradle.properties`:

```properties
kafkaVersion=3.9.2
multipartVersion=3.9.2-0.1.0
```

`multipartVersion` follows the `<kafkaCompatVersion>-<libSemver>` scheme: the prefix is the
Kafka API surface the jar was built against, the suffix is the library's own semver. Both
must align with a published artefact on Maven Central. See the
[compatibility matrix](https://github.com/SouquieresAdam/kafka-streams-multipart-test-utils#compatibility-matrix)
for which Kafka versions are supported.

## License

Apache License, Version 2.0.
