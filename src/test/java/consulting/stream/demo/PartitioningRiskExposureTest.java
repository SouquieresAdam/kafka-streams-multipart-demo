package consulting.stream.demo;

import consulting.stream.demo.topology.EcommerceTopology;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.MultiPartitionTestInputTopic;
import org.apache.kafka.streams.MultiPartitionTestOutputTopic;
import org.apache.kafka.streams.MultiPartitionTopologyTestDriver;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.MultiPartitionTestRecord;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests in this class do not exist to celebrate the multi-partition driver — they exist to
 * pin the gap that the stock {@link TopologyTestDriver} leaves open.
 *
 * <p>Stock {@code TopologyTestDriver} runs every record through a single fake task with
 * partition {@code 0}. That hides three classes of partitioning bugs that only surface on
 * the production migration when the topic actually has {@code N>1} partitions:</p>
 *
 * <ol>
 *   <li><b>Routing fidelity:</b> "where would this record actually land?"
 *       Stock {@code TestRecord} has no partition field, so the test cannot assert routing.</li>
 *   <li><b>Silent state split:</b> two sources merged onto a shared store with mismatched
 *       partition counts collapse into one task under stock TTD — a passing test that
 *       fails the moment production deploys at {@code N>1}.</li>
 *   <li><b>Partition count drift:</b> stock TTD has no way to ask "how many partitions does
 *       this store run at?", so a topology change to {@code Repartitioned.withNumberOfPartitions(N)}
 *       drifts undetected until the migration.</li>
 * </ol>
 *
 * <p>Each test in this class runs the <i>same</i> topology under both drivers and shows that
 * stock TTD passes silently while {@link MultiPartitionTopologyTestDriver} surfaces the gap as
 * an assertable, CI-failable signal.</p>
 */
public class PartitioningRiskExposureTest {

    private static final StringSerializer STRING_SER = new StringSerializer();
    private static final StringDeserializer STRING_DES = new StringDeserializer();

    private static Properties baseProps() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "risk-exposure");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    /**
     * Risk #1 — Output routing.
     *
     * <p>Topology: identity passthrough on a 4-partition input/output pair.</p>
     *
     * <p>Under stock {@code TopologyTestDriver}, {@link TestRecord} carries no partition,
     * so even if the developer wanted to assert routing, the API offers no surface to do
     * so. Bugs like "I forgot to {@code selectKey} before publishing" or "I changed the
     * key serializer and now keys re-hash differently" cannot fail this test.</p>
     *
     * <p>Under {@code MultiPartitionTopologyTestDriver}, {@link MultiPartitionTestRecord#partitionOf}
     * exposes the actual partition, and the driver routes by the production
     * {@link BuiltInPartitioner}. Routing bugs surface as assertion failures at CI.</p>
     */
    @Test
    public void outputPartitionRoutingIsObservableOnlyWithMultiPartitionDriver() {
        final String in = "in";
        final String out = "out";
        final int n = 4;

        final StreamsBuilder b = new StreamsBuilder();
        b.stream(in, Consumed.with(Serdes.String(), Serdes.String()))
            .to(out, Produced.with(Serdes.String(), Serdes.String()));
        final Topology topology = b.build();

        final String[] keys = {"alice", "bob", "carol", "dave", "key-42"};

        // ---------- Stock TopologyTestDriver: no partition info on output records ----------
        try (TopologyTestDriver stock = new TopologyTestDriver(topology, baseProps())) {
            final TestInputTopic<String, String> stockIn =
                stock.createInputTopic(in, STRING_SER, STRING_SER);
            final TestOutputTopic<String, String> stockOut =
                stock.createOutputTopic(out, STRING_DES, STRING_DES);
            for (final String k : keys) {
                stockIn.pipeInput(k, "v");
            }
            for (int i = 0; i < keys.length; i++) {
                final TestRecord<String, String> rec = stockOut.readRecord();
                // The TestRecord API exposes key, value, headers, timestamp — but no partition.
                // There is no public method that would let this test assert "this record went
                // to partition X". The partitioning behaviour is invisible to the test.
                assertNotNull(rec.getKey());
            }
        }

        // ---------- MultiPartitionTopologyTestDriver: partition is asserted per record ----------
        try (MultiPartitionTopologyTestDriver mp = new MultiPartitionTopologyTestDriver(topology, baseProps())) {
            mp.declareTopic(in, n);
            mp.declareTopic(out, n);
            mp.init();

            final MultiPartitionTestInputTopic<String, String> mpIn =
                mp.createInputTopic(in, STRING_SER, STRING_SER);
            final MultiPartitionTestOutputTopic<String, String> mpOut =
                mp.createOutputTopic(out, STRING_DES, STRING_DES);

            for (final String k : keys) {
                mpIn.pipeInput(k, "v");
            }

            final Map<String, Integer> seen = new HashMap<>();
            for (int i = 0; i < keys.length; i++) {
                final TestRecord<String, String> rec = mpOut.readRecord();
                seen.put(rec.getKey(), MultiPartitionTestRecord.partitionOf(rec));
            }

            for (final String k : keys) {
                final int expected = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(in, k), n);
                assertEquals(Integer.valueOf(expected), seen.get(k),
                    "MP-TTD must route key '" + k + "' to its production-side hash partition "
                        + expected + "; if a developer drops the rekey, this assertion fails at CI.");
            }
        }
    }

    /**
     * Risk #2 — Silent state split when sources are not co-partitioned.
     *
     * <p>The topology under test is intentionally buggy: two sources at <i>mismatched</i>
     * partition counts (2 and 3) feed a shared {@code .process(...)} store with no explicit
     * {@code Repartitioned} alignment.</p>
     *
     * <p><b>Under stock {@code TopologyTestDriver}</b>, both inputs collapse into a single
     * task and the same key from each side increments the <i>same</i> store entry —
     * count = 2. The test passes. Production then deploys with 2 vs 3 partitions, the same
     * key hashes to <i>different</i> store partitions on the two sides, and the per-key
     * state silently splits.</p>
     *
     * <p><b>Under {@code MultiPartitionTopologyTestDriver}</b>, the merged sub-topology runs
     * at {@code max(2, 3) = 3} partitions. The same key from the inA side lands on
     * {@code murmur2(k) % 2}; from the inB side it lands on {@code murmur2(k) % 3}. The two
     * increments do <i>not</i> co-locate. The test surfaces the bug as observable per-partition
     * state — exactly what production would do at scale.</p>
     */
    @Test
    public void coPartitionMismatchIsSilentUnderStockDriverButCaughtByMultiPartitionDriver() {
        final String inA = "buggy-side-a";
        final String inB = "buggy-side-b";

        final StreamsBuilder buggyBuilder = new StreamsBuilder();
        buggyBuilder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("counts"),
            Serdes.String(),
            Serdes.Long()));
        final KStream<String, String> sideA =
            buggyBuilder.stream(inA, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> sideB =
            buggyBuilder.stream(inB, Consumed.with(Serdes.String(), Serdes.String()));
        sideA.merge(sideB).process(SimpleCounter::new, "counts");
        final Topology buggy = buggyBuilder.build();

        final Properties props = baseProps();
        // With merged sources, the default max.task.idle.ms=0 makes pipeInput stall
        // waiting for the other side. -1 disables idling so each pipeInput drains.
        props.setProperty(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "-1");

        // ---------- Stock TTD: ONE TASK. Mismatch is invisible. ----------
        try (TopologyTestDriver stock = new TopologyTestDriver(buggy, props)) {
            stock.createInputTopic(inA, STRING_SER, STRING_SER).pipeInput("k", "from-A");
            stock.createInputTopic(inB, STRING_SER, STRING_SER).pipeInput("k", "from-B");

            final KeyValueStore<String, Long> store = stock.getKeyValueStore("counts");
            assertEquals(Long.valueOf(2L), store.get("k"),
                "stock TopologyTestDriver collapses partitions: both increments land in "
                    + "one bucket. The same code in production with 2-vs-3 partitions would "
                    + "silently split the state, but this test gives a misleading green.");
        }

        // ---------- MP-TTD: TASKS PER PARTITION. The split is observable. ----------
        try (MultiPartitionTopologyTestDriver mp = new MultiPartitionTopologyTestDriver(buggy, props)) {
            mp.declareTopic(inA, 2);
            mp.declareTopic(inB, 3);
            mp.init();

            // Pick a key whose %2 hash differs from its %3 hash so the split is observable.
            String key = null;
            int partA = -1;
            int partB = -1;
            for (final String candidate : new String[]{"k", "alpha", "beta", "gamma", "delta", "epsilon", "zeta"}) {
                final int a = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inA, candidate), 2);
                final int b = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inB, candidate), 3);
                if (a != b) {
                    key = candidate;
                    partA = a;
                    partB = b;
                    break;
                }
            }
            assertNotNull(key, "test setup: need a key whose %2 and %3 hashes diverge");

            mp.createInputTopic(inA, STRING_SER, STRING_SER).pipeInput(key, "from-A");
            mp.createInputTopic(inB, STRING_SER, STRING_SER).pipeInput(key, "from-B");

            assertEquals(3, mp.partitionsOf("counts"),
                "merged sub-topology runs at max(inA, inB) = 3 partitions");

            // The bug is now observable: the per-key state is split, not summed.
            assertEquals(Long.valueOf(1L),
                mp.<String, Long>getKeyValueStore("counts", partA).get(key),
                "inA's increment lives in store partition " + partA + " (its %2 hash)");
            assertEquals(Long.valueOf(1L),
                mp.<String, Long>getKeyValueStore("counts", partB).get(key),
                "inB's increment lives in store partition " + partB + " (its %3 hash) "
                    + "— STATE SPLIT that stock TTD could not see");

            // Quantify it: 2 events total, but spread across two partitions, never 2 in one.
            long total = 0;
            for (int p = 0; p < 3; p++) {
                final Long v = mp.<String, Long>getKeyValueStore("counts", p).get(key);
                if (v != null) {
                    total += v;
                }
            }
            assertEquals(2L, total, "events accounted for, but distributed");
        }
    }

    /**
     * Risk #3 — Partition count drift.
     *
     * <p>The demo's {@code EcommerceTopology} pins the post-repartition aggregate at
     * {@code Repartitioned.withNumberOfPartitions(4)}. Stock {@code TopologyTestDriver}
     * has no API to observe this number — it's a deploy-time artifact, completely
     * uncoupled from tests. A developer who changes the value (or whose change rolls back
     * during a refactor) would only notice in production.</p>
     *
     * <p>{@code MultiPartitionTopologyTestDriver} exposes
     * {@link MultiPartitionTopologyTestDriver#partitionsOf(String) partitionsOf(storeName)}
     * and runs that many task instances. The contract becomes a unit-test assertion.</p>
     */
    @Test
    public void storePartitionCountIsAssertableOnlyWithMultiPartitionDriver() {
        final Topology topology = EcommerceTopology.build(/* lowStockThreshold */ 5);

        try (MultiPartitionTopologyTestDriver mp =
                 new MultiPartitionTopologyTestDriver(topology, baseProps())) {
            mp.declareTopic(EcommerceTopology.CART_EVENTS_TOPIC, 4);
            mp.declareTopic(EcommerceTopology.ORDERS_TOPIC, 3);
            mp.declareTopic(EcommerceTopology.INVENTORY_UPDATES_TOPIC, 3);
            mp.declareTopic(EcommerceTopology.LOW_STOCK_ALERTS_TOPIC, 3);
            mp.init();

            // These three assertions have NO equivalent on stock TopologyTestDriver. They
            // pin the topology's partitioning contract at unit-test time. If a developer
            // edits Repartitioned.withNumberOfPartitions(4) → (8) — or removes it entirely —
            // assertion #2 fails in CI, before the change ever reaches production.
            assertEquals(4, mp.partitionsOf(EcommerceTopology.CART_STORE),
                "cart store inherits its 4-partition source layout");
            assertEquals(4, mp.partitionsOf(EcommerceTopology.TENANT_SKU_STORE),
                "tenant-sku store is pinned to 4 by the explicit Repartitioned");
            assertEquals(3, mp.partitionsOf(EcommerceTopology.INVENTORY_STORE),
                "inventory store inherits its 3-partition source layout");
        }
    }

    /**
     * Minimal counter processor used by
     * {@link #coPartitionMismatchIsSilentUnderStockDriverButCaughtByMultiPartitionDriver}.
     * Increments {@code counts[key]} on every input. Owns no other side-effect.
     */
    private static final class SimpleCounter implements Processor<String, String, Void, Void> {
        private KeyValueStore<String, Long> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore("counts");
        }

        @Override
        public void process(final Record<String, String> record) {
            final Long current = store.get(record.key());
            store.put(record.key(), (current == null ? 0L : current) + 1L);
        }
    }
}
