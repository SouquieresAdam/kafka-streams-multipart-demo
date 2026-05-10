package consulting.stream.demo;

import consulting.stream.demo.topology.EcommerceTopology;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.MultiPartitionTestInputTopic;
import org.apache.kafka.streams.MultiPartitionTestOutputTopic;
import org.apache.kafka.streams.MultiPartitionTopologyTestDriver;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end demonstration of {@link MultiPartitionTopologyTestDriver} on a realistic
 * e-commerce topology built from three disjoint sub-topologies:
 *
 * <ul>
 *   <li><b>Sub-topology A — DSL stateful count</b> on {@code cart-events} (4 partitions),
 *       keyed by user, into the {@code cart-actions-per-user} store.</li>
 *   <li><b>Sub-topology B — DSL selectKey + explicit repartition + aggregate</b> on
 *       {@code orders} (3 partitions), rekeyed to {@code tenant#sku}, repartitioned to 4,
 *       aggregated into {@code tenant-sku-totals}.</li>
 *   <li><b>Sub-topology C — PAPI {@code .process} with partitioned store</b> on
 *       {@code inventory-updates} (3 partitions), maintaining {@code inventory-store}
 *       and emitting low-stock alerts below a configured threshold.</li>
 * </ul>
 */
public class EcommerceTopologyTest {

    private static final StringSerializer STRING_SER = new StringSerializer();
    private static final StringDeserializer STRING_DES = new StringDeserializer();
    private static final LongSerializer LONG_SER = new LongSerializer();
    private static final LongDeserializer LONG_DES = new LongDeserializer();

    private static final String CART_EVENTS = EcommerceTopology.CART_EVENTS_TOPIC;
    private static final String ORDERS = EcommerceTopology.ORDERS_TOPIC;
    private static final String INVENTORY_UPDATES = EcommerceTopology.INVENTORY_UPDATES_TOPIC;
    private static final String LOW_STOCK_ALERTS = EcommerceTopology.LOW_STOCK_ALERTS_TOPIC;

    private static final String CART_STORE = EcommerceTopology.CART_STORE;
    private static final String TENANT_SKU_STORE = EcommerceTopology.TENANT_SKU_STORE;
    private static final String INVENTORY_STORE = EcommerceTopology.INVENTORY_STORE;

    private static Properties baseProps() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "ecommerce-demo");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    private static MultiPartitionTopologyTestDriver newDriver(final Topology topology) {
        final MultiPartitionTopologyTestDriver driver = new MultiPartitionTopologyTestDriver(topology, baseProps());
        driver.declareTopic(CART_EVENTS, 4);
        driver.declareTopic(ORDERS, 3);
        driver.declareTopic(INVENTORY_UPDATES, 3);
        driver.declareTopic(LOW_STOCK_ALERTS, 3);
        driver.init();
        return driver;
    }

    /**
     * Sub-topology A: cart events flow through {@code groupByKey().count()} into a partitioned
     * key-value store. Each user's events must accumulate in the partition determined by the
     * production partitioner, demonstrating that the multi-partition driver runs N stream tasks
     * for the source and routes per the murmur2 hash.
     */
    @Test
    public void cartActionsAreCountedPerUserAcrossPartitions() {
        final Topology topology = EcommerceTopology.build(/* lowStockThreshold */ 5);

        try (MultiPartitionTopologyTestDriver driver = newDriver(topology)) {
            final MultiPartitionTestInputTopic<String, String> cart =
                driver.createInputTopic(CART_EVENTS, STRING_SER, STRING_SER);

            final String[] users = {"alice", "bob", "carol", "dave", "eve"};
            for (final String user : users) {
                cart.pipeInput(user, "ADD:sku-1");
                cart.pipeInput(user, "ADD:sku-2");
                cart.pipeInput(user, "REMOVE:sku-1");
            }

            assertEquals(4, driver.partitionsOf(CART_STORE),
                "the cart-actions-per-user store must inherit the 4-partition source layout");

            for (final String user : users) {
                final int p = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(CART_EVENTS, user), 4);
                final KeyValueStore<String, Long> store = driver.getKeyValueStore(CART_STORE, p);
                assertNotNull(store, "expected the cart store to exist at partition " + p);
                assertEquals(Long.valueOf(3L), store.get(user),
                    "user '" + user + "' should have 3 cart events recorded in partition " + p);
            }
        }
    }

    /**
     * Sub-topology B: orders are rekeyed to {@code tenant#sku} and explicitly repartitioned to
     * 4 partitions before aggregation. The same logical key from different source partitions
     * must converge to a single store partition — this is what the explicit repartition buys us.
     */
    @Test
    public void tenantSkuTotalsAggregateAfterRepartition() {
        final Topology topology = EcommerceTopology.build(5);

        try (MultiPartitionTopologyTestDriver driver = newDriver(topology)) {
            final MultiPartitionTestInputTopic<String, String> orders =
                driver.createInputTopic(ORDERS, STRING_SER, STRING_SER);

            // Three orders for tenant "acme", sku "WIDGET-1": qty 3, 5, 2 → total 10.
            orders.pipeInput("order-1", "acme|WIDGET-1|3");
            orders.pipeInput("order-2", "acme|WIDGET-1|5");
            orders.pipeInput("order-3", "acme|WIDGET-1|2");
            // Two orders for tenant "globex", sku "GADGET-9": qty 7, 1 → total 8.
            orders.pipeInput("order-4", "globex|GADGET-9|7");
            orders.pipeInput("order-5", "globex|GADGET-9|1");
            // One order for a different sku for "acme" — must not bleed into the WIDGET-1 total.
            orders.pipeInput("order-6", "acme|WIDGET-2|4");

            assertEquals(4, driver.partitionsOf(TENANT_SKU_STORE),
                "explicit Repartitioned.withNumberOfPartitions(4) must pin the aggregate store at 4 partitions");

            final String acmeWidget1Key = "acme#WIDGET-1";
            final int acmeWidget1Part = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(ORDERS, acmeWidget1Key), 4);
            final KeyValueStore<String, Long> acmeStore =
                driver.getKeyValueStore(TENANT_SKU_STORE, acmeWidget1Part);
            assertEquals(Long.valueOf(10L), acmeStore.get(acmeWidget1Key),
                "acme#WIDGET-1 total must aggregate to 10 in its repartitioned store partition");

            final String globexKey = "globex#GADGET-9";
            final int globexPart = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(ORDERS, globexKey), 4);
            final KeyValueStore<String, Long> globexStore =
                driver.getKeyValueStore(TENANT_SKU_STORE, globexPart);
            assertEquals(Long.valueOf(8L), globexStore.get(globexKey),
                "globex#GADGET-9 total must aggregate to 8");

            final String acmeWidget2Key = "acme#WIDGET-2";
            final int acmeWidget2Part = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(ORDERS, acmeWidget2Key), 4);
            assertEquals(Long.valueOf(4L),
                driver.<String, Long>getKeyValueStore(TENANT_SKU_STORE, acmeWidget2Part).get(acmeWidget2Key),
                "the second sku for acme must aggregate independently");
        }
    }

    /**
     * Sub-topology C ({@code .process} operator): inventory deltas are applied per sku via a
     * custom PAPI processor that owns a partitioned key-value store. This proves the driver
     * gives each (subtopology, partition) task its own store instance and that
     * {@link org.apache.kafka.streams.processor.api.ProcessorContext#getStateStore} resolves
     * to the partition-local store.
     */
    @Test
    public void inventoryProcessorMaintainsStockPerSku() {
        final Topology topology = EcommerceTopology.build(5);

        try (MultiPartitionTopologyTestDriver driver = newDriver(topology)) {
            final MultiPartitionTestInputTopic<String, Long> inv =
                driver.createInputTopic(INVENTORY_UPDATES, STRING_SER, LONG_SER);

            inv.pipeInput("sku-A", 100L);
            inv.pipeInput("sku-A", -10L);
            inv.pipeInput("sku-A", -5L);
            inv.pipeInput("sku-B", 50L);
            inv.pipeInput("sku-B", -20L);

            assertEquals(3, driver.partitionsOf(INVENTORY_STORE),
                "the inventory-store must run with 3 partitions, matching its source");

            final int partA = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(INVENTORY_UPDATES, "sku-A"), 3);
            final int partB = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(INVENTORY_UPDATES, "sku-B"), 3);

            assertEquals(Long.valueOf(85L),
                driver.<String, Long>getKeyValueStore(INVENTORY_STORE, partA).get("sku-A"),
                "sku-A net stock should be 100 - 10 - 5 = 85");
            assertEquals(Long.valueOf(30L),
                driver.<String, Long>getKeyValueStore(INVENTORY_STORE, partB).get("sku-B"),
                "sku-B net stock should be 50 - 20 = 30");
        }
    }

    /**
     * Sub-topology C, alert path: when the post-update stock falls strictly below the
     * configured threshold, the processor forwards a {@code (sku, currentStock)} record to
     * {@code low-stock-alerts}. Above-or-equal updates produce no alert. Output records must
     * preserve the source-side partition (no repartition between the processor and the sink).
     */
    @Test
    public void inventoryProcessorEmitsLowStockAlertsBelowThreshold() {
        final int threshold = 10;
        final Topology topology = EcommerceTopology.build(threshold);

        try (MultiPartitionTopologyTestDriver driver = newDriver(topology)) {
            final MultiPartitionTestInputTopic<String, Long> inv =
                driver.createInputTopic(INVENTORY_UPDATES, STRING_SER, LONG_SER);
            final MultiPartitionTestOutputTopic<String, Long> alerts =
                driver.createOutputTopic(LOW_STOCK_ALERTS, STRING_DES, LONG_DES);

            // sku-A starts at 50 (no alert), drops to 8 (alert), then 4 (alert), then climbs to 12 (no alert).
            inv.pipeInput("sku-A", 50L);   // stock = 50  → no alert
            inv.pipeInput("sku-A", -42L);  // stock = 8   → alert
            inv.pipeInput("sku-A", -4L);   // stock = 4   → alert
            inv.pipeInput("sku-A", 8L);    // stock = 12  → no alert

            // sku-B never falls below the threshold.
            inv.pipeInput("sku-B", 100L);  // no alert
            inv.pipeInput("sku-B", -50L);  // stock = 50, no alert

            // Boundary test: equal-to-threshold must NOT alert (strict less-than).
            inv.pipeInput("sku-C", 10L);   // stock = 10, no alert (exactly at threshold)
            inv.pipeInput("sku-C", -1L);   // stock = 9, alert

            final List<TestRecord<String, Long>> emitted = alerts.readRecordsToList();
            assertEquals(3, emitted.size(),
                "three updates dropped strictly below threshold 10: sku-A=8, sku-A=4, sku-C=9; got " + emitted);

            final Map<String, Long> latestPerSku = new HashMap<>();
            final Map<String, Integer> partitionPerSku = new HashMap<>();
            for (final TestRecord<String, Long> rec : emitted) {
                latestPerSku.put(rec.getKey(), rec.getValue());
                partitionPerSku.put(rec.getKey(),
                    org.apache.kafka.streams.test.MultiPartitionTestRecord.partitionOf(rec));
            }

            assertEquals(Long.valueOf(4L), latestPerSku.get("sku-A"),
                "the last alert for sku-A must reflect the most recent post-update stock (4)");
            assertEquals(Long.valueOf(9L), latestPerSku.get("sku-C"),
                "sku-C alert must report the post-update stock (9)");
            assertNull(latestPerSku.get("sku-B"),
                "sku-B never went below threshold and must not appear in alerts");

            final int expectedPartA = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(INVENTORY_UPDATES, "sku-A"), 3);
            assertEquals(Integer.valueOf(expectedPartA), partitionPerSku.get("sku-A"),
                "alerts for sku-A must be emitted on the same partition the source landed on");
        }
    }

    /**
     * Top-level smoke test: running the three sub-topologies side-by-side under one driver
     * gives three independent task fan-outs. Sub-topologies A and C own a source each at
     * 4 and 3 partitions, while B has a 3-partition source paired with a 4-partition repartition
     * downstream — so the driver should expose <b>at least</b> three sub-topologies and the
     * declared partition counts on the right stores.
     */
    @Test
    public void multiSubTopologyIndependenceAndPartitionCounts() {
        final Topology topology = EcommerceTopology.build(5);

        try (MultiPartitionTopologyTestDriver driver = newDriver(topology)) {
            final List<Integer> sids = driver.subtopologies();
            assertTrue(sids.size() >= 3,
                "expected at least 3 sub-topologies (cart, orders+repartition, inventory); got " + sids);

            assertEquals(4, driver.partitionsOf(CART_STORE),       "cart store: 4 partitions");
            assertEquals(4, driver.partitionsOf(TENANT_SKU_STORE), "tenant-sku store: 4 (after repartition)");
            assertEquals(3, driver.partitionsOf(INVENTORY_STORE),  "inventory store: 3 (matches source)");

            // The three stores must live in three different sub-topologies.
            final Set<Integer> uniqueSids = new HashSet<>();
            for (final int sid : sids) {
                final int n = driver.partitionsOfSubtopology(sid);
                if (n == 3 || n == 4) {
                    uniqueSids.add(sid);
                }
            }
            assertTrue(uniqueSids.size() >= 3,
                "the three flows should each map to a distinct sub-topology; got " + uniqueSids);
        }
    }

    /**
     * Drives all three sub-topologies in the same test to demonstrate that a single
     * {@link MultiPartitionTopologyTestDriver} can host a heterogeneous, stateful topology
     * with multiple sources, multiple stores, an explicit repartition and a custom processor.
     */
    @Test
    public void allThreeFlowsCoexistUnderASingleDriver() {
        final Topology topology = EcommerceTopology.build(10);

        try (MultiPartitionTopologyTestDriver driver = newDriver(topology)) {
            final MultiPartitionTestInputTopic<String, String> cart =
                driver.createInputTopic(CART_EVENTS, STRING_SER, STRING_SER);
            final MultiPartitionTestInputTopic<String, String> orders =
                driver.createInputTopic(ORDERS, STRING_SER, STRING_SER);
            final MultiPartitionTestInputTopic<String, Long> inv =
                driver.createInputTopic(INVENTORY_UPDATES, STRING_SER, LONG_SER);
            final MultiPartitionTestOutputTopic<String, Long> alerts =
                driver.createOutputTopic(LOW_STOCK_ALERTS, STRING_DES, LONG_DES);

            cart.pipeInput("alice", "ADD:sku-1");
            orders.pipeInput("o1", "acme|sku-1|2");
            inv.pipeInput("sku-1", 4L); // stock = 4, threshold = 10 → alert

            // Sub-topology A: alice has 1 cart event in her partition.
            final int alicePart = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(CART_EVENTS, "alice"), 4);
            assertEquals(Long.valueOf(1L),
                driver.<String, Long>getKeyValueStore(CART_STORE, alicePart).get("alice"));

            // Sub-topology B: acme#sku-1 aggregated to qty 2 (after repartition).
            final int acmeSkuPart = BuiltInPartitioner.partitionForKey(
                STRING_SER.serialize(ORDERS, "acme#sku-1"), 4);
            assertEquals(Long.valueOf(2L),
                driver.<String, Long>getKeyValueStore(TENANT_SKU_STORE, acmeSkuPart).get("acme#sku-1"));

            // Sub-topology C: sku-1 stock is 4 and the alert was emitted.
            assertFalse(alerts.isEmpty(), "low-stock alert expected for sku-1 (stock=4 < threshold=10)");
            final TestRecord<String, Long> alert = alerts.readRecord();
            assertEquals("sku-1", alert.getKey());
            assertEquals(Long.valueOf(4L), alert.getValue());
        }
    }
}
