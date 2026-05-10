package consulting.stream.demo.topology;

import consulting.stream.demo.processor.InventoryProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;

/**
 * Demo e-commerce topology that exercises the multi-partition test driver across three
 * disjoint sub-topologies:
 *
 * <ol>
 *   <li><b>Cart events (DSL stateful count)</b> — {@code cart-events} → {@code groupByKey().count()}
 *       into {@link #CART_STORE}. No rekeying, so the sub-topology runs at the source partition
 *       count.</li>
 *   <li><b>Orders aggregation (DSL with explicit repartition)</b> — {@code orders} →
 *       {@code selectKey(tenant#sku)} → {@code repartition(4)} → {@code groupByKey().aggregate(sum)}
 *       into {@link #TENANT_SKU_STORE}. The pre-repartition side runs at the source's partition
 *       count; the post-repartition sub-topology runs at the explicit 4.</li>
 *   <li><b>Inventory updates ({@code .process} with custom store)</b> — {@code inventory-updates} →
 *       {@link InventoryProcessor} writing {@link #INVENTORY_STORE} and forwarding low-stock
 *       alerts onto {@link #LOW_STOCK_ALERTS_TOPIC}.</li>
 * </ol>
 */
public final class EcommerceTopology {

    public static final String CART_EVENTS_TOPIC = "cart-events";
    public static final String ORDERS_TOPIC = "orders";
    public static final String INVENTORY_UPDATES_TOPIC = "inventory-updates";
    public static final String LOW_STOCK_ALERTS_TOPIC = "low-stock-alerts";

    public static final String CART_STORE = "cart-actions-per-user";
    public static final String TENANT_SKU_STORE = "tenant-sku-totals";
    public static final String INVENTORY_STORE = "inventory-store";

    public static final int ORDERS_REPARTITION_PARTITIONS = 4;

    private EcommerceTopology() {}

    public static Topology build(final long lowStockThreshold) {
        final StreamsBuilder builder = new StreamsBuilder();

        // ---- Sub-topology A: cart-events → groupByKey().count() ----
        builder.stream(CART_EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(Stores.inMemoryKeyValueStore(CART_STORE))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        // ---- Sub-topology B: orders → selectKey(tenant#sku) → repartition(4) → aggregate(sum) ----
        builder.stream(ORDERS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .selectKey((orderId, orderValue) -> {
                // value layout: "tenantId|sku|qty"
                final String[] parts = orderValue.split("\\|");
                return parts[0] + "#" + parts[1];
            })
            .repartition(Repartitioned.<String, String>with(Serdes.String(), Serdes.String())
                .withNumberOfPartitions(ORDERS_REPARTITION_PARTITIONS))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> 0L,
                (key, orderValue, agg) -> {
                    final String[] parts = orderValue.split("\\|");
                    return agg + Long.parseLong(parts[2]);
                },
                Materialized.<String, Long>as(Stores.inMemoryKeyValueStore(TENANT_SKU_STORE))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long()));

        // ---- Sub-topology C: inventory-updates → .process(InventoryProcessor) → low-stock-alerts ----
        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(INVENTORY_STORE),
            Serdes.String(),
            Serdes.Long()));

        final KStream<String, Long> inventoryStream =
            builder.stream(INVENTORY_UPDATES_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()));

        inventoryStream
            .process(() -> new InventoryProcessor(INVENTORY_STORE, lowStockThreshold), INVENTORY_STORE)
            .to(LOW_STOCK_ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
