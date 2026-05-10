package consulting.stream.demo.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * PAPI processor that maintains a per-sku stock count in a partitioned key-value store.
 *
 * <p>Inputs are signed deltas keyed by sku. The processor reads the current stock, applies
 * the delta, persists the new value, and forwards a {@code (sku, currentStock)} alert
 * downstream when the post-update stock is strictly below {@code lowStockThreshold}.</p>
 *
 * <p>Designed to demonstrate that {@link org.apache.kafka.streams.MultiPartitionTopologyTestDriver}
 * gives each {@code (subtopologyId, partition)} its own store instance — each instance ends
 * up holding only the skus that hash to that partition.</p>
 */
public class InventoryProcessor implements Processor<String, Long, String, Long> {

    private final String storeName;
    private final long lowStockThreshold;

    private KeyValueStore<String, Long> store;
    private ProcessorContext<String, Long> ctx;

    public InventoryProcessor(final String storeName, final long lowStockThreshold) {
        this.storeName = storeName;
        this.lowStockThreshold = lowStockThreshold;
    }

    @Override
    public void init(final ProcessorContext<String, Long> context) {
        this.ctx = context;
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(final Record<String, Long> record) {
        final String sku = record.key();
        final Long delta = record.value();
        if (sku == null || delta == null) {
            return;
        }
        final Long current = store.get(sku);
        final long updated = (current == null ? 0L : current) + delta;
        store.put(sku, updated);

        if (updated < lowStockThreshold) {
            ctx.forward(record.withValue(updated));
        }
    }
}
