package com.example.dedup;

import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.KeyValue;

public class DedupTransformer implements Transformer<String, String, KeyValue<String, String>> {
    private ProcessorContext ctx;
    private final String storeName;
    private WindowStore<String, Long> seen;
    private final long minSpaceBtwEvents;



    public DedupTransformer(String storeName, long minSpaceBtwEvents) { 
        this.storeName = storeName; 
        this.minSpaceBtwEvents = minSpaceBtwEvents;
    }

    @Override public void init(ProcessorContext context) { 
        this.ctx = context;
        this.seen = ctx.getStateStore(storeName); 
    }

    @Override public KeyValue<String, String> transform(String key, String value) {
        if (key == null) return null;
        Long eventTime = ctx.timestamp();

        final WindowStoreIterator<Long> iterator = seen.fetch(
            key, 
            eventTime - minSpaceBtwEvents, 
            eventTime + minSpaceBtwEvents);

        // update the latest timestamp for the event
        seen.put(key, eventTime, eventTime);
        if (iterator.hasNext()) {
            // already seen, do nothing
            return null;
        } else {
            return new KeyValue<>(key, value);
        }
    }
    @Override public void close() {}
}