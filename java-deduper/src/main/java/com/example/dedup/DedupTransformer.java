package com.example.dedup;

import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.KeyValue;

public class DedupTransformer implements Transformer<String, String, KeyValue<String, String>> {
    private final String storeName;
    private KeyValueStore<String, Long> seen;

    public DedupTransformer(String storeName) { this.storeName = storeName; }

    @Override public void init(ProcessorContext context) { this.seen = context.getStateStore(storeName); }

    @Override public KeyValue<String, String> transform(String key, String value) {
        if (key == null) return null;
        if (seen.get(key) == null) {
            seen.put(key, System.currentTimeMillis());
            return new KeyValue<>(key, value);
        }
        return null; // drop duplicate
    }
    @Override public void close() {}
}