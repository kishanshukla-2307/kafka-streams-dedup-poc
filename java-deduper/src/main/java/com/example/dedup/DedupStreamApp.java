package com.example.dedup;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

public class DedupStreamApp {
    public static void main(String[] args) {
        final String bootstrap  = args.length>0?args[0]:"localhost:9092";
        final String inputTopic = args.length>1?args[1]:"input";
        final String outputTopic= args.length>2?args[2]:"output";
        final long minSpaceBtwEvents = args.length>3?Long.parseLong(args[3]):1000 * 60 * 2; // in ms
        final String appId      = "kstreams-dedup";
        final String storeName  = "seen-store";

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);

        StreamsBuilder b = new StreamsBuilder();
        // var store = Stores.keyValueStoreBuilder(
        //     Stores.inMemoryKeyValueStore("seen-store"),
        //     Serdes.String(), Serdes.Long()
        // );
        // b.addStateStore(store);

        final Duration windowSize = Duration.ofMinutes(10);
        final Duration retentionPeriod = windowSize;

        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(storeName,
                                        retentionPeriod,
                                        windowSize,
                                        false
            ),
            Serdes.String(),
            Serdes.Long());

        b.addStateStore(dedupStoreBuilder);

        b.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
         .transform(() -> new DedupTransformer(storeName, minSpaceBtwEvents), storeName)
         .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams s = new KafkaStreams(b.build(), p);
        Runtime.getRuntime().addShutdownHook(new Thread(s::close));
        s.start();
        try { Thread.sleep(Duration.ofDays(365).toMillis()); } catch (InterruptedException ignored) {}
    }
}