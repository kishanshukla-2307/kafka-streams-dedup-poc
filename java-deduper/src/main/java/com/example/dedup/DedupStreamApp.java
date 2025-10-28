package com.example.dedup;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;

public class DedupStreamApp {
    public static void main(String[] args) {
        final String bootstrap  = args.length>0?args[0]:"localhost:9092";
        final String inputTopic = args.length>1?args[1]:"input";
        final String outputTopic= args.length>2?args[2]:"output";
        final String appId      = args.length>3?args[3]:"kstreams-dedup";

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);

        StreamsBuilder b = new StreamsBuilder();
        var store = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("seen-store"),
            Serdes.String(), Serdes.Long()
        );
        b.addStateStore(store);

        b.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
         .transform(() -> new DedupTransformer("seen-store"), "seen-store")
         .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams s = new KafkaStreams(b.build(), p);
        Runtime.getRuntime().addShutdownHook(new Thread(s::close));
        s.start();
        try { Thread.sleep(Duration.ofDays(365).toMillis()); } catch (InterruptedException ignored) {}
    }
}