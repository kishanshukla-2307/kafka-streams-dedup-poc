package com.example.dedup;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import java.time.Duration;
import java.util.Properties;

public class WindowedDedup {

    // Define the time window for deduplication (5 minutes)
    private static final Duration DEDUP_WINDOW_SIZE = Duration.ofMinutes(5);

    public static void main(String[] args) {
        final String bootstrap  = args.length > 0 ? args[0] : "localhost:9092";
        final String inputTopic = args.length > 1 ? args[1] : "input";
        final String outputTopic= args.length > 2 ? args[2] : "output";
        final String appId      = args.length > 3 ? args[3] : "kstreams-dedup-windowed";

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
               // 1. Group records by key.
               .groupByKey()
               // 2. Define a 5-minute tumbling window. All duplicates within this window will be dropped.
               .windowedBy(TimeWindows.of(DEDUP_WINDOW_SIZE).grace(Duration.ofMinutes(0)))
               // 3. Reduce: Keep the first record received (v1) and discard any subsequent duplicates (v2) within the window.
               // The KTable produced here will only update when a new key is seen for the first time in a window.
               .reduce((v1, v2) -> v1, Materialized.as("dedup-window-store"))
               // 4. Convert the KTable back to a KStream. This KStream emits an event ONLY when a record is first reduced.
               .toStream()
               // 5. Map the key back from Windowed<String> to String for the output topic.
               .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value))
               // 6. Send the deduplicated, time-windowed record to the output topic.
               .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), p);
        
        // Clean up state on startup for easy local development (remove for production)
        // streams.cleanUp(); 

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        
        System.out.println("Kafka Streams Dedup App Started with 5-minute window.");
        try { 
            // Keep the application running indefinitely
            Thread.sleep(Duration.ofDays(365).toMillis()); 
        } catch (InterruptedException ignored) {}
    }
}
