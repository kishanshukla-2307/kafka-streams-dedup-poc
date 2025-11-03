// package com.example.dedup;

// import org.apache.kafka.common.serialization.Serdes;
// import org.apache.kafka.streams.KafkaStreams;
// import org.apache.kafka.streams.StreamsBuilder;
// import org.apache.kafka.streams.StreamsConfig;
// import org.apache.kafka.streams.kstream.*;
// import org.apache.kafka.streams.KeyValue;
// import org.apache.kafka.streams.state.WindowStore;
// import java.time.Duration;
// import java.util.Properties;

// public class WindowedDedup {

//     // Define the time window for deduplication (5 minutes)
//     private static final Duration DEDUP_WINDOW_SIZE = Duration.ofMinutes(24*60*7);

//     public static void main(String[] args) {
//         final String bootstrap  = args.length > 0 ? args[0] : "localhost:9092";
//         final String inputTopic = args.length > 1 ? args[1] : "input1";
//         final String outputTopic= args.length > 2 ? args[2] : "output1";
//         final String appId      = args.length > 3 ? args[3] : "kstreams-dedup-windowed";

//         Properties p = new Properties();
//         p.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
//         p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
//         p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//         p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//         p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);

//         StreamsBuilder builder = new StreamsBuilder();

//         KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
//         KGroupedStream<String, String> groupedStream = inputStream.groupByKey();
//         TimeWindowedKStream<String, String> windowedStream = groupedStream.windowedBy(TimeWindows.ofSizeAndGrace(DEDUP_WINDOW_SIZE, Duration.ofMinutes(0)));
//         KTable<Windowed<String>, String> filteredKtable = windowedStream.reduce((v1, v2) -> v1, Materialized.as("dedup-window-store"));
//         KStream<Windowed<String>, String> kstream = filteredKtable.toStream();
//         KStream<String, String> formattedStream = kstream.map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value));
//         formattedStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

//         // builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
//         //        // CRITICAL STEP 0: Filter out records with null keys, as they cannot be grouped correctly.
//         //        // Only records with a non-null key are eligible for groupByKey and windowing.
//         //        .filter((key, value) -> key != null)
//         //        // 1. Group records by key.
//         //        .groupByKey()
//         //        // 2. Define a 5-minute tumbling window. All duplicates within this window will be dropped.
//         //        // Grace period is set to zero, meaning late events outside the window will be dropped.
//         //        .windowedBy(TimeWindows.of(DEDUP_WINDOW_SIZE).grace(Duration.ofMinutes(0)))
//         //        // 3. Reduce: Keep the first record received (v1) and discard any subsequent duplicates (v2) within the window.
//         //        // The KTable produced here will only update when a new key is seen for the first time in a window.
//         //        .reduce((v1, v2) -> v1, Materialized.as("dedup-window-store"))
//         //        // 4. Convert the KTable back to a KStream. This KStream emits an event ONLY when a record is first reduced.
//         //        .toStream()
//         //        // 5. Map the key back from Windowed<String> to String for the output topic.
//         //        .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value))
//         //        // 6. Send the deduplicated, time-windowed record to the output topic.
//         //        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        
//         // builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
//         //        // 1. Group records by key.
//         //        .groupByKey()
//         //        // 2. Define a 5-minute tumbling window. All duplicates within this window will be dropped.
//         //        .windowedBy(TimeWindows.of(DEDUP_WINDOW_SIZE).grace(Duration.ofMinutes(10)))
//         //        // 3. Reduce: Keep the first record received (v1) and discard any subsequent duplicates (v2) within the window.
//         //        // The KTable produced here will only update when a new key is seen for the first time in a window.
//         //     //    .reduce((v1, v2) -> v1, Materialized.as("dedup-window-store"))

//         //        .aggregate(
//         //         () -> null, /* initializer */
//         //         (aggKey, newValue, aggValue) -> aggValue, /* adder */
//         //         Materialized.<String, Long, WindowStore<Byte, byte[]>>as("time-windowed-aggregated-stream-store") /* state store name */
//         //         .withValueSerde(Serdes.Long())) /* serde for aggregate value */
//         //        // 4. Convert the KTable back to a KStream. This KStream emits an event ONLY when a record is first reduced.
//         //        .toStream()
//         //        // 5. Map the key back from Windowed<String> to String for the output topic.
//         //        .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value))
//         //        // 6. Send the deduplicated, time-windowed record to the output topic.
//         //        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

//         KafkaStreams streams = new KafkaStreams(builder.build(), p);
        
//         // Clean up state on startup for easy local development (remove for production)
//         // streams.cleanUp(); 

//         Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//         streams.start();
        
//         System.out.println("Kafka Streams Dedup App Started with 5-minute window.");
//         try { 
//             // Keep the application running indefinitely
//             Thread.sleep(Duration.ofDays(365).toMillis()); 
//         } catch (InterruptedException ignored) {}
//     }
// }




// ----------------------
// ----------------------

package com.example.dedup;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.common.utils.Bytes;
import java.time.Duration;
import java.util.Properties;

public class WindowedDedup {

    private static final Duration DEDUP_WINDOW_SIZE = Duration.ofMinutes(1); 
    private static final Duration WINDOW_GRACE_PERIOD = Duration.ofSeconds(0);

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
        p.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, Duration.ofSeconds(5).toMillis()); 
        // p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);

        StreamsBuilder builder = new StreamsBuilder();

        Materialized<String, String, WindowStore<Bytes, byte[]>> dedupStore = 
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("dedup-window-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());


        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        
               .filter((key, value) -> key != null)
               
               .groupByKey()

               // tumbling window
               .windowedBy(TimeWindows.ofSizeAndGrace(DEDUP_WINDOW_SIZE, WINDOW_GRACE_PERIOD))
               
               .reduce((v1, v2) -> v1, dedupStore)
               
               .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
              
               .toStream()
               
               .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value))
               
               .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), p);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        
        System.out.println("Kafka Streams Dedup App Started. Window Size: " + DEDUP_WINDOW_SIZE.toMinutes() + " minutes.");
        try { 
            Thread.sleep(Duration.ofDays(365).toMillis()); 
        } catch (InterruptedException ignored) {}
    }
}
