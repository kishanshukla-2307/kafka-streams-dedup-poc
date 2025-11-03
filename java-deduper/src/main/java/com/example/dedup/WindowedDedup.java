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
        p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);
        p.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, Duration.ofSeconds(5).toMillis()); 
        // p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);

        StreamsBuilder builder = new StreamsBuilder();

        Materialized<String, String, WindowStore<Bytes, byte[]>> dedupStore = 
            Materialized.<String, String, WindowStore<Bytes, byte[]>>as("dedup-window-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());

        Duration timeDifference = Duration.ofMinutes(2);
        Duration gracePeriod = Duration.ofSeconds(10);

        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
        
               .filter((key, value) -> key != null)
               
               .groupByKey()

               // tumbling window
               .windowedBy(TimeWindows.ofSizeAndGrace(DEDUP_WINDOW_SIZE, WINDOW_GRACE_PERIOD))

                // sliding window
                // .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod))
               
               .reduce((v1, v2) -> v1, dedupStore)
               
               .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
              
               .toStream()
               
               .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value))
               
               .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), p);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        
        // System.out.println("Kafka Streams Dedup App Started. Window Size: " + DEDUP_WINDOW_SIZE.toMinutes() + " minutes.");
        try { 
            Thread.sleep(Duration.ofDays(365).toMillis()); 
        } catch (InterruptedException ignored) {}
    }
}
