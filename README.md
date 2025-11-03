### Problem Statement

We have multiple validators producing block data to the same kafka topic. Hence for any block height, we have that many duplicates as validators; How do we deduplicate the data for downstream services?


### Initial processing on the validator side
Validators produce keyed records to the topic with key being the block height (eg: '*block_height_1*' for height 1). This ensures the all the duplicates for a particular block height are being produced with same key and hence land on the same topic partition on the kafka broker.


### Approach 1: Kafka streams - using groupByKey and Windowing

We first apply groupByKey transform to the input stream, which groups the records by existing keys. It's a stateless transformation, and hence no state store is needed.

Then we apply the windowing transform, which lets us sub-group the records with same key for stateful transformations like aggregations or joins. Kafka streams supports following types of windows:
-   *Hopping Time Window*: These are time-interval based windows defined by 2 properties: window size and advance interval. The advance interval specifies by how much the window will move forward. These windows overlap and hence a data record can belong to multiple of these windows
-   *Tumbling Time Window*: These are a special case of hopping time windows and are based on time intervals. They model fixed-size, non-overlapping, gap-less windows. It is defined by a single property: the window’s size. It is a hopping window whose window size is equal to its advance interval. Since these never overlap, a data record will belong to one and only one window.
-   *Sliding Time Window*: These are fixed-size window that slide continuously over the time axis. Two data records are said to be included in the same window if the difference of their timestamps is within the window size. As it moves along the time axis, records may fall into multiple snapshots of the window, but each unique combination of records appears only in one window snapshot.
-   *Session Time Window*: These are used to aggregate key-based events into so-called sessions. Sessions represent a period of activity separated by a defined gap of inactivity. Any events processed that fall within the inactivity gap of any existing sessions are merged into the existing sessions. If an event falls outside of the session gap, then a new session will be created.


The Sliding Window is the one that seems most reasonable for our purpose, coz if we get a block data from a validator then we can expect a duplicate of it in near future, and sliding window merges events closer in time. We still need to define the 'near future', what should be max difference in timestamp that we'll wait for?

I tried sliding window, it didnt seem to work. It seems it is emitting multiple records from a single window (even after using suppress operator described below). So I went with tumbling window.

With tumbling windows we'll have to choose appropriate window size so that it covers all duplicates.

Once we have the windowed stream, we use *reduce* which is a type of *aggregation* that combines the values of the records *per window*.

This is where we do the actual deduplication. The *reduce* takes a lambda expression which does the reduction. 

```
(aggValue, newVal) -> aggValue
```

This is the lambda we have used. When the first record arrives for a window, the value of that record is used as the initial aggVal. For the subsequent records, we just ignore the records value and keep the same aggVal; as can be seen from the lambda exp.

The *reduce* transform returns a Ktable. Ktable has a property of emitting records for downstream operators whenever the table is updated. This is to ensure the streaming application provides continuous fresh values (More can be read about this [here](https://kafka.apache.org/41/documentation/streams/developer-guide/dsl-api.html#window-final-results) and [here](https://kafka.apache.org/41/documentation/streams/developer-guide/dsl-api.html#controlling-emit-rate)). But in our case, we want only one value to be emitted per window, which is enforced by the *suppress* operator. 

After suppress we just do some formatting and publish to the deduped topic.


### Approach 2: kafka streams - Using state store 

Once the initial processing is performed, we need to deduplicate keyed msgs on the partition level, we are sure that duplicates can't land on different partitions.

In kafka streams, each topic partition is processed by a fixed '*task*'. A *task* is the smallest unit of parallelism in kafka streams. More can be read about tasks and high level architecture of kafka streams from  [here](https://kafka.apache.org/41/documentation/streams/architecture#streams_architecture_tasks).

Each *task* can have one or more local state stores embedded into it. The kafka streams documentation states following about the fault-tolerance of tasks:


> Kafka Streams builds on fault-tolerance capabilities integrated natively within Kafka.
Kafka partitions are highly available and replicated; so when stream data is persisted to Kafka it is available even if the application fails and needs to re-process it. 

> Tasks in Kafka Streams leverage the fault-tolerance capability offered by the Kafka
> consumer client to handle failures. If a task runs on a machine that fails, Kafka 
> Streams automatically restarts the task in one of the remaining running instances of 
> the application.

> In addition, Kafka Streams makes sure that the local state stores are robust to failures, too. For each state store, it maintains a replicated changelog Kafka topic in which it tracks any state updates. 

> These changelog topics are partitioned as well so that each local state store instance, and hence the task accessing the store, has its own dedicated changelog topic partition.



We already know all the duplicates for a particular block height belong to the same partition and hence will be processed by the same task. We can use local state store to store the keys which have been already processed, hence discarding any record with already seen key.

Kafka streams also provides *exactly-once* processing gurantees, which is configurable. It ensures commits on the input topic offsets, updates on the state stores, and writes to the output topics will be completed atomically. This avoids any race condition possible, for ex two records with same key being processed at the same time and hence none of them being discarded. More about processing guarantees can be read from the [doc](https://kafka.apache.org/41/documentation/streams/core-concepts#streams_processing_guarantee) and about exactly-once semantics from this [KIP](https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics)


Now the issue with our approach is that our local state store is going to grow indefinitely. To solve this, Kafka Streams provides a WindowStore which keeps the data only for the specified time period and purges it afterwards.


### Demo

To test if the approahes work, I have created a mock producer under */kafka* which imitates multiple validators pushing block data to a *input* topic. And i have a mock consumer which consumes from *output* topic. 

To test Approach 1:


1.  Do *docker compose up -d* in /kafka dir to spin up a kafka broker
2.  Create input and output topic by running following cmd:
    -   *docker exec -it {kafka_container_id} /opt/kafka/bin/kafka-topics.sh --create --topic input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1*
    -   *docker exec -it {kafka_container_id} /opt/kafka/bin/kafka-topics.sh --create --topic output --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1*
3. Make sure main class is set to *com.example.dedup.WindowedDedup* in *build.gradle.kts*
4. Run the deduper using following cmd:
    -   *./gradlew clean run --args "localhost:9092 input output kstreams-dedup"*
5. Run the producer and consumer in separate terminals 


You should see duplicated msgs in producers terminal and deduplicated msgs in consumers.

To test Approach 2:

Do the same as approach 1, except set the main class to *com.example.dedup.DedupStreamApp*


