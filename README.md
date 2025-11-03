### Problem Statement

We have multiple validators producing block data to the same kafka topic. Hence for any block height, we have that many duplicates as validators; How do we deduplicate the data for downstream services?


### Approach 1: kafka streams - Using state store 

Validators produce keyed records to the topic with key being the block height (eg: '*block_height_1*' for height 1). This ensures the all the duplicates for a particular block height are being produced with same key and hence land on the same topic partition on the kafka broker.

Now, we need to deduplicate keyed msgs on the partition level, we are sure that duplicates can't land on different partitions.

In kafka streams, each topic partition is processed by a fixed '*task*'. A *task* is the smallest unit of parallelism in kafka streams. More can be read about tasks and high level architecture of kafka streams from  [here](https://kafka.apache.org/41/documentation/streams/architecture#streams_architecture_tasks).

Each *task* can have one or more local state stores embedded into it. The kafka streams documentation states following about the fault-tolerance of tasks:

```
Kafka Streams builds on fault-tolerance capabilities integrated natively within Kafka. Kafka partitions are highly available and replicated; so when stream data is persisted to Kafka it is available even if the application fails and needs to re-process it. Tasks in Kafka Streams leverage the fault-tolerance capability offered by the Kafka consumer client to handle failures. If a task runs on a machine that fails, Kafka Streams automatically restarts the task in one of the remaining running instances of the application.

In addition, Kafka Streams makes sure that the local state stores are robust to failures, too. For each state store, it maintains a replicated changelog Kafka topic in which it tracks any state updates. These changelog topics are partitioned as well so that each local state store instance, and hence the task accessing the store, has its own dedicated changelog topic partition.

```

We already know all the duplicates for a particular block height belong to the same partition and hence will be processed by the same task. We can use local state store to store the keys which have been already processed, hence discarding any record with already seen key.

Kafka streams also provides *exactly-once* processing gurantees, which is configurable. It ensures commits on the input topic offsets, updates on the state stores, and writes to the output topics will be completed atomically. This avoids any race condition possible, for ex two records with same key being processed at the same time and hence none of them being discarded. More about processing guarantees can be read from the [doc](https://kafka.apache.org/41/documentation/streams/core-concepts#streams_processing_guarantee) and about exactly-once semantics from this [KIP](https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics)


