package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	var brokers, topic string
	var producers, count int
	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka bootstrap servers")
	flag.StringVar(&topic, "topic", "raw_msgs", "Topic to produce to")
	flag.IntVar(&producers, "producers", 8, "Number of concurrent producers")
	flag.IntVar(&count, "count", 100, "Events per producer")
	flag.Parse()

	var wg sync.WaitGroup
	wg.Add(producers)

	for p := 0; p < producers; p++ {
		go func(id int) {
			defer wg.Done()
			prod, err := kafka.NewProducer(&kafka.ConfigMap{
				"bootstrap.servers":  brokers,
				"acks":               "all",
				"enable.idempotence": true,
				// keep default partitioner; key hash ensures routing
			})
			if err != nil {
				log.Fatalf("producer %d: %v", id, err)
			}
			defer prod.Close()

			// delivery reports
			go func() {
				for e := range prod.Events() {
					switch ev := e.(type) {
					case *kafka.Message:
						if ev.TopicPartition.Error != nil {
							log.Printf("producer %d delivery failed: %v", id, ev.TopicPartition)
						}
					}
				}
			}()

			for i := 1; i <= count; i++ {
				key := []byte(strconv.Itoa(i)) // event_id as string
				val := []byte(fmt.Sprintf("hello %d", i))
				err = prod.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Key:            key,
					Value:          val,
				}, nil)
				if err != nil {
					log.Printf("producer %d produce error: %v", id, err)
				}
				// tiny jitter to interleave duplicates
				time.Sleep(time.Duration(id%3) * time.Millisecond)
			}
			// ensure flush
			prod.Flush(10_000)
			log.Printf("producer %d done", id)
		}(p)
	}

	wg.Wait()
	log.Printf("produced %d×%d messages to %s", producers, count, topic)
}
