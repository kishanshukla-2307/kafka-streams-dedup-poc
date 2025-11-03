package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "input"
	broker := "localhost:9092" // Use the external advertised listener port

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer p.Close()

	// Delivery report handler (optional but recommended)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message with key: %s to topic %s [%d] at offset %v, Time: [%v]\n", ev.Key,
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, ev.Timestamp)
				}
			}
		}
	}()

	// Produce messages
	var block_data []string
	blocks := 100
	for i := range blocks {
		block_data = append(block_data, fmt.Sprintf("Block height: %v data from kafka input topic", i))
	}

	Validators := 4

	for block_height, block := range block_data {
		for _ = range Validators {
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("block_height_%d", block_height)),
				Value:          []byte(block),
				Timestamp:      time.Now(),
			}, nil)
			time.Sleep(1 * time.Second)
		}
		// p.Produce(&kafka.Message{
		// 	TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		// 	Key:            []byte("key"),
		// 	Value:          []byte(msg),
		// }, nil)
		// time.Sleep(1 * time.Second)
	}

	// Wait for all messages to be delivered
	p.Flush(5 * 1000)
}
