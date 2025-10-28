package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	var brokers, topic, group string
	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka bootstrap servers")
	flag.StringVar(&topic, "topic", "deduped_msgs", "Topic to consume")
	flag.StringVar(&group, "group", "printer-1", "Consumer group id")
	flag.Parse()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          group,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer c.Close()

	if err := c.Subscribe(topic, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	log.Printf("Consuming from %s ...", topic)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

run:
	for {
		select {
		case <-sig:
			break run
		default:
			ev := c.Poll(250)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("DEDUPED -> key=%s value=%s partition=%d offset=%d\n",
					string(e.Key), string(e.Value), e.TopicPartition.Partition, e.TopicPartition.Offset)
			case kafka.Error:
				log.Printf("consumer error: %v", e)
				time.Sleep(200 * time.Millisecond)
			}
		}
	}

	log.Println("consumer shutting down")
}
