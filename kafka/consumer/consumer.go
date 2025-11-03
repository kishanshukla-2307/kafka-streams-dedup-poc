package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "output"
	broker := "localhost:9092"
	groupID := "go-test-consumer-group"

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupID,
		"auto.offset.reset": "earliest", // Start reading from the beginning of the topic
	})

	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer c.Close()

	c.SubscribeTopics([]string{topic}, nil)

	// Set up signal handling to cleanly exit
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100) // Poll for events with a timeout
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// fmt.Printf("Consumed message from %s: %s\n",
				// 	e.TopicPartition, string(e.Value))
				fmt.Printf("Consumed message from %s, with key: %s\n",
					e.TopicPartition, string(e.Key))
				if string(e.Value) == "FINISH" {
					run = false // Exit after receiving the "FINISH" message
				}
				// Manually commit offset (optional, depends on your Beam test scenario)
				// _, err := c.CommitMessage(e)
			case kafka.Error:
				// Errors should generally be considered informational (e.g., EOF)
				if e.IsFatal() {
					fmt.Fprintf(os.Stderr, "Fatal Error: %v: %v\n", e.Code(), e)
					run = false
				}
			}
		}
	}
}
