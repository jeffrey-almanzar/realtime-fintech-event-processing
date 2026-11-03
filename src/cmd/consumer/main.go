// Kafka consumer that processes financial events.

package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "financial-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	c.SubscribeTopics([]string{"financial-events"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Printf("Received message: %s", string(msg.Value))
		} else {
			log.Printf("Consumer error: %v (%v)
", err, msg)
		}
	}

	c.Close()
}