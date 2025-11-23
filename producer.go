package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Create writer (producer)
	writer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "orders",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: false, // we already created the topic
	}

	defer func() {
		if err := writer.Close(); err != nil {
			log.Println("failed to close writer:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 1; i <= 5; i++ {
		value := fmt.Sprintf("OrderID: %d, Amount: %d", i, 100*i)

		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("order-%d", i)),
			Value: []byte(value),
		}

		log.Printf("Sending message %d: %s\n", i, value)

		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("failed to write message %d: %v", i, err)
		}
	}

	log.Println("All messages sent successfully!")
}
