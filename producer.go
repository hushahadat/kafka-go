package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/segmentio/kafka-go"
)

type Order struct {
    ID        string  `json:"id"`
    UserID    string  `json:"userId"`
    Amount    float64 `json:"amount"`
    Currency  string  `json:"currency"`
    CreatedAt string  `json:"createdAt"`
}

func main() {
    writer := &kafka.Writer{
        Addr:                   kafka.TCP("localhost:9092"),
        Topic:                  "orders",
        Balancer:               &kafka.LeastBytes{},
        AllowAutoTopicCreation: true, // topic already exists, but safe
    }
    defer func() {
        if err := writer.Close(); err != nil {
            log.Println("failed to close writer:", err)
        }
    }()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    for i := 1; i <= 5; i++ {
        order := Order{
            ID:        fmt.Sprintf("order-%d", i),
            UserID:    fmt.Sprintf("user-%d", i),
            Amount:    float64(100 * i),
            Currency:  "INR",
            CreatedAt: time.Now().Format(time.RFC3339),
        }

        // Convert struct → JSON
        valueBytes, err := json.Marshal(order)
        if err != nil {
            log.Fatalf("failed to marshal order: %v", err)
        }

        msg := kafka.Message{
            Key:   []byte(order.ID),
            Value: valueBytes,
        }

        log.Printf("Producing order: %s\n", valueBytes)

        if err := writer.WriteMessages(ctx, msg); err != nil {
            log.Fatalf("failed to write message: %v", err)
        }
    }

    log.Println("All orders produced successfully!")
}
