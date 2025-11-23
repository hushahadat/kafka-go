package main

import (
    "context"
    "encoding/json"
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
    // Consumer group id – all instances with same GroupID share the work
    groupID := "order-consumers"

    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:        []string{"localhost:9092"},
        Topic:          "orders",
        GroupID:        groupID,
        MinBytes:       1,        // minimum bytes to fetch
        MaxBytes:       10e6,     // 10MB
        CommitInterval: time.Second, // how often to commit offsets
    })
    defer func() {
        if err := r.Close(); err != nil {
            log.Println("failed to close reader:", err)
        }
    }()

    log.Printf("Starting consumer in group %q, listening on topic %q...\n", groupID, "orders")

    ctx := context.Background()

    for {
        m, err := r.ReadMessage(ctx)
        if err != nil {
            log.Fatalf("failed to read message: %v", err)
        }

        var order Order
        if err := json.Unmarshal(m.Value, &order); err != nil {
            log.Printf("failed to unmarshal JSON (offset %d, partition %d): %v\n", m.Offset, m.Partition, err)
            continue
        }

        log.Printf(
            "✓ Received message | partition=%d offset=%d key=%s orderID=%s amount=%.2f user=%s",
            m.Partition,
            m.Offset,
            string(m.Key),
            order.ID,
            order.Amount,
            order.UserID,
        )
    }
}
