package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type ManualPartitioner struct{}

func (p *ManualPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	// Implement your custom partition logic here
	// You can use any custom logic to determine the target partition
	// In this example, we will simply return a fixed partition number
	return 0, nil // Return the desired partition number
}

func main() {
	// Set up configuration for Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	fmt.Println("---> default refresh time", config.Metadata.RefreshFrequency)

	// Specify brokers to connect to
	brokers := []string{"localhost:9093"}

	// Create new producer
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Create message to produce
	topic := "DUMMY-TOPIC-1"
	message := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: 0,
		Value:     sarama.StringEncoder("Hello, Kafka!"),
	}

	// Send message to Kafka
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Message sent successfully. Partition: %d, Offset: %d\n", partition, offset)
}
