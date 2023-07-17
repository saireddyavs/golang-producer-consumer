package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	// Set up configuration for Kafka consumer
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify brokers to connect to
	brokers := []string{"localhost:9092", "localhost:9093", "localhost:9094"}

	// Create new consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Define topic and partition to consume from
	topic := "DUMMY-TOPIC-1"
	partition := int32(0)

	// Set offset to newest
	offset := sarama.OffsetNewest

	// Start consuming messages
	consumerPartition, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := consumerPartition.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Set up signals to handle graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Start goroutine to consume messages
	go func() {
		for {
			select {
			case msg := <-consumerPartition.Messages():
				log.Printf("Received message: %s\n", string(msg.Value))
			case err := <-consumerPartition.Errors():
				log.Printf("Error: %s\n", err.Error())
			case <-signals:
				return
			}
		}
	}()

	// Wait for interrupt signal to gracefully shut down
	<-signals
	log.Println("Shutting down")
}
