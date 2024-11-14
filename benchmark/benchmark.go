package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:19092"}
	topic := "benchmark_topic"
	messageCount := 100000 // Total messages to send
	concurrency := 1000    // Concurrent producers

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	var wg sync.WaitGroup
	wg.Add(concurrency)

	start := time.Now()
	var totalSent int64

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			producer, err := sarama.NewSyncProducer(brokers, config)
			if err != nil {
				log.Fatalf("Failed to create producer: %v", err)
			}
			defer producer.Close()

			for j := 0; j < messageCount/concurrency; j++ {
				message := &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.StringEncoder("Benchmarking message"),
				}

				_, _, err := producer.SendMessage(message)
				if err != nil {
					log.Printf("Error sending message: %v", err)
				}
				totalSent++
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start).Seconds()

	// Calculate throughput and latency
	throughput := float64(totalSent) / elapsed
	avgLatency := elapsed / float64(totalSent) * 1000

	fmt.Printf("Benchmarking Results:\n")
	fmt.Printf("Total Messages Sent: %d\n", totalSent)
	fmt.Printf("Time Elapsed: %.2f seconds\n", elapsed)
	fmt.Printf("Throughput: %.2f messages/second\n", throughput)
	fmt.Printf("Average Latency per message: %.2f ms\n", avgLatency)
}
