package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"time"

	"net/http"

	"github.com/segmentio/kafka-go"
)

func ProcessMessages(serviceAddr string, frequency int, runLength int, httpClient *http.Client, w *kafka.Writer) {
	for i := 0; i < runLength; i += frequency {
		startTime := time.Now().UnixNano()
		resp, err := httpClient.Get(serviceAddr)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		messages := make([]kafka.Message, 0, 100)
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			messages = append(messages, kafka.Message{Value: []byte(scanner.Text())})
		}

		if len(messages) > 0 {
			err = w.WriteMessages(context.Background(), messages...)

			if err != nil {
				log.Fatal("failed to write messages:", err)
			}

			fmt.Printf("Took %d milliseconds to process %d records\n", (time.Now().UnixNano()-startTime)/1e6, len(messages))
		}

		runTime := int(time.Now().UnixNano() - startTime)
		runTimeUpperSeconds := int(math.Ceil(float64(runTime) / float64(1e6)))

		if runTimeUpperSeconds < frequency {
			time.Sleep(time.Duration(frequency-runTimeUpperSeconds) * time.Second)
		}
	}
}

func main() {
	// to produce messages
	topic := flag.String("t", "my-topic", "Kafka topic to push messages to")
	kafkaAddr := flag.String("k", "localhost:9092", "Kafka URL to connect to")
	serviceAddr := flag.String("s", "http://localhost:4056/next", "URL of service to fetch next round of events")
	frequency := flag.Int("f", 1, "Frequency in seconds to check for new messages")
	runLength := flag.Int("r", math.MaxInt32, "Total number of seconds the app will run for (at most 1 request after this time).")
	flag.Parse()

	log.Printf("Writing data to topic: %s\n", *topic)

	// connection for Kafka
	conn, err := kafka.Dial("tcp", *kafkaAddr)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer conn.Close()

	w := &kafka.Writer{
		Addr:     kafka.TCP(*kafkaAddr),
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// read messages and just print count for now
	httpClient := &http.Client{Timeout: time.Second * 10}

	ProcessMessages(*serviceAddr, *frequency, *runLength, httpClient, w)
}
