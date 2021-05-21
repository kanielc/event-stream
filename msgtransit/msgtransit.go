package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"strconv"
	"time"

	"net/http"

	"github.com/segmentio/kafka-go"
)

func CreateTopic(conn *kafka.Conn, topic string) {
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}

func ListTopics(conn *kafka.Conn) ([]string, error) {
	partitions, err := conn.ReadPartitions()

	if err != nil {
		return []string{}, err
	}

	m := map[string]bool{}

	for _, p := range partitions {
		m[p.Topic] = true
	}

	res := make([]string, len(m))
	pos := 0
	for k := range m {
		res[pos] = k
		pos++
	}

	return res, nil
}

func PrintOutMessages(topic string, kafkaAddr string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaAddr},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(0)

	defer r.Close()
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}

func main() {
	// to produce messages
	topic := flag.String("t", "my-topic", "Kafka topic to push messages to")
	kafkaAddr := flag.String("k", "localhost:9092", "Kafka URL to connect to")
	serviceAddr := flag.String("s", "http://localhost:4056/next", "URL of service to fetch next round of events")
	frequency := flag.Int("f", 1, "Frequency in seconds to check for new messages")
	runLength := flag.Int("r", math.MaxInt32, "Total number of seconds the app will run for (at most 1 request after this time).")

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

	for i := 0; i < *runLength; i += *frequency {
		startTime := time.Now().UnixNano()
		resp, err := httpClient.Get(*serviceAddr)
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

		if runTimeUpperSeconds < *frequency {
			time.Sleep(time.Duration(*frequency-runTimeUpperSeconds) * time.Second)
		}
	}
}
