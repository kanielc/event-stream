package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
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

func PrintOutMessages(topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
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
	topic := "my-topic"

	// connection for Kafka
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer conn.Close()

	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	conn.SetWriteDeadline(time.Now().Add(3 * time.Second))

	// read messages and just print count for now
	httpClient := &http.Client{Timeout: time.Second * 10}

	for i := 0; i < 100; i++ {
		startTime := time.Now().UnixNano()
		resp, err := httpClient.Get("http://localhost:8080/next")
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
		time.Sleep(2 * time.Second)
	}

	//CreateTopic(conn, topic)
	/*topics, err := ListTopics(conn)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("topics", strings.Join(topics, ", "))

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	PrintOutMessages(topic)
	*/
}
