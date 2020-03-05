package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	topic := "my13topic"
	partition := 0

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	defer conn.Close()
	tRole := time.NewTicker(time.Second * 30)
	tMes := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-tRole.C:
			fmt.Println("Time is over")
		case <-tMes.C:
			fmt.Println("write")
			_, err := conn.WriteMessages(
				kafka.Message{Value: []byte(fmt.Sprintf("server2 say: %s", time.Now().UTC().String()))},
			)
			if err != nil {
				log.Error(err)
			}
		}
	}

	conn.Close()
}
