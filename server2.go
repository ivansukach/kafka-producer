package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	topic := "my13313topic"
	partition := 0
	selector := true
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	defer conn.Close()
	tRole := time.NewTicker(time.Second * 30)
	tMes := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-tRole.C:
			if selector {
				log.Println("Time is over")
				_, err := conn.WriteMessages(
					kafka.Message{Value: []byte(fmt.Sprintf("server1:producer"))},
				)
				if err != nil {
					log.Error(err)
				}
				log.Println("Event on server2 : selector->consume")
				selector = false
			} else {
				log.Println("Time to read is over")
			}

		case <-tMes.C:
			if selector {
				log.Println("write")
				_, err := conn.WriteMessages(
					kafka.Message{Value: []byte(fmt.Sprintf("server2 say: %s", time.Now().UTC().String()))},
				)
				if err != nil {
					log.Error(err)
				}
			}
			if !selector {
				log.Println("read")
				batch := conn.ReadBatch(1, 1e3) // fetch 10KB min, 1MB max
				for {
					b := make([]byte, 1e3) // 10KB max per message
					amount, err := batch.Read(b)
					if err != nil {
						break
					}
					bb := b[:amount]
					mes := string(bb)
					log.Println(mes)
					if mes == "server2:producer" {
						log.Println("Event on server2 : selector->produce")
						selector = true
						break
					}
				}
				batch.Close()
			}
		}

	}
}
