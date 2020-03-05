package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func main() {
	log.Info("Kafka producer started")

	selector := true
	topic := "my-topic3"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Error(err)
	}

	file, err := os.Open("whoIsWho.txt")
	if err != nil {
		fmt.Println("Unable to open file:", err)
		os.Exit(1)
	}
	data := make([]byte, 64)
	amount, err := file.Read(data)
	str := string(data[:amount])
	if str == "server1:producer" || str == "server2:consumer" {
		selector = false
	}
	file.Close()
	t := time.NewTicker(time.Second * 30)
	tm := time.NewTicker(time.Second * 5)
	reader := bufio.NewReader(os.Stdin)
	message := ""
	for {
		select {}
		if selector {
			log.Println("Введите сообщение:")
			m, _, _ := reader.ReadLine()
			message = string(m)
		}

		if selector {
			_, err := conn.WriteMessages(
				kafka.Message{Value: []byte(message)},
				kafka.Message{Value: []byte("by server2")},
			)
			if err != nil {
				log.Error(err)
			}
		} else {
			batch := conn.ReadBatch(1, 1e6) // fetch 10KB min, 1MB max
			for {
				b := make([]byte, 10e3) // 10KB max per message
				amount, err := batch.Read(b)
				bb := b[:amount]
				if err != nil {
					break
				}
				mes := string(bb)
				fmt.Println(mes)
				if mes == "server2:consumer" {
					selector = false
					break
				} else if mes == "exit" {
					return
				}
			}
		}
		if message == "server1:producer" || message == "server2:consumer" {
			selector = false
		} else if message == "exit" {
			return
		}
	}

	defer conn.Close()
}
