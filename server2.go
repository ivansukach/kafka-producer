//package main
//
//import (
//	"context"
//	"fmt"
//	"github.com/segmentio/kafka-go"
//	log "github.com/sirupsen/logrus"
//	"time"
//)
//
//func main() {
//	log.Info("Kafka producer started")
//
//	selector := true
//	topic := "my-topic15"
//	partition := 0
//
//	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
//	if err != nil {
//		log.Error(err)
//	}
//
//	//file, err := os.Open("whoIsWho.txt")
//	//if err != nil {
//	//	fmt.Println("Unable to open file:", err)
//	//	os.Exit(1)
//	//}
//	//data := make([]byte, 64)
//	//amount, err := file.Read(data)
//	//str := string(data[:amount])
//	//if str == "server1:producer" || str == "server2:consumer" {
//	//	selector = false
//	//}
//	//file.Close()
//	tRole := time.NewTicker(time.Second * 30)
//	tMes := time.NewTicker(time.Second * 5)
//	//reader := bufio.NewReader(os.Stdin)
//	message := ""
//	for {
//		select {
//			case <-tMes.C:
//				if selector {
//					_, err := conn.WriteMessages(
//						kafka.Message{Value: []byte(fmt.Sprintf("server2 say: %s", time.Now().String()))},
//					)
//					if err != nil {
//						log.Error(err)
//					}
//					log.Println("send message")
//				}
//			case <-tRole.C:
//				if selector {
//					//message="server1:producer"
//					log.Println("Event on server2 :", message)
//					_, err := conn.WriteMessages(
//						kafka.Message{Value: []byte(message)},
//					)
//					if err != nil {
//						log.Error(err)
//					}
//
//				} else{
//					log.Println("Event on server2 :", message)
//					message="server2:producer"
//					_, err := conn.WriteMessages(
//						kafka.Message{Value: []byte(message)},
//					)
//					if err != nil {
//						log.Error(err)
//					}
//
//				}
//
//		}
//		//if selector {
//		//	log.Println("Введите сообщение:")
//		//	m, _, _ := reader.ReadLine()
//		//	message = string(m)
//		//}
//		//
//		//if selector {
//		//	_, err := conn.WriteMessages(
//		//		kafka.Message{Value: []byte(message)},
//		//		kafka.Message{Value: []byte("by server2")},
//		//	)
//		//	if err != nil {
//		//		log.Error(err)
//		//	}
//		//} else {
//		//if !selector{
//		//	log.Println("Attempt to ReadBatch")
//		//	batch := conn.ReadBatch(1, 1e6) // fetch 10KB min, 1MB max
//		//	log.Println("ReadBatch success")
//		//	for {
//		//		b := make([]byte, 10e3) // 10KB max per message
//		//		amount, err := batch.Read(b)
//		//		bb := b[:amount]
//		//		if err != nil {
//		//			break
//		//		}
//		//		mes := string(bb)
//		//		fmt.Println(mes)
//		//		if mes == "server2:producer" {
//		//			log.Println("Event on server2 : selector->produce")
//		//			selector = true
//		//			break
//		//		} else if mes == "exit" {
//		//			return
//		//		}
//		//	}
//		//
//		//}
//		if message == "server1:producer"{
//			log.Println("Event on server2 : selector->consume")
//			selector = false
//		} else if message == "exit" {
//			return
//		}
//	}
//
//	defer conn.Close()
//}

