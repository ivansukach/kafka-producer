package main

import (
	"context"
	"fmt"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/websocket"
	"time"
)

func changeRole(c echo.Context, selector *bool) error {
	websocket.Handler(func(ws *websocket.Conn) {
		defer ws.Close()
		for {
			log.Println("Handler")
			msg := ""
			err := websocket.Message.Receive(ws, &msg)
			if err != nil {
				c.Logger().Error(err)
			}
			fmt.Printf("%s\n", msg)
			switch msg {
			case "server2:producer":
				log.Println("Let's produce messages")
				*selector = true
			}
		}
	}).ServeHTTP(c.Response(), c.Request())
	return nil
}

func main() {
	selector := true
	go messageExchange(&selector)
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Static("/", "public")
	e.GET("/ws", func(c echo.Context) error {
		return changeRole(c, &selector)
	})
	e.Logger.Fatal(e.Start(":1323"))

}
func messageExchange(selector *bool) {
	origin := "http://localhost/"
	url := "ws://localhost:1333/ws"
	ws, err := websocket.Dial(url, "", origin)
	if err != nil {
		log.Fatal(err)
	}
	topic := "my100topic"
	partition := 0
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	defer conn.Close()
	tRole := time.NewTicker(time.Second * 30)
	tMes := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-tRole.C:
			if *selector {
				if _, err := ws.Write([]byte("server1:producer")); err != nil {
					log.Fatal(err)
				}
				log.Println("Let's read messages")
				*selector = false
			}
		case <-tMes.C:
			if *selector {
				log.Println("write")
				_, err := conn.WriteMessages(
					kafka.Message{Value: []byte(fmt.Sprintf("server2 say: %s", time.Now().UTC().String()))},
				)
				if err != nil {
					log.Error(err)
				}
			}
			if !*selector {
				log.Println("read")
				conn.SetReadDeadline(time.Now().Add(3 * time.Second))
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
				}
				batch.Close()
			}
		}

	}
}
