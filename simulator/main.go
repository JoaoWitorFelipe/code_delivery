package main

import (
	"fmt"
	kafka2 "github.com/JoaoWitorFelipe/code_delivery-simulator/application/kafka"
	"github.com/JoaoWitorFelipe/code_delivery-simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("error loading .env file")
	}
}

// {"clientId":"1","routeId":"1"}
// {"clientId":"2","routeId":"2"}
// {"clientId":"3","routeId":"3"}
func main() {

	msgChan := make(chan *ckafka.Message)
	consumer := kafka.NewKafkaConsumer(msgChan)

	go consumer.Consume()

	for msg := range msgChan {
		fmt.Println(string(msg.Value))
		go kafka2.Produce(msg)
	}
}
