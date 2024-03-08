package main

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func main() {
	// Устанавливаем соединение с RabbitMQ
	connection, err := amqp091.Dial("amqp://rmuser:rmpassword@localhost:5672/")
	if err != nil {
		log.Fatalf("Ошибка при установке соединения с RabbitMQ: %v", err)
	}
	defer connection.Close()

	// Создаем канал
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Ошибка при создании канала: %v", err)
	}
	defer channel.Close()

	// Объявляем очередь
	queueName := "webinar_queue"
	q, err := channel.QueueDeclare(
		queueName, // Имя очереди
		true,      // Долговеченость (переживет ли RabbitMQ перезапуск)
		false,     // Удалится ли очередь, когда отписавшихся от нее потребителей не останется
		false,     // Эксклюзивность (может ли использоваться только текущим соединением
		false,     // Автоудаление (удалится ли очередь, когда ее больше не используют никто)
		nil,       // Аргументы
	)
	if err != nil {
		log.Fatalf("Ошибка при объявлении очереди: %v", err)
	}

	msgs, err := channel.Consume(
		q.Name, // Имя очереди
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	var forever chan struct{}

	go func() {
		for d := range msgs {
			fmt.Printf("Received a message: %s\n", d.Body)
			time.Sleep(1 * time.Second)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
