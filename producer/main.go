package main

import (
	"encoding/json"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// MessageStruct - структура сообщения
type MessageStruct struct {
	Message string    `json:"message"`
	ID      int64     `json:"id"`
	Date    time.Time `json:"date"`
}

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
	_, err = channel.QueueDeclare(
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

	for i := 0; i <= 100; i++ {
		fmt.Println(i)
		messageBody := makeMessage(int64(i))

		// Публикуем сообщение в очередь
		err = channel.Publish( // Publish - deprecated
			"",        // пустая строка, т.к. мы используем default exchange)
			queueName, // Имя очереди
			false,     // Обязательность (если ни одна из очередей не связана с указанным маршрутизационным ключом, сообщение будет потеряно)
			false,     // Передача сообщения в очередь только одному потребителю
			amqp091.Publishing{
				ContentEncoding: "application/json",
				Body:            messageBody,
			},
		)
		if err != nil {
			log.Fatalf("Ошибка при публикации сообщения: %v", err)
		}
	}
	fmt.Println("Сообщение успешно отправлено в очередь RabbitMQ")
}

func makeMessage(id int64) []byte {
	// Создаем экземпляр структыры сообщения
	message := MessageStruct{
		Message: "Привет RabbitMQ!",
		ID:      id,
		Date:    time.Now(),
	}

	// Преобразуем структуру в JSON
	messageBody, err := json.Marshal(message)
	if err != nil {
		log.Fatalf("Ошибка при преобразовании структуры в JSON: %v", err)
	}

	return messageBody
}
