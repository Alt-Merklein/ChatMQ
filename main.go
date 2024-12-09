package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/gorilla/websocket"
)

// Message structure for communication with the server.
type Message struct {
	Action string `json:"action"`
	Topic  string `json:"topic"`
	Data   string `json:"data"`
}

func main() {
	// Connect to the WebSocket server.
	serverAddr := "ws://localhost:8080/ws"
	conn, _, err := websocket.DefaultDialer.Dial(serverAddr, nil)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to server:", serverAddr)

	// Handle interrupt signals to close connection gracefully.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	done := make(chan struct{})

	// Listen for messages from the server in a separate goroutine.
	go func() {
		defer close(done)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("Read error:", err)
				return
			}
			fmt.Println("Received:", string(message))
		}
	}()

	// Subscribe to a topic.
	subscribeMsg := Message{
		Action: "subscribe",
		Topic:  "news",
	}
	if err := sendMessage(conn, subscribeMsg); err != nil {
		log.Println("Failed to subscribe:", err)
		return
	}

	// Publish a message to the topic.
	publishMsg := Message{
		Action: "publish",
		Topic:  "news",
		Data:   "Hello, this is a test message for the news topic!",
	}
	if err := sendMessage(conn, publishMsg); err != nil {
		log.Println("Failed to publish:", err)
		return
	}

	// Wait for server messages or interrupt signal.
	for {
		select {
		case <-done:
			return
		case <-interrupt:
			log.Println("Interrupt received, closing connection...")
			// Send a close message to the server.
			err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("Write close message error:", err)
			}
			return
		}
	}
}

// sendMessage encodes and sends a JSON message to the server.
func sendMessage(conn *websocket.Conn, msg Message) error {
	messageJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return conn.WriteMessage(websocket.TextMessage, messageJSON)
}
