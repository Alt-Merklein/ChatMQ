package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	//"os"
	//"os/signal"

	"github.com/gorilla/websocket"
)

// Message structure for communication with the server.
type PubSubMessage struct {
	Action          string `json:"action"`
	Topic           string `json:"topic"`
	Data            string `json:"data"`
	ClientMessageNo int    `json:"ClientMessageNo"`
}

type AckMessage struct {
	ClientMessageNo int `json:"ClientMessageNo"`
}

type ServerMessage struct {
	QueueMessageNo int    `json:"QueueMessageNo"`
	Data           string `json:"data"`
}

var (
	UnackedClientMessageBuffer = make(map[int]PubSubMessage)     // Buffer to store unacknowledged messages
	bufferMutex                sync.Mutex                        // Mutex to protect access to the buffer
	ClientMsgNumber            int                           = 0 // Starts with 0. Identifies the current message number for the user. Also used on server acks to identify the acked message
	LastAckedClientMsgNumber   int                           = -1
	QueueMsgNumber             int                           = -1 //Unknown queue start location at the beginning
)

func main() {

	var topic string
	flag.StringVar(&topic, "topic", "", "Topic to subscribe to")
	flag.Parse()

	if topic == "" {
		fmt.Println("Error: Please specify a valid topic using the -topic flag.")
		os.Exit(1)
	}

	// Connect to the WebSocket server.
	serverAddr := "ws://localhost:8080/ws"
	conn, _, err := websocket.DefaultDialer.Dial(serverAddr, nil)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to server:", serverAddr)
	// start receiving messages
	go listenForServer(conn)

	subscribeMsg := PubSubMessage{
		Action: "subscribe",
		Topic:  topic,
	}

	if err := sendPubSubMessage(conn, subscribeMsg); err != nil {
		log.Println("Failed to subscribe:", err)
		return
	}
	// Read user input in a loop and send it to the server.
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter your message (or type 'exit' to quit): ")
		if !scanner.Scan() {
			log.Println("Error reading input:", scanner.Err())
			break
		}

		text := scanner.Text()
		if text == "exit" {
			fmt.Println("Exiting...")
			break
		}

		// Construct and send a message.
		msg := PubSubMessage{
			Action:          "publish", // Replace with appropriate action
			Topic:           topic,     // Replace with user-specified or default topic
			Data:            text,      // User input
			ClientMessageNo: ClientMsgNumber,
		}
		err := sendPubSubMessage(conn, msg)
		if err != nil {
			log.Println("Failed to send message:", err)
		}
	}

}

// sendMessage encodes and sends a JSON message to the server.
func sendPubSubMessage(conn *websocket.Conn, msg PubSubMessage) error {

	bufferMutex.Lock()
	UnackedClientMessageBuffer[ClientMsgNumber] = msg
	ClientMsgNumber++
	bufferMutex.Unlock()

	messageJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return conn.WriteMessage(websocket.TextMessage, messageJSON)
}

func listenForServer(conn *websocket.Conn) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			return
		}
		// fmt.Println("Received:", string(message))
		DecodeMessage(message)
	}
}

func DecodeMessage(msg []byte) {
	// Decode into a generic map to inspect fields
	var genericMessage map[string]interface{}
	err := json.Unmarshal(msg, &genericMessage) // Ensure err is declared
	if err != nil {
		log.Println("Failed to decode message:", err)
		return
	}

	// Check message type based on fields
	if _, exists := genericMessage["ClientMessageNo"]; exists {
		// Handle AckMessage
		var ack AckMessage
		err = json.Unmarshal(msg, &ack)
		if err != nil {
			log.Println("Failed to decode AckMessage:", err)
			return
		}
		fmt.Println("Acknowledgment received:", ack.ClientMessageNo)
		go handleServerAck(ack.ClientMessageNo)
	} else {
		// Handle unexpected or unknown message formats
		fmt.Println("Unknown message format received:", string(msg))
	}
}

func handleServerAck(msgAckNo int) {
	// Lock the buffer to ensure safe concurrent access
	// server must ack the next unacked message
	bufferMutex.Lock()
	defer bufferMutex.Unlock()

	if msgAckNo != LastAckedClientMsgNumber+1 {
		return
	}
	// Check if the message with the given AckNo exists in the buffer
	if msg, exists := UnackedClientMessageBuffer[msgAckNo]; exists {
		// Remove the acknowledged message from the buffer
		delete(UnackedClientMessageBuffer, msgAckNo)
		LastAckedClientMsgNumber++
		fmt.Printf("Message with AckNo %d acknowledged and removed from buffer: %+v\n", msgAckNo, msg)
	} else {
		// Log a warning if the acknowledgment is for an unknown or already removed message
		fmt.Printf("Warning: Received acknowledgment for unknown or already removed message with AckNo %d\n", msgAckNo)
	}
}
