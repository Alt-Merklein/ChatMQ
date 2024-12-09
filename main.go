package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Message structure for communication with the server.
type PubSubMessage struct {
	Action          string `json:"action"`
	Topic           string `json:"topic"`
	Data            string `json:"data"`
	ClientMessageNo int    `json:"clientMessageNo"`
}

type ToServerAckMessage struct {
	QueueMessageNo int `json:"queueMessageNo"`
}

type FromServerAckMessage struct {
	ClientMessageNo int `json:"clientMessageNo"`
}

type ServerMessage struct {
	QueueMessageNo int    `json:"queueMessageNo"`
	Data           string `json:"data"`
}

var (
	UnackedClientMessageBuffer = make(map[int]PubSubMessage) // Buffer to store unacknowledged messages
	bufferMutex                sync.Mutex                    // Mutex to protect access to the buffer
	serverQueueMutex           sync.Mutex
	ClientMsgNumber            int = -1 // Starts with 0. Identifies the current message number for the user. Also used on server acks to identify the acked message
	LastAckedClientMsgNumber   int = -1
	QueueMsgNumber             int = -1 //Unknown queue start location at the beginning
	resendTimeout                  = 1 * time.Second
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

	if err := sendNewPubSubMessage(conn, subscribeMsg); err != nil {
		log.Println("Failed to subscribe:", err)
		return
	}

	// async check buffer for unacked messages
	go resendUnackedMessages(conn)

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
		bufferMutex.Lock()
		ClientMsgNumber++
		bufferMutex.Unlock()
		// Construct and send a message.
		msg := PubSubMessage{
			Action:          "publish", // Replace with appropriate action
			Topic:           topic,     // Replace with user-specified or default topic
			Data:            text,      // User input
			ClientMessageNo: ClientMsgNumber,
		}
		err := sendNewPubSubMessage(conn, msg)
		if err != nil {
			log.Println("Failed to send message:", err)
		}
	}

}

// sendMessage encodes and sends a JSON message to the server.
func sendNewPubSubMessage(conn *websocket.Conn, msg PubSubMessage) error {

	bufferMutex.Lock()
	UnackedClientMessageBuffer[ClientMsgNumber] = msg
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
		DecodeMessage(message, conn)
	}
}

func DecodeMessage(msg []byte, conn *websocket.Conn) {
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
		var ack FromServerAckMessage
		err = json.Unmarshal(msg, &ack)
		if err != nil {
			log.Println("Failed to decode AckMessage:", err)
			return
		}
		fmt.Println("Acknowledgment received:", ack.ClientMessageNo)
		go handleServerAck(ack.ClientMessageNo)
	} else if _, exists := genericMessage["QueueMessageNo"]; exists {
		// Handle new Queue message
		var queueMsg ServerMessage
		err = json.Unmarshal(msg, &queueMsg)
		if err != nil {
			log.Println("Failed to decode ServerMessage:", err)
			return
		}
		fmt.Println("Message received:", queueMsg.Data)
		go handleServerMessage(queueMsg, conn)

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

func handleServerMessage(msg ServerMessage, conn *websocket.Conn) {
	serverQueueMutex.Lock()
	if QueueMsgNumber == -1 || msg.QueueMessageNo == 1+QueueMsgNumber {
		QueueMsgNumber = msg.QueueMessageNo
		// print on the terminal and send server the ACK
		fmt.Printf("QueueMessageNo %d received: %s\n", msg.QueueMessageNo, msg.Data)

		// Send acknowledgment for the received message
		ack := ToServerAckMessage{
			QueueMessageNo: msg.QueueMessageNo, // Use the queue number for acknowledgment
		}
		ackJSON, err := json.Marshal(ack)
		if err != nil {
			log.Printf("Failed to encode acknowledgment for QueueMessageNo %d: %v\n", msg.QueueMessageNo, err)
			return
		}

		// Use the WebSocket connection to send the acknowledgment
		if err := conn.WriteMessage(websocket.TextMessage, ackJSON); err != nil {
			log.Printf("Failed to send acknowledgment for QueueMessageNo %d: %v\n", msg.QueueMessageNo, err)
		} else {
			fmt.Printf("Acknowledgment sent for QueueMessageNo %d\n", msg.QueueMessageNo)
		}

	} else if QueueMsgNumber != -1 && msg.QueueMessageNo <= QueueMsgNumber {
		// Duplicate or already processed message, resend acknowledgment
		log.Printf("Duplicate or already processed message: QueueMessageNo %d\n", msg.QueueMessageNo)
		ack := ToServerAckMessage{
			QueueMessageNo: msg.QueueMessageNo, // Use the queue number for acknowledgment
		}
		ackJSON, err := json.Marshal(ack)
		if err != nil {
			log.Printf("Failed to encode acknowledgment for duplicate QueueMessageNo %d: %v\n", msg.QueueMessageNo, err)
			return
		}

		// Resend acknowledgment
		if err := conn.WriteMessage(websocket.TextMessage, ackJSON); err != nil {
			log.Printf("Failed to resend acknowledgment for QueueMessageNo %d: %v\n", msg.QueueMessageNo, err)
		} else {
			fmt.Printf("Resent acknowledgment for QueueMessageNo %d\n", msg.QueueMessageNo)
		}
	} else {
		log.Printf("Out-of-order message received: QueueMessageNo %d (expected %d)\n", msg.QueueMessageNo, QueueMsgNumber+1)
	}
	serverQueueMutex.Unlock()
}

// resendUnackedMessages periodically resends unacknowledged messages
func resendUnackedMessages(conn *websocket.Conn) {
	for {
		time.Sleep(resendTimeout) // Wait for the specified timeout

		bufferMutex.Lock()
		if LastAckedClientMsgNumber == ClientMsgNumber {
			bufferMutex.Unlock()
			return
		}
		nextMsg := UnackedClientMessageBuffer[LastAckedClientMsgNumber+1]
		log.Printf("Resending unacknowledged message: %+v\n", nextMsg)
		messageJSON, err := json.Marshal(nextMsg)
		if err != nil {
			fmt.Printf("failed to encode message: %v\n", err)
		}
		conn.WriteMessage(websocket.TextMessage, messageJSON)
		bufferMutex.Unlock()
	}
}
