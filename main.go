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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Message structure for communication with the server
type BaseMessage struct {
	Action string `json:"action"`
	Topic  string `json:"topic"`
}

type PublishMessage struct {
	BaseMessage
	Data            string `json:"data"`
	ClientMessageNo int    `json:"clientMessageNo"`
}

type SubscribeMessage struct {
	BaseMessage
}

type ToServerAckMessage struct {
	BaseMessage
	QueueMessageNo int `json:"queueMessageNo"`
}

type FromServerAckMessage struct {
	BaseMessage
	ClientMessageNo int `json:"clientMessageNo"`
}

type ServerSendMessage struct {
	BaseMessage
	QueueMessageNo int    `json:"queueMessageNo"`
	Data           string `json:"data"`
}

var (
	UnackedClientMessageBuffer = make(map[int]PublishMessage) // Buffer to store unacknowledged messages
	bufferMutex                sync.Mutex                     // Mutex to protect access to the buffer
	serverQueueMutex           sync.Mutex
	ClientMsgNumber            int = 0 // Starts with 0. Identifies the current message number for the user. Also used on server acks to identify the acked message
	LastAckedClientMsgNumber   int = 0
	QueueMsgNumber             int = -1 //Unknown queue start location at the beginning
	resendTimeout                  = 2 * time.Second
	topic                      string
	logger                     *zap.Logger
	pid                        int
)

func main() {

	flag.StringVar(&topic, "topic", "", "Topic to subscribe to")
	flag.IntVar(&pid, "pid", 0, "Process ID for logging purposes")
	flag.Parse()

	if topic == "" {
		logger.Fatal("Error: Please specify a valid topic using the -topic flag.")
		os.Exit(1)
	}
	if pid == 0 {
		logger.Fatal("Error: Please specify a valid topic using the -topic flag.")
		os.Exit(1)
	}

	initLogger()
	// Connect to the WebSocket server.
	serverAddr := "ws://localhost:8082/ws"
	conn, _, err := websocket.DefaultDialer.Dial(serverAddr, nil)
	if err != nil {
		logger.Fatal("Failed to connect to server", zap.String("address", serverAddr), zap.Error(err))
	}
	defer conn.Close()

	logger.Info("Connected to server", zap.String("address", serverAddr))

	// start receiving messages
	go listenForServer(conn)

	subscribeMsg := SubscribeMessage{
		BaseMessage: BaseMessage{
			Action: "subscribe",
			Topic:  topic,
		},
	}

	if err := sendSubscribeMessage(conn, subscribeMsg); err != nil {
		logger.Error("Failed to subscribe", zap.Error(err))
		return
	}

	// async check buffer for unacked messages
	go resendUnackedMessages(conn)

	// Read user input in a loop and send it to the server.
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("")
		if !scanner.Scan() {
			logger.Error("Error reading input", zap.Error(scanner.Err()))
			break
		}

		text := scanner.Text()
		if text == "exit" {
			logger.Info("Exiting...")
			break
		}
		bufferMutex.Lock()
		ClientMsgNumber++
		bufferMutex.Unlock()
		// Construct and send a message.
		msg := PublishMessage{
			BaseMessage: BaseMessage{
				Action: "publish", // Replace with appropriate action
				Topic:  topic,     // Replace with user-specified or default topic
			},
			Data:            text, // User input
			ClientMessageNo: ClientMsgNumber,
		}
		err := sendNewPublishMessage(conn, msg)
		if err != nil {
			logger.Error("Failed to send message", zap.Error(err))
		}
	}

}

func initLogger() {
	// Define log file path
	logFilePath := fmt.Sprintf("client_logs_pid_%d.log", pid)

	// Create a file writer
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// Create a Zap core that writes to the file
	writeSyncer := zapcore.AddSync(file)
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig), // Use JSON encoder for structured logging
		writeSyncer,
		zap.InfoLevel, // Set log level to Info or higher
	)

	// Add process ID as a default field in all logs
	logger = zap.New(core).With(zap.Int("pid", pid))

	defer logger.Sync()
}

// sendMessage encodes and sends a JSON message to the server.
func sendSubscribeMessage(conn *websocket.Conn, msg SubscribeMessage) error {
	messageJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	return conn.WriteMessage(websocket.TextMessage, messageJSON)
}

func sendNewPublishMessage(conn *websocket.Conn, msg PublishMessage) error {

	bufferMutex.Lock()
	UnackedClientMessageBuffer[ClientMsgNumber] = msg
	bufferMutex.Unlock()

	messageJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	logger.Info("Sending message to server",
		zap.ByteString("rawMessage", []byte(msg.Data)),
	)
	return conn.WriteMessage(websocket.TextMessage, messageJSON)
}

func listenForServer(conn *websocket.Conn) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			logger.Error("Read error from server",
				zap.Error(err),
			)
			return
		}
		logger.Info("Message received from server",
			zap.ByteString("rawMessage", message),
		)
		// fmt.Println("Received:", string(message))
		DecodeMessage(message, conn)
	}
}

func DecodeMessage(msg []byte, conn *websocket.Conn) {
	// Decode into a generic map to inspect fields
	var genericMessage map[string]interface{}
	err := json.Unmarshal(msg, &genericMessage) // Ensure err is declared
	if err != nil {
		logger.Error("Failed to decode message", zap.Error(err), zap.ByteString("rawMessage", msg))
		return
	}

	// Check message type based on fields
	if _, exists := genericMessage["clientMessageNo"]; exists {
		// Handle AckMessage
		var ack FromServerAckMessage
		err = json.Unmarshal(msg, &ack)
		if err != nil {
			logger.Error("Failed to decode AckMessage", zap.Error(err))
			return
		}
		logger.Info("Acknowledgment received", zap.Int("clientMessageNo", ack.ClientMessageNo))
		go handleServerAck(ack)
	} else if _, exists := genericMessage["data"]; exists {
		// Handle new Send Message
		var queueMsg ServerSendMessage
		err = json.Unmarshal(msg, &queueMsg)
		if err != nil {
			logger.Error("Failed to decode ServerMessage", zap.Error(err))
			return
		}
		go handleServerSend(queueMsg, conn)

	} else {
		// Handle unexpected or unknown message formats
		fmt.Println("Unknown message format received:", string(msg))
	}
}

func handleServerAck(msgAck FromServerAckMessage) {
	// Lock the buffer to ensure safe concurrent access
	// server must ack the next unacked message
	bufferMutex.Lock()
	defer bufferMutex.Unlock()
	msgAckNo := msgAck.ClientMessageNo
	// IGNORING FIELDS "TOPIC" AND "ACTION" FOR NOW
	if msgAckNo != LastAckedClientMsgNumber+1 {
		logger.Warn("Out-of-order acknowledgment received",
			zap.Int("expected", LastAckedClientMsgNumber+1),
			zap.Int("received", msgAck.ClientMessageNo),
		)
		return
	}
	// Check if the message with the given AckNo exists in the buffer
	if msg, exists := UnackedClientMessageBuffer[msgAckNo]; exists {
		// Remove the acknowledged message from the buffer
		delete(UnackedClientMessageBuffer, msgAckNo)
		LastAckedClientMsgNumber++
		logger.Info("Acknowledged and removed message from buffer",
			zap.Int("ackNo", msgAck.ClientMessageNo),
			zap.Any("message", msg),
		)
	} else {
		// Log a warning if the acknowledgment is for an unknown or already removed message
		logger.Warn("Acknowledgment for unknown or already removed message",
			zap.Int("ackNo", msgAck.ClientMessageNo),
		)
	}
}

func handleServerSend(msg ServerSendMessage, conn *websocket.Conn) {
	// FOR NOW, IGNORING MSG FIELDS "TOPIC" AND "ACTION"
	serverQueueMutex.Lock()
	if QueueMsgNumber == -1 || msg.QueueMessageNo == 1+QueueMsgNumber {
		QueueMsgNumber = msg.QueueMessageNo
		fmt.Println("[ Topic -", msg.BaseMessage.Topic, "]: ", msg.Data)
		// Send acknowledgment for the received message
		ack := ToServerAckMessage{
			BaseMessage: BaseMessage{
				Topic:  topic,
				Action: "ack",
			},
			QueueMessageNo: msg.QueueMessageNo, // Use the queue number for acknowledgment
		}
		ackJSON, err := json.Marshal(ack)
		if err != nil {
			logger.Error("Failed to encode acknowledgment", zap.Int("queueMessageNo", msg.QueueMessageNo), zap.Error(err))
			return
		}

		// Use the WebSocket connection to send the acknowledgment
		if err := conn.WriteMessage(websocket.TextMessage, ackJSON); err != nil {
			logger.Error("Failed to send acknowledgment", zap.Int("queueMessageNo", msg.QueueMessageNo), zap.Error(err))
		} else {
			logger.Info("Acknowledgment sent", zap.Int("queueMessageNo", msg.QueueMessageNo))
		}

	} else if QueueMsgNumber != -1 && msg.QueueMessageNo <= QueueMsgNumber {
		// Duplicate or already processed message, resend acknowledgment
		logger.Warn("Duplicate or already processed message received",
			zap.Int("queueMessageNo", msg.QueueMessageNo),
		)
		ack := ToServerAckMessage{
			BaseMessage: BaseMessage{
				Topic:  topic,
				Action: "ack",
			},
			QueueMessageNo: msg.QueueMessageNo, // Use the queue number for acknowledgment
		}
		ackJSON, err := json.Marshal(ack)
		if err != nil {
			logger.Error("Failed to encode acknowledgment for duplicate message",
				zap.Int("queueMessageNo", msg.QueueMessageNo),
				zap.Error(err),
			)
			return
		}

		// Resend acknowledgment
		if err := conn.WriteMessage(websocket.TextMessage, ackJSON); err != nil {
			logger.Error("Failed to resend acknowledgment for duplicate message",
				zap.Int("queueMessageNo", msg.QueueMessageNo),
				zap.Error(err),
			)
		} else {
			logger.Info("Resent acknowledgment for duplicate message",
				zap.Int("queueMessageNo", msg.QueueMessageNo),
			)
		}
	} else {
		logger.Warn("Out-of-order message received",
			zap.Int("received", msg.QueueMessageNo),
			zap.Int("expected", QueueMsgNumber+1),
		)
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
			continue
		}
		nextMsg, exists := UnackedClientMessageBuffer[LastAckedClientMsgNumber+1]
		if !exists {
			logger.Warn("Next unacknowledged message not found in buffer",
				zap.Int("expectedMessageNo", LastAckedClientMsgNumber+1),
			)
			bufferMutex.Unlock()
			continue
		}

		logger.Info("Resending unacknowledged message",
			zap.Int("clientMessageNo", nextMsg.ClientMessageNo),
			zap.String("topic", nextMsg.Topic),
			zap.String("data", nextMsg.Data),
		)

		messageJSON, err := json.Marshal(nextMsg)
		if err != nil {
			logger.Error("Failed to encode unacknowledged message",
				zap.Int("clientMessageNo", nextMsg.ClientMessageNo),
				zap.Error(err),
			)
		}
		// Send the message
		if err := conn.WriteMessage(websocket.TextMessage, messageJSON); err != nil {
			logger.Error("Failed to resend unacknowledged message",
				zap.Int("clientMessageNo", nextMsg.ClientMessageNo),
				zap.Error(err),
			)
		} else {
			logger.Info("Successfully resent unacknowledged message",
				zap.Int("clientMessageNo", nextMsg.ClientMessageNo),
			)
		}
		bufferMutex.Unlock()
	}
}
