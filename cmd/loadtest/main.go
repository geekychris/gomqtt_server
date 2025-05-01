package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	
	"github.com/chris/gomqtt/pkg/mqtt"
)

// TestClient represents a test client for load testing
type TestClient struct {
	ID              string
	Username        string
	Conn            net.Conn
	ServerAddr      string
	Room            string
	Connected       bool
	MessagesSent    int
	MessagesReceived map[string]bool // message ID -> received
	ClientID        string
	lock            sync.RWMutex
	wg              sync.WaitGroup
	closeChan       chan struct{}
}

// NewTestClient creates a new test client with the given ID and configuration
func NewTestClient(id string, serverAddr, room string) *TestClient {
	return &TestClient{
		ID:              id,
		Username:        fmt.Sprintf("test-user-%s", id),
		ServerAddr:      serverAddr,
		Room:            room,
		Connected:       false,
		MessagesSent:    0,
		MessagesReceived: make(map[string]bool),
		closeChan:       make(chan struct{}),
	}
}

// Connect establishes a connection to the MQTT server
func (c *TestClient) Connect() error {
	// Try to connect with retries
	var conn net.Conn
	var err error
	
	// Attempt connection with retries
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, err = net.Dial("tcp", c.ServerAddr)
		if err == nil {
			break
		}
		
		if attempt < maxRetries {
			// Exponential backoff
			backoffTime := time.Duration(attempt*100) * time.Millisecond
			time.Sleep(backoffTime)
		}
	}
	
	if err != nil {
		return fmt.Errorf("client %s failed to connect after %d attempts: %w", c.ID, maxRetries, err)
	}
	c.Conn = conn
	c.Connected = true

	// Send CONNECT message
	connectMsg := mqtt.MQTTMessage{
		Type: "CONNECT",
	}

	if err := c.sendMessage(connectMsg); err != nil {
		c.Conn.Close()
		c.Connected = false
		return fmt.Errorf("client %s failed to send CONNECT message: %w", c.ID, err)
	}

	// Start message reading goroutine
	c.wg.Add(1)
	go c.readLoop()

	// Wait for CONNACK
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			c.Disconnect()
			return fmt.Errorf("client %s connection timeout", c.ID)
		case <-ticker.C:
			if c.ClientID != "" {
				return nil
			}
		}
	}
}

// JoinRoom subscribes the client to a chat room
func (c *TestClient) JoinRoom() error {
	if !c.Connected {
		return fmt.Errorf("client %s not connected", c.ID)
	}

	// Send SUBSCRIBE message
	subscribeMsg := mqtt.MQTTMessage{
		Type:  "SUBSCRIBE",
		Topic: c.Room,
	}

	if err := c.sendMessage(subscribeMsg); err != nil {
		return fmt.Errorf("client %s failed to join room: %w", c.ID, err)
	}

	// Wait for SUBACK
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("client %s subscription timeout", c.ID)
		case <-ticker.C:
			c.lock.RLock()
			suback := false
			// Check if we've received a SUBACK message
			for key := range c.MessagesReceived {
				if strings.Contains(key, "SUBACK:"+c.Room) {
					suback = true
					break
				}
			}
			c.lock.RUnlock()
			if suback {
				return nil
			}
		}
	}
}

// SendMessage sends a chat message to the room
func (c *TestClient) SendMessage(messageText string) error {
	if !c.Connected {
		return fmt.Errorf("client %s not connected", c.ID)
	}

	// Create a unique message ID for tracking
	msgID := fmt.Sprintf("%s-%d", c.ID, c.MessagesSent)
	
	// Format includes message ID for tracking
	formattedMessage := fmt.Sprintf("[%s] %s: %s", msgID, c.Username, messageText)
	
	// Prepare actual message
	publishMsg := mqtt.MQTTMessage{
		Type:    "PUBLISH",
		Topic:   c.Room,
		Payload: formattedMessage,
	}

	// Send the message
	if err := c.sendMessage(publishMsg); err != nil {
		return fmt.Errorf("client %s failed to send message: %w", c.ID, err)
	}

	c.lock.Lock()
	c.MessagesSent++
	c.lock.Unlock()

	return nil
}

// GetMessagesSent returns the number of messages sent by this client
func (c *TestClient) GetMessagesSent() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.MessagesSent
}

// GetMessagesReceived returns the number of messages received by this client
func (c *TestClient) GetMessagesReceived() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.MessagesReceived)
}

// CheckDelivery checks if a message with the given ID was received
func (c *TestClient) CheckDelivery(msgID string) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.MessagesReceived[msgID]
}

// Disconnect gracefully disconnects from the server
func (c *TestClient) Disconnect() {
	if !c.Connected {
		return
	}

	// Send DISCONNECT message if connection is still active
	disconnectMsg := mqtt.MQTTMessage{
		Type: "DISCONNECT",
	}
	c.sendMessage(disconnectMsg)

	// Signal reader to stop
	close(c.closeChan)

	// Close connection
	c.Conn.Close()
	c.Connected = false

	// Wait for goroutines to complete
	c.wg.Wait()
}

// readLoop continuously reads messages from the server
func (c *TestClient) readLoop() {
	defer c.wg.Done()
	reader := bufio.NewReader(c.Conn)

	for {
		select {
		case <-c.closeChan:
			return
		default:
			// Set read deadline to detect stale connections
			c.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))

			line, err := reader.ReadString('\n')
			if err != nil {
				// Connection closed or error
				return
			}

			var msg mqtt.MQTTMessage
			if err := json.Unmarshal([]byte(line), &msg); err != nil {
				continue
			}

			// Process the message
			c.processMessage(msg)
		}
	}
}

// processMessage handles received messages based on type
func (c *TestClient) processMessage(msg mqtt.MQTTMessage) {
	switch msg.Type {
	case "CONNACK":
		c.ClientID = msg.ClientID
	case "SUBACK":
		c.lock.Lock()
		c.MessagesReceived["SUBACK:"+msg.Topic] = true
		c.lock.Unlock()
	case "PUBLISH":
		if msg.ClientID == c.ClientID {
			// Skip our own messages
			return
		}

		// Track that we received a message
		c.lock.Lock()
		// Extract message ID from payload if it exists
		payload := msg.Payload
		if strings.HasPrefix(payload, "[") {
			parts := strings.SplitN(payload, "]", 2)
			if len(parts) == 2 {
				msgID := strings.TrimPrefix(parts[0], "[")
				c.MessagesReceived[msgID] = true
			} else {
				// Generic tracking if ID isn't found
				c.MessagesReceived[fmt.Sprintf("msg:%d", len(c.MessagesReceived))] = true
			}
		} else {
			// Generic tracking if ID isn't found
			c.MessagesReceived[fmt.Sprintf("msg:%d", len(c.MessagesReceived))] = true
		}
		c.lock.Unlock()
	}
}

// sendMessage serializes and sends a message to the server
func (c *TestClient) sendMessage(msg mqtt.MQTTMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Add newline delimiter
	data = append(data, '\n')

	_, err = c.Conn.Write(data)
	return err
}

// TestResults holds results for the load test
type TestResults struct {
	ClientCount          int
	RoomCount            int
	Duration             time.Duration
	TotalMessagesSent    int
	TotalMessagesReceived int
	DeliveryRate         float64
	ClientsConnected     int
	MessageRatePerSec    float64
}

func main() {
	// Parse command line arguments
	numClients := flag.Int("clients", 10, "Number of clients to simulate")
	numRooms := flag.Int("rooms", 3, "Number of chat rooms to use")
	testDuration := flag.Int("duration", 30, "Test duration in seconds")
	sendInterval := flag.Int("interval", 1000, "Message send interval in milliseconds")
	serverAddr := flag.String("server", "localhost:1883", "MQTT server address")
	verbose := flag.Bool("verbose", false, "Enable verbose output")
	flag.Parse()

	fmt.Printf("Starting MQTT chat load test with %d clients across %d rooms\n", *numClients, *numRooms)
	fmt.Printf("Test will run for %d seconds with messages sent every %d ms\n", *testDuration, *sendInterval)
	fmt.Printf("Server address: %s\n", *serverAddr)

	// Generate room names
	rooms := make([]string, *numRooms)
	for i := 0; i < *numRooms; i++ {
		rooms[i] = fmt.Sprintf("chat/room%d", i+1)
	}

	// Seed random generator
	rand.Seed(time.Now().UnixNano())
	
	// Create test clients
	clients := make([]*TestClient, *numClients)
	for i := 0; i < *numClients; i++ {
		// Assign client to a room round-robin
		roomIndex := i % *numRooms
		clientID := fmt.Sprintf("%d", i)
		clients[i] = NewTestClient(clientID, *serverAddr, rooms[roomIndex])
	}

	// Connect clients to the server
	var wg sync.WaitGroup
	clientsConnected := 0
	clientsMutex := sync.Mutex{}

	for i, client := range clients {
		wg.Add(1)
		go func(idx int, c *TestClient) {
			defer wg.Done()

			// Connect client to server
			if err := c.Connect(); err != nil {
				if *verbose {
					fmt.Printf("Client %s failed to connect: %v\n", c.ID, err)
				}
				return
			}

			// Join assigned room
			if err := c.JoinRoom(); err != nil {
				if *verbose {
					fmt.Printf("Client %s failed to join room: %v\n", c.ID, err)
				}
				c.Disconnect()
				return
			}

			clientsMutex.Lock()
			clientsConnected++
			clientsMutex.Unlock()

			if *verbose {
				fmt.Printf("Client %s connected and joined room %s\n", c.ID, c.Room)
			}
		}(i, client)
	}

	// Wait for all connection attempts to complete
	wg.Wait()

	fmt.Printf("Connected %d/%d clients successfully\n", clientsConnected, *numClients)

	if clientsConnected == 0 {
		fmt.Println("No clients connected. Exiting.")
		return
	}

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create test done channel
	testDone := make(chan struct{})
	
	// Generate random messages in each room
	startTime := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Track all messages for delivery verification
		msgTracker := make(map[string]time.Time)
		var msgMutex sync.Mutex

		// Create a message generator per room
		ticker := time.NewTicker(time.Duration(*sendInterval) * time.Millisecond)
		defer ticker.Stop()
		
		// Sample messages for chat
		chatMessages := []string{
			"Hello everyone!",
			"How are you doing today?",
			"This is a test message",
			"MQTT chat is working great",
			"Let's test the performance",
			"Can everyone see this message?",
			"Testing multi-client chat",
			"Message from a test client",
			"Hope this reaches everyone",
			"Final test message",
		}

		// Run the test for the specified duration
		testTimeout := time.After(time.Duration(*testDuration) * time.Second)

		for {
			select {
			case <-testTimeout:
				// Test duration reached
				close(testDone)
				return
			case <-sigChan:
				// Interrupted by user
				fmt.Println("\nTest interrupted by user")
				close(testDone)
				return
			case <-ticker.C:
				// Select a random client to send a message
				clientIdx := rand.Intn(len(clients))
				client := clients[clientIdx]

				if !client.Connected {
					continue
				}

				// Select a random message
				msgText := chatMessages[rand.Intn(len(chatMessages))]

				// Create message ID for tracking
				msgID := fmt.Sprintf("%s-%d", client.ID, client.GetMessagesSent())

				// Send the message
				if err := client.SendMessage(msgText); err != nil {
					if *verbose {
						fmt.Printf("Failed to send message from client %s: %v\n", client.ID, err)
					}
					continue
				}

				// Track the message
				msgMutex.Lock()
				msgTracker[msgID] = time.Now()
				msgMutex.Unlock()

				if *verbose {
					fmt.Printf("Client %s sent message to room %s: %s\n", client.ID, client.Room, msgText)
				}
			}
		}
	}()

	// Wait for test to complete
	<-testDone

	// Allow a short grace period for any in-flight messages
	time.Sleep(1 * time.Second)

	// Calculate statistics
	results := calculateResults(clients, *numClients, *numRooms, startTime, time.Now(), clientsConnected)

	// Print results
	printResults(results, rooms, clients)

	// Clean up: Disconnect all clients
	fmt.Println("Disconnecting clients...")
	for _, client := range clients {
		if client.Connected {
			client.Disconnect()
		}
	}

	fmt.Println("Test completed.")
}

// calculateResults computes statistics from the test
func calculateResults(clients []*TestClient, numClients, numRooms int, startTime, endTime time.Time, connectedClients int) *TestResults {
	duration := endTime.Sub(startTime)
	
	// Calculate total messages sent/received
	totalSent := 0
	totalReceived := 0
	messageIDs := make(map[string]bool)
	
	// Track messages by room
	roomMessages := make(map[string]int)
	roomDeliveredCount := make(map[string]int)
	
	for _, client := range clients {
		if !client.Connected {
			continue
		}
		
		sent := client.GetMessagesSent()
		totalSent += sent
		
		// Track messages by room
		roomMessages[client.Room] += sent
		
		// Count messages received per room
		received := client.GetMessagesReceived()
		totalReceived += received
		roomDeliveredCount[client.Room] += received
		
		// Collect all message IDs that should have been received
		for i := 0; i < sent; i++ {
			msgID := fmt.Sprintf("%s-%d", client.ID, i)
			messageIDs[msgID] = true
		}
	}
	
	// Check message delivery rates
	correctlyDelivered := 0
	
	// For each message ID that was sent
	for msgID := range messageIDs {
		parts := strings.Split(msgID, "-")
		if len(parts) != 2 {
			continue
		}
		
		senderID := parts[0]
		
		// Find sender's room
		var senderRoom string
		for _, client := range clients {
			if client.ID == senderID {
				senderRoom = client.Room
				break
			}
		}
		
		if senderRoom == "" {
			continue
		}
		
		// Count clients who should have received this message
		totalReceivers := 0
		receivedCount := 0
		
		for _, client := range clients {
			if !client.Connected || client.Room != senderRoom || client.ID == senderID {
				continue
			}
			
			totalReceivers++
			if client.CheckDelivery(msgID) {
				receivedCount++
			}
		}
		
		// If more than 80% of clients received the message, count it as correctly delivered
		if totalReceivers > 0 && float64(receivedCount)/float64(totalReceivers) >= 0.8 {
			correctlyDelivered++
		}
	}
	
	// Avoid division by zero
	deliveryRate := 0.0
	if len(messageIDs) > 0 {
		deliveryRate = float64(correctlyDelivered) / float64(len(messageIDs)) * 100
	}
	
	messageRate := 0.0
	if duration.Seconds() > 0 {
		messageRate = float64(totalSent) / duration.Seconds()
	}
	
	return &TestResults{
		ClientCount:          numClients,
		RoomCount:            numRooms,
		Duration:             duration,
		TotalMessagesSent:    totalSent,
		TotalMessagesReceived: totalReceived,
		DeliveryRate:         deliveryRate,
		ClientsConnected:     connectedClients,
		MessageRatePerSec:    messageRate,
	}
}

// printResults displays the test results
func printResults(results *TestResults, rooms []string, clients []*TestClient) {
	fmt.Println("\n---------------------------------------------")
	fmt.Println("MQTT CHAT LOAD TEST RESULTS")
	fmt.Println("---------------------------------------------")
	fmt.Printf("Clients: %d/%d connected\n", results.ClientsConnected, results.ClientCount)
	fmt.Printf("Rooms: %d\n", results.RoomCount)
	fmt.Printf("Duration: %.2f seconds\n", results.Duration.Seconds())
	fmt.Printf("Total messages sent: %d\n", results.TotalMessagesSent)
	fmt.Printf("Message rate: %.2f messages/second\n", results.MessageRatePerSec)
	fmt.Printf("Message delivery rate: %.2f%%\n", results.DeliveryRate)
	fmt.Println("---------------------------------------------")
	
	// Room-specific statistics
	fmt.Println("\nPer-Room Statistics:")
	fmt.Println("---------------------------------------------")
	
	// Build room statistics
	type RoomStats struct {
		clientCount   int
		messagesSent  int
		messagesRecv  int
		deliveryRate  float64
	}
	
	roomStats := make(map[string]*RoomStats)
	
	// Initialize stats for all rooms
	for _, room := range rooms {
		roomStats[room] = &RoomStats{}
	}
	
	// Collect per-room statistics
	for _, client := range clients {
		if !client.Connected {
			continue
		}
		
		stats, exists := roomStats[client.Room]
		if !exists {
			continue // Skip if room not in our list
		}
		
		stats.clientCount++
		stats.messagesSent += client.GetMessagesSent()
		stats.messagesRecv += client.GetMessagesReceived()
	}
	
	// Calculate room-specific delivery rates
	for room, stats := range roomStats {
		// Calculate expected messages per client
		expectedPerClient := 0
		if stats.clientCount > 1 { // Need at least 2 clients for delivery
			// Each message should be received by (clientCount - 1) clients
			// Total expected deliveries = messagesSent * (clientCount - 1)
			expectedPerClient = stats.messagesSent * (stats.clientCount - 1)
		}
		
		// Calculate delivery rate
		if expectedPerClient > 0 {
			stats.deliveryRate = float64(stats.messagesRecv) / float64(expectedPerClient) * 100
		}
		
		fmt.Printf("Room: %s\n", room)
		fmt.Printf("  Clients: %d\n", stats.clientCount)
		fmt.Printf("  Messages sent: %d\n", stats.messagesSent)
		fmt.Printf("  Messages received: %d\n", stats.messagesRecv)
		fmt.Printf("  Delivery rate: %.2f%%\n", stats.deliveryRate)
		fmt.Println()
	}
	
	fmt.Println("---------------------------------------------")
	
	// General assessment
	fmt.Println("\nPerformance Assessment:")
	fmt.Println("---------------------------------------------")
	
	// Additional stats for debugging
	fmt.Printf("Total messages received across all clients: %d\n", results.TotalMessagesReceived)
	
	// Assess delivery rate
	if results.DeliveryRate >= 95 {
		fmt.Println("Excellent message delivery rate (>95%)")
	} else if results.DeliveryRate >= 80 {
		fmt.Println("Good message delivery rate (80-95%)")
	} else {
		fmt.Println("Poor message delivery rate (<80%)")
	}
	
	if results.MessageRatePerSec >= 100 {
		fmt.Println("High throughput (>100 msgs/sec)")
	} else if results.MessageRatePerSec >= 50 {
		fmt.Println("Good throughput (50-100 msgs/sec)")
	} else {
		fmt.Println("Low throughput (<50 msgs/sec)")
	}
	
	fmt.Println("---------------------------------------------")
}
