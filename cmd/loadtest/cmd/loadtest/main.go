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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	
	"github.com/chris/gomqtt/pkg/mqtt"
)

// TestResults holds results for the load test
type TestResults struct {
	ClientCount           int
	RoomCount             int
	Duration              time.Duration
	TotalMessagesSent     int
	TotalMessagesReceived int
	DeliveryRate          float64
	ClientsConnected      int
	MessageRatePerSec     float64
	// High throughput metrics
	AverageLatency        time.Duration
	MaxLatency            time.Duration
	MinLatency            time.Duration
	MessageSizeBytes      int
	TotalBytesSent        int64
	SuccessRate           float64
}

// TestClient represents a test client for load testing
type TestClient struct {
	ID               string
	Username         string
	Conn             net.Conn
	ServerAddr       string
	Room             string
	Connected        bool
	MessagesSent     int
	MessagesReceived map[string]bool // message ID -> received
	ClientID         string
	lock             sync.RWMutex
	wg               sync.WaitGroup
	closeChan        chan struct{}
	// For high-throughput testing
	MessageTimes     map[string]time.Time // Message ID -> receive time
	SendTimes        map[string]time.Time // Message ID -> send time
	BytesSent        int64                // Total bytes sent
}

// NewTestClient creates a new test client with the given ID and configuration
func NewTestClient(id string, serverAddr, room string) *TestClient {
	return &TestClient{
		ID:               id,
		Username:         fmt.Sprintf("test-user-%s", id),
		ServerAddr:       serverAddr,
		Room:             room,
		Connected:        false,
		MessagesSent:     0,
		MessagesReceived: make(map[string]bool),
		MessageTimes:     make(map[string]time.Time),
		SendTimes:        make(map[string]time.Time),
		BytesSent:        0,
		closeChan:        make(chan struct{}),
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

	// Record send time for latency tracking
	sendTime := time.Now()
	
	// Send the message
	if err := c.sendMessage(publishMsg); err != nil {
		return fmt.Errorf("client %s failed to send message: %w", c.ID, err)
	}

	c.lock.Lock()
	// Store send time for latency calculation
	c.SendTimes[msgID] = sendTime
	
	// Track bytes sent for throughput calculation
	msgSize := len(messageText) + estimateOverhead(msgID, messageText)
	c.BytesSent += int64(msgSize)
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
		var msgID string
		
		if strings.HasPrefix(payload, "[") {
			parts := strings.SplitN(payload, "]", 2)
			if len(parts) == 2 {
				msgID = strings.TrimPrefix(parts[0], "[")
				c.MessagesReceived[msgID] = true
			} else {
				// Generic tracking if ID isn't found
				msgID = fmt.Sprintf("msg:%d", len(c.MessagesReceived))
				c.MessagesReceived[msgID] = true
			}
		} else {
			// Generic tracking if ID isn't found
			msgID = fmt.Sprintf("msg:%d", len(c.MessagesReceived))
			c.MessagesReceived[msgID] = true
		}
		
		// Store receive time for latency calculation
		c.MessageTimes[msgID] = time.Now()
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

// Helper functions

// generateFixedSizePayload creates a string of the specified size in bytes
func generateFixedSizePayload(size int) string {
	if size <= 0 {
		return ""
	}
	
	// Create payload with repeating characters to achieve desired size
	payload := make([]byte, size)
	for i := 0; i < size; i++ {
		payload[i] = byte('A' + (i % 26))
	}
	
	return string(payload)
}

// estimateOverhead estimates the additional bytes added by the protocol
func estimateOverhead(msgID string, payload string) int {
	// JSON overhead + protocol headers approximate
	return 50 + len(msgID)
}

// calculateLatencyPercentile calculates percentile latency value
func calculateLatencyPercentile(latencies []time.Duration, percentile float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	
	// Sort latencies
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	
	// Calculate index for percentile
	idx := int(float64(len(latencies)) * percentile / 100)
	if idx >= len(latencies) {
		idx = len(latencies) - 1
	}
	
	return latencies[idx]
}

// sendMessagesInBursts sends messages in bursts for high-throughput testing
func sendMessagesInBursts(clients []*TestClient, burstSize, burstInterval, messageSize, duration int) {
	burstCount := 0
	maxBursts := duration / burstInterval // Maximum number of bursts based on test duration
	
	// Create ticker for controlled sending
	ticker := time.NewTicker(time.Duration(burstInterval) * time.Second)
	defer ticker.Stop()
	
	// Create a fixed-size payload for consistent message size
	payload := generateFixedSizePayload(messageSize)
	
	// Send bursts until duration is reached
	for range ticker.C {
		burstCount++
		
		// Send a burst of messages from each client
		var wg sync.WaitGroup
		for _, client := range clients {
			if !client.Connected {
				continue
			}
			
			wg.Add(1)
			go func(c *TestClient) {
				defer wg.Done()
				for i := 0; i < burstSize; i++ {
					if err := c.SendMessage(payload); err != nil {
						fmt.Printf("Error sending message from client %s: %v\n", c.ID, err)
					}
					// Small sleep to prevent overwhelming the server
					time.Sleep(5 * time.Millisecond)
				}
			}(client)
		}
		
		// Wait for all clients to finish sending their burst
		wg.Wait()
		
		// Check if we've reached the test duration
		if burstCount >= maxBursts {
			break
		}
	}
}

// sendRegularMessages sends messages at a regular interval for standard testing
func sendRegularMessages(clients []*TestClient, messagesPerClient, sendInterval int, done chan struct{}) {
	// Calculate delay based on send interval
	delay := time.Duration(sendInterval) * time.Millisecond
	
	var wg sync.WaitGroup
	for _, client := range clients {
		if !client.Connected {
			continue
		}
		
		wg.Add(1)
		go func(c *TestClient) {
			defer wg.Done()
			for i := 0; i < messagesPerClient; i++ {
				select {
				case <-done:
					return
				default:
					message := fmt.Sprintf("Test message %d from %s", i, c.ID)
					if err := c.SendMessage(message); err != nil {
						fmt.Printf("Error sending message from client %s: %v\n", c.ID, err)
					}
					time.Sleep(delay)
				}
			}
		}(client)
	}
	wg.Wait()
	close(done)
}

// printResults prints detailed test results to the console
func printResults(results *TestResults, highThroughputMode bool) {
	fmt.Println("\n===== TEST RESULTS =====")
	fmt.Printf("Clients: %d, Rooms: %d, Duration: %.2f seconds\n", 
		results.ClientCount, results.RoomCount, results.Duration.Seconds())
	
	fmt.Printf("Total Messages Sent: %d\n", results.TotalMessagesSent)
	fmt.Printf("Total Messages Received: %d\n", results.TotalMessagesReceived)
	fmt.Printf("Message Rate: %.2f msgs/sec\n", results.MessageRatePerSec)
	fmt.Printf("Delivery Rate: %.2f%%\n", results.DeliveryRate)
	
	if highThroughputMode {
		fmt.Println("\n-- High Throughput Metrics --")
		fmt.Printf("Message Size: %d bytes\n", results.MessageSizeBytes)
		fmt.Printf("Total Data Sent: %.2f KB\n", float64(results.TotalBytesSent)/1024)
		fmt.Printf("Data Rate: %.2f KB/sec\n", float64(results.TotalBytesSent)/1024/results.Duration.Seconds())
		fmt.Printf("Success Rate: %.2f%%\n", results.SuccessRate)
		
		fmt.Println("\n-- Latency Metrics --")
		fmt.Printf("Average Latency: %v\n", results.AverageLatency)
		fmt.Printf("Min Latency: %v\n", results.MinLatency)
		fmt.Printf("Max Latency: %v\n", results.MaxLatency)
	}
	
	fmt.Println("========================")
}

func main() {
	// Basic test parameters
	serverAddr := flag.String("server", "localhost:1883", "MQTT server address")
	numClients := flag.Int("clients", 10, "Number of clients to create")
	numRooms := flag.Int("rooms", 3, "Number of rooms to distribute clients to")
	testDuration := flag.Int("duration", 30, "Test duration in seconds")
	messagesPerClient := flag.Int("messages", 10, "Number of messages each client sends")
	sendInterval := flag.Int("interval", 1000, "Send interval in milliseconds")
	verbose := flag.Bool("verbose", false, "Enable verbose output")

	// High-throughput test parameters
	highThroughputMode := flag.Bool("high-throughput", false, "Enable high-throughput mode")
	messageSize := flag.Int("message-size", 1024, "Message size in bytes")
	burstSize := flag.Int("burst-size", 10, "Messages per burst")
	burstInterval := flag.Int("burst-interval", 2, "Seconds between bursts")
	
	flag.Parse()
	fmt.Println("MQTT Load Test Client")
	fmt.Printf("Server: %s, Clients: %d, Rooms: %d\n", *serverAddr, *numClients, *numRooms)
	
	if highThroughputMode {
		fmt.Println("Running in high-throughput mode")
		fmt.Printf("Burst Size: %d, Burst Interval: %d sec, Message Size: %d bytes\n", 
			*burstSize, *burstInterval, *messageSize)
	} else {
		fmt.Printf("Messages per Client: %d, Send Interval: %d ms\n", 
			*messagesPerClient, *sendInterval)
	}
	
	// Create clients
	clients := make([]*TestClient, *numClients)
	for i := 0; i < *numClients; i++ {
		// Distribute clients evenly across rooms
		roomID := i % *numRooms
		roomName := fmt.Sprintf("room-%d", roomID)
		
		clients[i] = NewTestClient(fmt.Sprintf("client-%d", i), *serverAddr, roomName)
	}
	
	// Connect clients
	connectedClients := 0
	fmt.Println("Connecting clients...")
	var wg sync.WaitGroup
	
	for i, client := range clients {
		wg.Add(1)
		go func(c *TestClient, idx int) {
			defer wg.Done()
			if err := c.Connect(); err != nil {
				fmt.Printf("Failed to connect client %s: %v\n", c.ID, err)
				return
			}
			
			if err := c.JoinRoom(); err != nil {
				fmt.Printf("Failed to join room for client %s: %v\n", c.ID, err)
				c.Disconnect()
				return
			}
			
			// Successfully connected and joined
			connectedClients++
		}(client, i)
	}
	wg.Wait()
	
	fmt.Printf("Connected %d/%d clients\n", connectedClients, *numClients)
	if connectedClients == 0 {
		fmt.Println("No clients connected. Exiting.")
		return
	}
	
	// Set up graceful shutdown handling
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	
	done := make(chan struct{})
	
	// Start test duration timer in a goroutine
	go func() {
		select {
		case <-signalChan:
			fmt.Println("\nReceived interrupt signal. Stopping test...")
			close(done)
		case <-time.After(time.Duration(*testDuration) * time.Second):
			// Test duration exceeded
			fmt.Println("Test duration completed")
			close(done)
		}
	}()
	
	// Run the test
	fmt.Println("Starting test...")
	startTime := time.Now()
	
	// Different test modes
	if highThroughputMode {
		// High-throughput mode with burst sending
		sendMessagesInBursts(clients, *burstSize, *burstInterval, *messageSize, *testDuration)
	} else {
		// Regular mode with steady message rate
		sendDone := make(chan struct{})
		go sendRegularMessages(clients, *messagesPerClient, *sendInterval, sendDone)
		
		// Wait for sending to complete or for test to be stopped
		select {
		case <-done:
			// Test was stopped externally
		case <-sendDone:
			// All messages sent
		}
	}
	
	// Allow some time for messages to be delivered
	select {
	case <-done:
		// Already done
	default:
		// Give 5 more seconds for final message delivery
		fmt.Println("Waiting for final message delivery...")
		time.Sleep(5 * time.Second)
	}
	// Record test end time
	endTime := time.Now()
	
	// Calculate and print results
	results := calculateResults(clients, *numClients, *numRooms, startTime, endTime, connectedClients)
	printResults(results, highThroughputMode)
	
	// Clean up - disconnect all clients
	fmt.Println("Disconnecting clients...")
	for _, client := range clients {
		if client.Connected {
			client.Disconnect()
		}
	}
	
	fmt.Println("Test complete.")
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
		var sender *TestClient
		for _, client := range clients {
			if client.ID == senderID {
				senderRoom = client.Room
				sender = client
				break
			}
		}
		
		if senderRoom == "" || sender == nil {
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
	
	// Calculate additional high-throughput metrics
	var minLatency time.Duration = 24 * time.Hour // Initialize to a large value
	var maxLatency time.Duration
	var totalLatency time.Duration
	var latencyCount int
	var latencies []time.Duration
	var totalBytesSent int64
	
	// Calculate message latencies and byte counts
	for _, client := range clients {
		if !client.Connected {
			continue
		}
		
		client.lock.RLock()
		for msgID, receiveTime := range client.MessageTimes {
			if sendTime, ok := client.SendTimes[msgID]; ok {
				latency := receiveTime.Sub(sendTime)
				
				latencies = append(latencies, latency)
				totalLatency += latency
				latencyCount++
				
				if latency < minLatency {
					minLatency = latency
				}
				if latency > maxLatency {
					maxLatency = latency
				}
			}
		}
		
		// Sum up total bytes sent
		totalBytesSent += client.BytesSent
		client.lock.RUnlock()
	}
	
	// Calculate success rate - percentage of messages correctly delivered
	var successRate float64
	if len(messageIDs) > 0 {
		successRate = float64(correctlyDelivered) / float64(len(messageIDs)) * 100
	}
	
	// Calculate average latency
	var avgLatency time.Duration
	if latencyCount > 0 {
		avgLatency = totalLatency / time.Duration(latencyCount)
	}
	
	// If no latencies were tracked, set min to 0
	if minLatency == 24*time.Hour {
		minLatency = 0
	}
	
	// Create result object
	results := &TestResults{
		ClientCount:           numClients,
		RoomCount:             numRooms,
		Duration:              duration,
		TotalMessagesSent:     totalSent,
		TotalMessagesReceived: totalReceived,
		DeliveryRate:          float64(totalReceived) / float64(totalSent) * 100,
		ClientsConnected:      connectedClients,
		MessageRatePerSec:     float64(totalSent) / duration.Seconds(),
		// High-throughput specific metrics
		AverageLatency:        avgLatency,
		MaxLatency:            maxLatency,
		MinLatency:            minLatency,
		MessageSizeBytes:      len(messageIDs),
		TotalBytesSent:        totalBytesSent,
		SuccessRate:           successRate,
	}
	
	return results
}
