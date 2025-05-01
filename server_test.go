package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerStartStop tests basic server startup and shutdown
func TestServerStartStop(t *testing.T) {
	// Create a server with a random available port
	server := NewServer(0, DEBUG)
	err := server.Start()
	require.NoError(t, err, "Server should start without error")

	// Get the actual port assigned
	port := server.listener.Addr().(*net.TCPAddr).Port
	assert.Greater(t, port, 0, "Server should be assigned a port")

	// Test shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = server.Stop(ctx)
	assert.NoError(t, err, "Server should stop gracefully")
}

// mockClient represents a test client
type mockClient struct {
	t           *testing.T
	conn        net.Conn
	received    []*MQTTMessage
	mu          sync.Mutex
	clientID    string
	connected   bool
	onMessage   func(*MQTTMessage)
}

// Connect connects to the MQTT server
func (c *mockClient) Connect(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	
	c.conn = conn
	c.connected = true
	
	// Send CONNECT message
	connectMsg := &MQTTMessage{
		Type: "CONNECT",
	}
	if err := c.sendMessage(connectMsg); err != nil {
		return err
	}
	
	// Start reader goroutine
	go c.readLoop()
	
	// Wait for CONNACK
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for CONNACK")
		default:
			c.mu.Lock()
			for _, msg := range c.received {
				if msg.Type == "CONNACK" {
					c.clientID = msg.ClientID
					c.mu.Unlock()
					return nil
				}
			}
			c.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Subscribe subscribes to a topic
func (c *mockClient) Subscribe(topic string) error {
	if !c.connected {
		return fmt.Errorf("not connected")
	}
	
	subscribeMsg := &MQTTMessage{
		Type:  "SUBSCRIBE",
		Topic: topic,
	}
	
	return c.sendMessage(subscribeMsg)
}

// Publish publishes a message to a topic
func (c *mockClient) Publish(topic, payload string) error {
	if !c.connected {
		return fmt.Errorf("not connected")
	}
	
	publishMsg := &MQTTMessage{
		Type:    "PUBLISH",
		Topic:   topic,
		Payload: payload,
	}
	
	return c.sendMessage(publishMsg)
}

// Disconnect gracefully disconnects from the server
func (c *mockClient) Disconnect() error {
	if !c.connected {
		return nil
	}
	
	disconnectMsg := &MQTTMessage{
		Type: "DISCONNECT",
	}
	
	err := c.sendMessage(disconnectMsg)
	if err != nil {
		return err
	}
	
	c.connected = false
	c.conn.Close()
	return nil
}

// sendMessage sends a message to the server
func (c *mockClient) sendMessage(msg *MQTTMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	
	data = append(data, '\n')
	_, err = c.conn.Write(data)
	return err
}

// readLoop reads and processes messages from the server
func (c *mockClient) readLoop() {
	buf := make([]byte, 4096)
	remainBuf := make([]byte, 0)
	
	for c.connected {
		n, err := c.conn.Read(buf)
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				c.t.Logf("Read error: %v", err)
			}
			return
		}
		
		data := append(remainBuf, buf[:n]...)
		messages, remain := splitMessages(data)
		
		for _, msgData := range messages {
			var msg MQTTMessage
			if err := json.Unmarshal(msgData, &msg); err != nil {
				c.t.Logf("Failed to parse message: %v", err)
				continue
			}
			
			c.mu.Lock()
			c.received = append(c.received, &msg)
			c.mu.Unlock()
			
			if c.onMessage != nil {
				c.onMessage(&msg)
			}
		}
		
		remainBuf = remain
	}
}

// waitForMessage waits for a message matching the predicate
func (c *mockClient) waitForMessage(predicate func(*MQTTMessage) bool, timeout time.Duration) (*MQTTMessage, error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-deadline:
			return nil, fmt.Errorf("timeout waiting for message")
		case <-ticker.C:
			c.mu.Lock()
			for _, msg := range c.received {
				if predicate(msg) {
					c.mu.Unlock()
					return msg, nil
				}
			}
			c.mu.Unlock()
		}
	}
}

// splitMessages splits a byte buffer into individual JSON messages
func splitMessages(data []byte) ([][]byte, []byte) {
	var messages [][]byte
	remain := data
	
	for {
		idx := bytes.IndexByte(remain, '\n')
		if idx == -1 {
			break
		}
		
		messages = append(messages, remain[:idx])
		remain = remain[idx+1:]
	}
	
	return messages, remain
}

// newMockClient creates a new test client
func newMockClient(t *testing.T) *mockClient {
	return &mockClient{
		t:        t,
		received: make([]*MQTTMessage, 0),
	}
}

// TestClientConnection tests client connection and disconnection
func TestClientConnection(t *testing.T) {
	// Start server
	server := NewServer(0, DEBUG)
	err := server.Start()
	require.NoError(t, err)
	defer server.Stop(context.Background())
	
	// Get server address
	address := server.listener.Addr().String()
	
	// Connect client
	client := newMockClient(t)
	err = client.Connect(address)
	assert.NoError(t, err, "Client should connect successfully")
	assert.NotEmpty(t, client.clientID, "Client should receive a client ID")
	
	// Disconnect client
	err = client.Disconnect()
	assert.NoError(t, err, "Client should disconnect successfully")
	
	// Ensure server tracks clients properly
	time.Sleep(100 * time.Millisecond) // Brief delay for server to process disconnect
	assert.Equal(t, 0, server.GetClientCount(), "Server should have no connected clients")
}

// TestPublishSubscribe tests publishing and subscribing
func TestPublishSubscribe(t *testing.T) {
	// Start server
	server := NewServer(0, DEBUG)
	err := server.Start()
	require.NoError(t, err)
	defer server.Stop(context.Background())
	
	address := server.listener.Addr().String()
	
	// Create and connect subscriber
	subscriber := newMockClient(t)
	err = subscriber.Connect(address)
	require.NoError(t, err)
	defer subscriber.Disconnect()
	
	// Subscribe to test topic
	topic := "test/topic"
	err = subscriber.Subscribe(topic)
	require.NoError(t, err)
	
	// Wait for SUBACK
	_, err = subscriber.waitForMessage(func(msg *MQTTMessage) bool {
		return msg.Type == "SUBACK" && msg.Topic == topic
	}, 2*time.Second)
	require.NoError(t, err, "Client should receive SUBACK")
	
	// Create and connect publisher
	publisher := newMockClient(t)
	err = publisher.Connect(address)
	require.NoError(t, err)
	defer publisher.Disconnect()
	
	// Publish message to test topic
	message := "Hello, MQTT world!"
	err = publisher.Publish(topic, message)
	require.NoError(t, err)
	
	// Verify subscriber receives the message
	received, err := subscriber.waitForMessage(func(msg *MQTTMessage) bool {
		return msg.Type == "PUBLISH" && msg.Topic == topic && msg.Payload == message
	}, 2*time.Second)
	
	require.NoError(t, err, "Subscriber should receive published message")
	assert.Equal(t, topic, received.Topic, "Message topic should match")
	assert.Equal(t, message, received.Payload, "Message payload should match")
	assert.Equal(t, publisher.clientID, received.ClientID, "Message should contain publisher's client ID")
}

// TestMultiClientChat tests a chat scenario with multiple clients
func TestMultiClientChat(t *testing.T) {
	// Start server
	server := NewServer(0, DEBUG)
	err := server.Start()
	require.NoError(t, err)
	defer server.Stop(context.Background())
	
	address := server.listener.Addr().String()
	chatTopic := "chat/room1"
	
	// Connect multiple clients
	clientCount := 5
	clients := make([]*mockClient, clientCount)
	receivedMessages := make([]map[string]bool, clientCount)
	
	var wg sync.WaitGroup
	
	// Set up message tracking
	for i := 0; i < clientCount; i++ {
		receivedMessages[i] = make(map[string]bool)
	}
	
	// Connect clients and subscribe to chat topic
	for i := 0; i < clientCount; i++ {
		i := i // Capture loop variable
		clients[i] = newMockClient(t)
		
		// Setup message handler
		clients[i].onMessage = func(msg *MQTTMessage) {
			if msg.Type == "PUBLISH" && msg.Topic == chatTopic {
				receivedMessages[i][msg.Payload] = true
			}
		}
		
		err := clients[i].Connect(address)
		require.NoError(t, err)
		
		err = clients[i].Subscribe(chatTopic)
		require.NoError(t, err)
		
		// Ensure client subscribes successfully
		_, err = clients[i].waitForMessage(func(msg *MQTTMessage) bool {
			return msg.Type == "SUBACK" && msg.Topic == chatTopic
		}, 2*time.Second)
		require.NoError(t, err)
		
		defer clients[i].Disconnect()
	}
	
	// Wait a moment for all subscriptions to be processed
	time.Sleep(200 * time.Millisecond)
	
	// Each client sends a unique message
	for i, client := range clients {
		message := fmt.Sprintf("Message from client %d", i)
		err := client.Publish(chatTopic, message)
		require.NoError(t, err)
	}
	
	// Wait for message propagation
	time.Sleep(500 * time.Millisecond)
	
	// Verify all clients received all messages
	for i, client := range clients {
		for j := 0; j < clientCount; j++ {
			message := fmt.Sprintf("Message from client %d", j)
			assert.True(t, receivedMessages[i][message], 
				"Client %d should have received message: %s", i, message)
		}
	}
}

// TestConcurrentConnections tests many clients connecting simultaneously
func TestConcurrentConnections(t *testing.T) {
	// Start server
	server := NewServer(0, DEBUG)
	err := server.Start()
	require.NoError(t, err)
	defer server.Stop(context.Background())
	
	address := server.listener.Addr().String()
	
	// Number of concurrent clients
	concurrentClients := 50
	var wg sync.WaitGroup
	wg.Add(concurrentClients)
	
	// Start clients concurrently
	for i := 0; i < concurrentClients; i++ {
		go func(id int) {
			defer wg.Done()
			
			client := newMockClient(t)
			err := client.Connect(address)
			if err != nil {
				t.Logf("Client %d connection error: %v", id, err)
				return
			}
			
			// Subscribe to a unique topic for this client
			topic := fmt.Sprintf("client/%d", id)
			err = client.Subscribe(topic)
			if err != nil {
				t.Logf("Client %d subscription error: %v", id, err)
				client.Disconnect()
				return
			}
			
			// Wait for subscription confirmation
			_, err = client.waitForMessage(func(msg *MQTTMessage) bool {
				return msg.Type == "SUBACK" && msg.Topic == topic
			}, 5*time.Second)
			
			// Clean up
			client.Disconnect()
		}(i)
	}
	
	// Wait for all client operations to complete
	wg.Wait()
	
	// Give server time to clean up disconnected clients
	time.Sleep(500 * time.Millisecond)
	
	// Verify all clients have disconnected
	assert.Equal(t, 0, server.GetClientCount(), "All clients should have disconnected")
}

// TestErrorCases tests error handling of the server
func TestErrorCases(t *testing.T) {
	// Start server
	server := NewServer(0, DEBUG)
	err := server.Start()
	require.NoError(t, err)
	defer server.Stop(context.Background())
	
	address := server.listener.Addr().String()
	
	// Test case 1: Abrupt client disconnection
	t.Run("AbruptDisconnection", func(t *testing.T) {
		client := newMockClient(t)
		err := client.Connect(address)
		require.NoError(t, err)
		
		// Subscribe to a topic
		err = client.Subscribe("test/error")
		require.NoError(t, err)
		
		// Force connection close without DISCONNECT message
		client.conn.Close()
		client.connected = false
		
		// Give server time to detect disconnection
		time.Sleep(200 * time.Millisecond)
		
		// Verify client is removed from server
		initialCount := server.GetClientCount()
		
		// Connect a new client
		newClient := newMockClient(t)
		err = newClient.Connect(address)
		require.NoError(t, err)
		defer newClient.Disconnect()
		
		// Verify server accepted new connection
		assert.Equal(t, initialCount+1, server.GetClientCount(), 
			"Server should accept new connection after abrupt disconnect")
	})
	
	// Test case 2: Invalid message format
	t.Run("InvalidMessageFormat", func(t *testing.T) {
		client := newMockClient(t)
		err := client.Connect(address)
		require.NoError(t, err)
		defer client.Disconnect()
		
		// Send invalid JSON
		_, err = client.conn.Write([]byte("this is not valid JSON\n"))
		require.NoError(t, err)
		
		// Server should not disconnect client for invalid message
		time.Sleep(100 * time.Millisecond)
		assert.True(t, client.connected, "Client should remain connected after invalid message")
		
		// Client should still be able to send valid messages
		err = client.Subscribe("test/recovery")
		require.NoError(t, err)
		
		_, err = client.waitForMessage(func(msg *MQTTMessage) bool {
			return msg.Type == "SUBACK" && msg.Topic == "test/recovery"
		}, 2*time.Second)
		require.NoError(t, err, "Client should still function after sending invalid message")
	})
}

// TestStressHighThroughput tests the server under high message throughput
func TestStressHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}
	
	// Start server
	server := NewServer(0, INFO) // Use INFO level to reduce log noise
	err := server.Start()
	require.NoError(t, err)
	defer server.Stop(context.Background())
	
	address := server.listener.Addr().String()
	chatTopic := "stress/chat"
	
	// Create publisher and multiple subscribers
	publisher := newMockClient(t)
	err = publisher.Connect(address)
	require.NoError(t, err)
	defer publisher.Disconnect()
	
	subscriberCount := 10
	subscribers := make([]*mockClient, subscriberCount)
	
	// Message counters
	messageCount := 100
	receivedCounts := make([]int, subscriberCount)
	var counterMutex sync.Mutex
	
	// Connect subscribers
	for i := 0; i < subscriberCount; i++ {
		i := i // Capture loop variable
		subscribers[i] = newMockClient(t)
		
		// Set up message counting
		subscribers[i].onMessage = func(msg *MQTTMessage) {
			if msg.Type == "PUBLISH" && msg.Topic == chatTopic {
				counterMutex.Lock()
				receivedCounts[i]++
				counterMutex.Unlock()
			}
		}
		
		err := subscribers[i].Connect(address)
		require.NoError(t, err)
		
		err = subscribers[i].Subscribe(chatTopic)
		require.NoError(t, err)
		
		// Wait for subscription confirmation
		_, err = subscribers[i].waitForMessage(func(msg *MQTTMessage) bool {
			return msg.Type == "SUBACK" && msg.Topic == chatTopic
		}, 2*time.Second)
		require.NoError(t, err)
		
		defer subscribers[i].Disconnect()
	}
	
	// Allow time for all subscriptions to register
	time.Sleep(200 * time.Millisecond)
	
	// Send messages at high rate
	start := time.Now()
	for i := 0; i < messageCount; i++ {
		message := fmt.Sprintf("Stress test message %d", i)
		err := publisher.Publish(chatTopic, message)
		require.NoError(t, err)
	}
	
	// Wait for all messages to be processed
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	allReceived := false
	for !allReceived {
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for message delivery")
		case <-ticker.C:
			allReceived = true
			counterMutex.Lock()
			for i, count := range receivedCounts {
				if count < messageCount {
					allReceived = false
					t.Logf("Subscriber %d has received %d/%d messages", i, count, messageCount)
					break
				}
			}
			counterMutex.Unlock()
			
			if allReceived {
				break
			}
		}
	}
	
	elapsed := time.Since(start)
	t.Logf("Processed %d messages to %d clients in %v", 
		messageCount, subscriberCount, elapsed)
	
	// Verify all subscribers received all messages

