package main

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/chris/gomqtt/pkg/mqtt"
	"github.com/google/uuid"
)
// Client represents a connected MQTT client
type Client struct {
	ID             string
	Conn           net.Conn
	Broker         *Broker
	Subscriptions  map[string]bool
	IncomingMsgs   chan *mqtt.MQTTMessage
	OutgoingMsgs   chan *mqtt.MQTTMessage
	DisconnectChan chan struct{}
	mu             sync.RWMutex
	logger         *Logger
}

// NewClient creates a new MQTT client instance
func NewClient(conn net.Conn, broker *Broker, logger *Logger) *Client {
	clientID := uuid.New().String()
	return &Client{
		ID:             clientID,
		Conn:           conn,
		Broker:         broker,
		Subscriptions:  make(map[string]bool),
		IncomingMsgs:   make(chan *mqtt.MQTTMessage, 100),
		OutgoingMsgs:   make(chan *mqtt.MQTTMessage, 100),
		DisconnectChan: make(chan struct{}),
		logger:         logger,
	}
}

// Start begins client message handling
func (c *Client) Start() {
	c.logger.Info("Client %s connected from %s", c.ID, c.Conn.RemoteAddr())
	
	// Start reader and writer goroutines
	go c.readLoop()
	go c.writeLoop()
}

// readLoop reads messages from client connection
func (c *Client) readLoop() {
	defer c.Disconnect()
	
	reader := bufio.NewReader(c.Conn)
	
	for {
		select {
		case <-c.DisconnectChan:
			return
		default:
			// Set read deadline to detect stale connections
			c.Conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
			
			line, err := reader.ReadString('\n')
			if err != nil {
				c.logger.Error("Read error from client %s: %v", c.ID, err)
				return
			}
			
			var msg mqtt.MQTTMessage
			if err := json.Unmarshal([]byte(line), &msg); err != nil {
				c.logger.Error("Failed to parse message from client %s: %v", c.ID, err)
				continue
			}
			
			// Process the message based on type
			c.handleMessage(&msg)
		}
	}
}

// writeLoop sends messages to the client
func (c *Client) writeLoop() {
	defer c.Disconnect()
	
	for {
		select {
		case <-c.DisconnectChan:
			return
		case msg := <-c.OutgoingMsgs:
			data, err := json.Marshal(msg)
			if err != nil {
				c.logger.Error("Failed to marshal message for client %s: %v", c.ID, err)
				continue
			}
			
			// Add newline as message delimiter
			data = append(data, '\n')
			
			_, err = c.Conn.Write(data)
			if err != nil {
				c.logger.Error("Failed to write to client %s: %v", c.ID, err)
				return
			}
		}
	}
}

// handleMessage processes incoming MQTT messages
func (c *Client) handleMessage(msg *mqtt.MQTTMessage) {
	switch msg.Type {
	case "CONNECT":
		// For simplicity, we accept all connections
		// In a real implementation, you'd verify credentials here
		response := &mqtt.MQTTMessage{
			Type:     "CONNACK",
			ClientID: c.ID,
		}
		c.OutgoingMsgs <- response
		
	case "PUBLISH":
		if msg.Topic == "" {
			c.logger.Error("Client %s attempted to publish with empty topic", c.ID)
			return
		}
		
		// Add clientID to the message
		msg.ClientID = c.ID
		
		// Send to broker for distribution
		c.Broker.Publish(msg)
		
	case "SUBSCRIBE":
		if msg.Topic == "" {
			c.logger.Error("Client %s attempted to subscribe with empty topic", c.ID)
			return
		}
		
		c.mu.Lock()
		c.Subscriptions[msg.Topic] = true
		c.mu.Unlock()
		
		// Register with broker
		c.Broker.Subscribe(msg.Topic, c)
		
		// Confirm subscription
		response := &mqtt.MQTTMessage{
			Type:  "SUBACK",
			Topic: msg.Topic,
		}
		c.OutgoingMsgs <- response
		
	case "DISCONNECT":
		c.logger.Info("Client %s requested disconnect", c.ID)
		c.Disconnect()
		
	default:
		c.logger.Error("Unknown message type from client %s: %s", c.ID, msg.Type)
	}
}
// SendMessage sends a message to this client
func (c *Client) SendMessage(msg *mqtt.MQTTMessage) {
	select {
	case c.OutgoingMsgs <- msg:
		// Message sent successfully
	default:
		c.logger.Error("Failed to send message to client %s: queue full", c.ID)
	}
}

// IsSubscribed checks if client is subscribed to a topic
func (c *Client) IsSubscribed(topic string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Subscriptions[topic]
}

// Disconnect disconnects the client and cleans up resources
func (c *Client) Disconnect() {
	// Use close(c.DisconnectChan) in a thread-safe way to avoid closing a closed channel
	c.mu.Lock()
	select {
	case <-c.DisconnectChan:
		// Channel already closed
	default:
		close(c.DisconnectChan)
	}
	c.mu.Unlock()

	// Unsubscribe from all topics
	c.mu.RLock()
	for topic := range c.Subscriptions {
		c.Broker.Unsubscribe(topic, c)
	}
	c.mu.RUnlock()

	// Remove from broker
	c.Broker.RemoveClient(c)

	// Close connection
	c.Conn.Close()
	
	c.logger.Info("Client %s disconnected", c.ID)
}

