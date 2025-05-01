package main

import (
	"strings"
	"sync"
	
	"github.com/chris/gomqtt/pkg/mqtt"
)

// Broker manages MQTT topics and message distribution
type Broker struct {
	clients        map[string]*Client
	subscriptions  map[string]map[string]*Client // topic -> clientID -> client
	mu             sync.RWMutex
	logger         *Logger
}

// NewBroker creates a new MQTT broker instance
func NewBroker(logger *Logger) *Broker {
	return &Broker{
		clients:       make(map[string]*Client),
		subscriptions: make(map[string]map[string]*Client),
		logger:        logger,
	}
}

// AddClient registers a new client with the broker
func (b *Broker) AddClient(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	b.clients[client.ID] = client
	b.logger.Info("Added client %s to broker", client.ID)
}

// RemoveClient removes a client from the broker
func (b *Broker) RemoveClient(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	// Remove client from all subscriptions
	for topic, clients := range b.subscriptions {
		delete(clients, client.ID)
		// Clean up empty topics
		if len(clients) == 0 {
			delete(b.subscriptions, topic)
		}
	}
	
	// Remove from clients list
	delete(b.clients, client.ID)
	b.logger.Info("Removed client %s from broker", client.ID)
}

// Subscribe registers a client's interest in a topic
func (b *Broker) Subscribe(topic string, client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	// Initialize topic map if it doesn't exist
	if _, exists := b.subscriptions[topic]; !exists {
		b.subscriptions[topic] = make(map[string]*Client)
	}
	
	// Add client to topic subscribers
	b.subscriptions[topic][client.ID] = client
	b.logger.Info("Client %s subscribed to topic %s", client.ID, topic)
}

// Unsubscribe removes a client's subscription
func (b *Broker) Unsubscribe(topic string, client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if clients, exists := b.subscriptions[topic]; exists {
		// Remove client from topic
		delete(clients, client.ID)
		
		// Clean up empty topics
		if len(clients) == 0 {
			delete(b.subscriptions, topic)
		}
		
		b.logger.Info("Client %s unsubscribed from topic %s", client.ID, topic)
	}
}

// Publish distributes a message to all clients subscribed to the topic
func (b *Broker) Publish(msg *mqtt.MQTTMessage) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	topic := msg.Topic
	b.logger.Info("Publishing message to topic %s from client %s", topic, msg.ClientID)
	
	// Find matching topics (direct match and wildcard matches)
	for subscribedTopic, clients := range b.subscriptions {
		if topicMatches(subscribedTopic, topic) {
			// Send to all subscribed clients
			for _, client := range clients {
				// Don't send back to the publisher unless they're subscribed
				if client.ID != msg.ClientID || client.IsSubscribed(topic) {
					client.SendMessage(msg)
				}
			}
		}
	}
}

// topicMatches checks if a subscription matches a published topic
// Supports simple wildcards:
// - "#" matches everything
// - "+" matches a single level
func topicMatches(subscription, published string) bool {
	// Exact match
	if subscription == published {
		return true
	}
	
	// Wildcard match
	if subscription == "#" {
		return true
	}
	
	// Split into segments
	subParts := strings.Split(subscription, "/")
	pubParts := strings.Split(published, "/")
	
	// Different depths without wildcards can't match
	if len(subParts) != len(pubParts) && !strings.Contains(subscription, "#") {
		return false
	}
	
	for i, subPart := range subParts {
		// # wildcard matches everything at this position and beyond
		if subPart == "#" {
			return true
		}
		
		// + wildcard matches exactly one level
		if subPart == "+" {
			continue
		}
		
		// We've gone past the published topic segments
		if i >= len(pubParts) {
			return false
		}
		
		// Exact segment match required
		if subPart != pubParts[i] {
			return false
		}
	}
	
	// If we get here and the lengths match, then everything matched
	return len(subParts) == len(pubParts)
}

// GetClientCount returns the current number of connected clients
func (b *Broker) GetClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// GetSubscriptionCount returns the number of subscriptions for a topic
func (b *Broker) GetSubscriptionCount(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	if clients, exists := b.subscriptions[topic]; exists {
		return len(clients)
	}
	return 0
}
