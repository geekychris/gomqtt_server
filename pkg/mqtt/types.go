package mqtt

// MQTTMessage defines the structure of an MQTT message
type MQTTMessage struct {
	Type     string `json:"type"`              // CONNECT, PUBLISH, SUBSCRIBE, DISCONNECT
	Topic    string `json:"topic,omitempty"`   // Used for PUBLISH and SUBSCRIBE
	Payload  string `json:"payload,omitempty"` // Message content for PUBLISH
	ClientID string `json:"clientID,omitempty"` // Client identifier
}

