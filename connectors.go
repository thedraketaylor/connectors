package connectors

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/opensearch-project/opensearch-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	RabbitMQ struct {
		Host     string
		Port     string
		User     string
		Password string
		Queue    string
	}
	OpenSearch struct {
		URL      string
		Username string
		Password string
	}
}

func LoadConfig() (*Config, error) {
	// In a real application, this would load from environment variables or a config file
	config := &Config{}
	config.RabbitMQ.Host = GetEnvOrDefault("RABBITMQ_HOST", "")
	config.RabbitMQ.Port = GetEnvOrDefault("RABBITMQ_PORT", "5672")
	config.RabbitMQ.User = GetEnvOrDefault("RABBITMQ_USER", "")
	config.RabbitMQ.Password = GetEnvOrDefault("RABBITMQ_PASSWORD", "")
	config.RabbitMQ.Queue = GetEnvOrDefault("RABBITMQ_QUEUE", "")

	config.OpenSearch.URL = GetEnvOrDefault("OPENSEARCH_URL", "")
	config.OpenSearch.Username = GetEnvOrDefault("OPENSEARCH_USER", "")
	config.OpenSearch.Password = GetEnvOrDefault("OPENSEARCH_PASSWORD", "")

	return config, nil
}

func GetEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func GetSHA256(str string) string {
	hash := sha256.Sum256([]byte(str))
	return hex.EncodeToString(hash[:])
}

func NewRMQConnection(config *Config) (*RMQConnection, error) {
	connectionURL := fmt.Sprintf("amqp://%s:%s@%s:%s/",
		config.RabbitMQ.User, config.RabbitMQ.Password,
		config.RabbitMQ.Host, config.RabbitMQ.Port)

	conn, err := amqp.Dial(connectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return &RMQConnection{
		conn: conn,
		ch:   ch,
	}, nil
}

// RMQConnection represents a RabbitMQ connection wrapper
type RMQConnection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func (c *RMQConnection) Close() {
	if c.ch != nil {
		if err := c.ch.Close(); err != nil {
			slog.Error("Error closing RabbitMQ channel", "error", err)
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			slog.Error("Error closing RabbitMQ connection", "error", err)
		}
	}
}

func ConnectToOS(config *Config) (*opensearch.Client, error) {
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // Note: In production, properly configure TLS
		},
		Addresses: []string{config.OpenSearch.URL},
		Username:  config.OpenSearch.Username,
		Password:  config.OpenSearch.Password,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create OpenSearch client: %w", err)
	}

	return client, nil
}
