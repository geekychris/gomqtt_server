package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Server represents the MQTT server
type Server struct {
	broker      *Broker
	listener    net.Listener
	logger      *Logger
	port        int
	shutdown    chan struct{}
	wg          sync.WaitGroup
}

// NewServer creates a new MQTT server instance
func NewServer(port int, logLevel LogLevel) *Server {
	logger := NewLogger(logLevel)
	broker := NewBroker(logger)
	
	return &Server{
		broker:   broker,
		logger:   logger,
		port:     port,
		shutdown: make(chan struct{}),
	}
}

// Start begins listening for client connections
func (s *Server) Start() error {
	// Create listener
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener
	
	s.logger.Info("MQTT server started on %s", addr)
	
	// Accept connections in a goroutine
	s.wg.Add(1)
	go s.acceptConnections()
	
	return nil
}

// acceptConnections handles incoming client connections
func (s *Server) acceptConnections() {
	defer s.wg.Done()
	
	for {
		select {
		case <-s.shutdown:
			return
		default:
			// Set accept deadline to allow for graceful shutdown
			s.listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second))
			
			conn, err := s.listener.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// This is just a timeout during shutdown, continue the loop
					continue
				}
				s.logger.Error("Failed to accept connection: %v", err)
				continue
			}
			
			// Create a new client for this connection
			client := NewClient(conn, s.broker, s.logger)
			
			// Register client with broker
			s.broker.AddClient(client)
			
			// Start client goroutines
			client.Start()
		}
	}
}

// Stop gracefully shuts down the server
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping MQTT server...")
	
	// Signal shutdown
	close(s.shutdown)
	
	// Close listener to stop accepting new connections
	if s.listener != nil {
		s.listener.Close()
	}
	
	// Wait for ongoing operations with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		s.logger.Info("MQTT server stopped gracefully")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("server shutdown timed out: %w", ctx.Err())
	}
}

// GetClientCount returns the number of connected clients
func (s *Server) GetClientCount() int {
	return s.broker.GetClientCount()
}

func main() {
	// Parse command line flags
	port := flag.Int("port", 1883, "Port to listen on")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()
	
	// Determine log level
	logLevel := INFO
	if *verbose {
		logLevel = DEBUG
	}
	
	// Create and start server
	server := NewServer(*port, logLevel)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting server: %v\n", err)
		os.Exit(1)
	}
	
	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Wait for termination signal
	sig := <-sigChan
	fmt.Printf("Received signal %v, shutting down...\n", sig)
	
	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := server.Stop(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error during shutdown: %v\n", err)
		os.Exit(1)
	}
}
