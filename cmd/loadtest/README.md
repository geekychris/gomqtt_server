# MQTT Chat Load Test

## Overview

The MQTT Chat Load Test is a tool for stress testing and benchmarking MQTT servers with simulated chat clients. It helps evaluate server performance under various loads and usage patterns by simulating multiple clients connecting to chat rooms and exchanging messages.

Key features:
- Simulates multiple concurrent clients
- Distributes clients across multiple chat rooms
- Tracks message delivery rates and latencies
- Provides detailed performance metrics
- Offers both standard and high-throughput testing modes

## Prerequisites

- Go 1.16 or later
- A running MQTT server to test against
- Network connectivity to the MQTT server

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/chris/gomqtt.git
   cd gomqtt/cmd/loadtest
   ```

2. Build the application:
   ```
   go build
   ```

## Usage

The load test tool offers two main testing modes:

1. **Standard Mode**: Simulates clients sending messages at a steady rate
2. **High-Throughput Mode**: Tests server performance under bursts of messages

### Basic Usage

Run the test with default parameters:

```
./loadtest --server localhost:1883
```

This will run a test with:
- 10 clients
- 3 chat rooms
- 30 seconds duration
- 1000ms message send interval

### Command-Line Parameters

#### Common Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--server` | MQTT server address | localhost:1883 |
| `--clients` | Number of clients to simulate | 10 |
| `--rooms` | Number of chat rooms | 3 |
| `--duration` | Test duration in seconds | 30 |
| `--verbose` | Enable detailed logging | false |

#### Standard Mode Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--messages` | Messages per client | 10 |
| `--interval` | Send interval in milliseconds | 1000 |

#### High-Throughput Mode Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--high-throughput` | Enable high-throughput mode | false |
| `--message-size` | Message size in bytes | 1024 |
| `--burst-size` | Messages per burst | 10 |
| `--burst-interval` | Seconds between bursts | 2 |

### Example Commands

1. Standard test with 100 clients across 5 rooms:
   ```
   ./loadtest --server mqtt.example.com:1883 --clients 100 --rooms 5 --duration 60
   ```

2. High-throughput test with large messages:
   ```
   ./loadtest --server mqtt.example.com:1883 --high-throughput --clients 50 --message-size 4096 --burst-size 20
   ```

3. Quick test with more frequent messages:
   ```
   ./loadtest --server localhost:1883 --clients 10 --interval 100 --duration 10
   ```

## Test Results

The load test provides comprehensive results, including:

### Common Metrics

- Total messages sent and received
- Message rate (messages per second)
- Delivery rate (percentage of messages successfully delivered)
- Number of clients successfully connected
- Test duration

### High-Throughput Mode Metrics

In high-throughput mode, additional metrics are provided:

- Message size in bytes
- Total data sent (KB)
- Data rate (KB/second)
- Average, minimum, and maximum latency

### Example Output

```
===== TEST RESULTS =====
Clients: 10, Rooms: 3, Duration: 30.00 seconds
Total Messages Sent: 300
Total Messages Received: 270
Message Rate: 10.00 msgs/sec
Delivery Rate: 90.00%

-- High Throughput Metrics --
Message Size: 1024 bytes
Total Data Sent: 307.20 KB
Data Rate: 10.24 KB/sec
Success Rate: 90.00%

-- Latency Metrics --
Average Latency: 15ms
Min Latency: 5ms
Max Latency: 150ms
========================
```

## Interpreting Results

- **High Delivery Rate (>95%)**: Excellent server performance
- **Good Delivery Rate (80-95%)**: Acceptable server performance
- **Poor Delivery Rate (<80%)**: Server may be struggling with the load

For high-throughput tests:
- **Latency**: Lower is better. Spikes in max latency may indicate server congestion
- **Success Rate**: Should remain high even under heavy load
- **Data Rate**: Higher values indicate better throughput capability

## Troubleshooting

If you encounter issues:

1. Check network connectivity to the MQTT server
2. Verify the server is correctly configured to handle the test load
3. For connection failures, try reducing the number of clients
4. For high packet loss, try increasing send intervals
5. Use the `--verbose` flag to see detailed client activity

## License

[Insert appropriate license information here]

