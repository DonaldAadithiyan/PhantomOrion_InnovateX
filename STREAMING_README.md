# Project Sentinel - Real-time Streaming Detection System

This document explains how to use the new real-time streaming detection system that processes events from the streaming server instead of static CSV files.

## Overview

The system consists of two main components:

1. **Stream Server** (`data/streaming-server/stream_server.py`) - Replays historical data as a real-time stream
2. **Detection Applications** - Process the streaming data and run detection algorithms

## Quick Start

### 1. Start the Stream Server

First, start the streaming server to replay the historical data:

```bash
cd data/streaming-server
python stream_server.py --port 8765 --loop --speed 1.0
```

Options:
- `--port 8765` - Port to listen on (default: 8765)
- `--loop` - Continuously loop the dataset
- `--speed 1.0` - Replay speed (1.0 = real-time, higher = faster)
- `--datasets` - Specify which datasets to stream (optional)

### 2. Run the Detection Application

In a separate terminal, run the main detection application:

```bash
cd src
python app.py --host 127.0.0.1 --port 8765 --detection-interval 5 --verbose
```

Options:
- `--host 127.0.0.1` - Stream server host
- `--port 8765` - Stream server port
- `--detection-interval 5` - Run detections every N seconds
- `--verbose` - Show detailed event information
- `--limit 100` - Stop after N events (0 = unlimited)
- `--output-dir logs` - Directory to save detection results

### 3. Alternative: Use the Stream Client

You can also use the standalone stream client:

```bash
cd data/streaming-clients
python stream_client.py --host 127.0.0.1 --port 8765 --detection-interval 5
```

## How It Works

### Data Flow

1. **Stream Server** reads historical JSON/JSONL files from `data/input/`
2. **Stream Server** replays them as real-time events over TCP
3. **Detection Application** connects to the stream and receives events
4. **Detection Application** buffers events by type (RFID, POS, Queue, etc.)
5. **Detection Application** runs detection algorithms periodically on buffered data
6. **Detection Application** saves detected events to `logs/` directory

### Event Types

The system processes these event types:

- **RFID_data** - RFID tag readings
- **POS_Transactions** - Point of sale transactions
- **Queue_monitor** - Queue monitoring data
- **Product_recognism** - Product recognition data
- **Current_inventory_data** - Inventory snapshots

### Detection Algorithms

The following detection algorithms run on the streaming data:

1. **Scanner Avoidance** - Items leaving scan area without being scanned
2. **Barcode Switching** - Price manipulation through barcode switching
3. **Weight Discrepancies** - Weight differences beyond threshold
4. **System Errors** - Hardware/software failures
5. **Long Queues** - Persistent long customer queues
6. **Extended Wait Times** - Customers experiencing long wait times
7. **Inventory Discrepancies** - Stock level discrepancies

## Configuration

### Stream Server Configuration

```bash
python stream_server.py --help
```

Key options:
- `--data-root` - Directory containing input data files
- `--datasets` - Specific datasets to stream
- `--speed` - Replay speed multiplier (1-100)
- `--loop` - Continuously loop the dataset

### Detection Application Configuration

```bash
python app.py --help
```

Key options:
- `--detection-interval` - How often to run detections (seconds)
- `--buffer-size` - Maximum events to keep in memory
- `--output-dir` - Where to save detection results

## Output

### Detection Events

Detected events are saved to JSONL files in the `logs/` directory:

```
logs/detection_events_20250104_143022.jsonl
```

Each line contains a JSON object with:
- `timestamp` - When the event was detected
- `event_id` - Unique identifier
- `event_data` - Event details including type, station, customer, etc.

### Console Output

The application provides real-time feedback:

```
[   1] POS_Transactions     2024-01-01T10:00:00
[   2] RFID_data           2024-01-01T10:00:01
...

--- Detection Run #1 ---
Processing: 45 RFID, 23 POS, 12 queue events
  → Scanner Avoidance: 2 events
  → Barcode Switching: 1 events
  → Weight Discrepancies: 0 events
  → System Errors: 0 events
  → Long Queues: 1 events
  → Extended Wait Times: 0 events
  → Inventory Discrepancies: 0 events
  → Total events detected: 4
  → Saved 4 events to logs/detection_events_20250104_143022.jsonl
```

## Troubleshooting

### Connection Issues

- Ensure the stream server is running before starting the detection app
- Check that the port is not already in use
- Verify firewall settings allow TCP connections

### Data Issues

- Ensure input data files exist in `data/input/`
- Check that product data CSV is accessible
- Verify JSON/JSONL files are properly formatted

### Performance Issues

- Increase `--detection-interval` to run detections less frequently
- Reduce `--buffer-size` to use less memory
- Use `--limit` to process only a subset of events

## Examples

### Process 100 events with verbose output

```bash
python app.py --limit 100 --verbose --detection-interval 2
```

### Fast replay with frequent detection

```bash
# Terminal 1: Fast server
python stream_server.py --speed 10.0 --loop

# Terminal 2: Frequent detection
python app.py --detection-interval 1 --verbose
```

### Process specific datasets only

```bash
# Terminal 1: Stream only POS and RFID data
python stream_server.py --datasets POS_Transactions RFID_data --loop

# Terminal 2: Run detection app
python app.py --verbose
```
