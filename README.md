# Project Sentinel - Real-time Detection System

A real-time streaming detection system that processes retail data to identify anomalies, fraud, and operational issues in a smart checkout environment.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- All dependencies installed (see `src/requirements.txt`)

### Running the Application

#### Option 1: Run with All Datasets (Recommended)
```bash
# Terminal 1: Start the Stream Server
cd data/streaming-server
python stream_server.py --port 8765 --loop --speed 2.0

# Terminal 2: Run the Detection Application
cd src
python app.py --host 127.0.0.1 --port 8765 --detection-interval 3 --verbose --limit 100
```

#### Option 2: Run with Specific Datasets
```bash
# Terminal 1: Start the Stream Server with specific datasets
cd data/streaming-server
python stream_server.py --port 8765 --loop --speed 2.0 --datasets POS_Transactions RFID_data Queue_monitor

# Terminal 2: Run the Detection Application
cd src
python app.py --host 127.0.0.1 --port 8765 --detection-interval 3 --verbose --limit 100
```

## 📁 Project Structure

```
PhantomOrion_InnovateX/
├── data/
│   ├── input/                          # Input data files
│   │   ├── pos_transactions.jsonl     # POS transaction data
│   │   ├── rfid_readings.jsonl        # RFID tag readings
│   │   ├── queue_monitoring.jsonl     # Queue monitoring data
│   │   ├── product_recognition.jsonl  # Product recognition data
│   │   ├── inventory_snapshots.jsonl  # Inventory snapshots
│   │   └── products_list.csv          # Product catalog
│   ├── output/                         # Expected output format
│   │   └── events.jsonl               # Sample detection events
│   ├── streaming-server/              # Stream server
│   │   └── stream_server.py           # Main server script
│   └── streaming-clients/             # Client examples
│       ├── client_example.py          # Basic client
│       └── stream_client.py           # Detection client
├── src/
│   ├── detection/                     # Detection algorithms
│   │   ├── all_detections.py         # Main detection functions
│   │   └── utils.py                  # Utility functions
│   ├── app.py                        # Main detection application
│   └── logs/                         # Generated detection events
└── README.md                         # This file
```

## 🔧 Configuration Options

### Stream Server Options
```bash
python stream_server.py [OPTIONS]

Options:
  --port PORT              TCP port to expose the stream (default: 8765)
  --host HOST              Bind address (default: 0.0.0.0)
  --speed SPEED            Replay speed multiplier (default: 1.0)
  --loop                   Continuously loop the dataset
  --datasets DATASETS      Specific datasets to stream
  --log-level LEVEL        Logging verbosity (DEBUG, INFO, WARNING, ERROR)
```

### Detection Application Options
```bash
python app.py [OPTIONS]

Options:
  --host HOST              Stream server host (default: 127.0.0.1)
  --port PORT              Stream server port (default: 8765)
  --product-data PATH      Path to products CSV file
  --output-dir DIR         Directory to save detection results
  --detection-interval N   Run detections every N seconds (default: 5)
  --limit N                Stop after N events (0 = unlimited)
  --verbose                Enable verbose output
```

## 🎯 Detection Algorithms

The system detects the following types of anomalies:

### 1. Scanner Avoidance
- **Description**: Items that leave the scan area without being scanned at POS
- **Data Sources**: RFID readings + POS transactions
- **Output**: Event with customer, product, and station information

### 2. Barcode Switching
- **Description**: Price manipulation through barcode switching
- **Data Sources**: POS transactions + RFID readings + Product recognition
- **Output**: Event with actual vs scanned product information

### 3. Weight Discrepancies
- **Description**: Items with weight differences beyond threshold
- **Data Sources**: POS transactions + Product catalog
- **Output**: Event with expected vs actual weight

### 4. System Errors
- **Description**: Hardware/software failures and recurring issues
- **Data Sources**: All datasets (status field)
- **Output**: Event with error type and duration

### 5. Long Queues
- **Description**: Persistent long customer queues
- **Data Sources**: Queue monitoring data
- **Output**: Event with queue length and duration

### 6. Extended Wait Times
- **Description**: Customers experiencing long wait times
- **Data Sources**: Queue monitoring data
- **Output**: Event with wait time and priority

### 7. Inventory Discrepancies
- **Description**: Stock level discrepancies
- **Data Sources**: Inventory snapshots + POS transactions + Product catalog
- **Output**: Event with expected vs actual inventory

## 📊 Output Format

Detection events are saved as JSONL files in the `src/logs/` directory:

```json
{
  "timestamp": "2025-08-13T16:08:40",
  "event_id": "E001",
  "event_data": {
    "event_name": "Barcode Switching",
    "station_id": "SCC1",
    "customer_id": "C056",
    "actual_sku": "PRD_F_14",
    "scanned_sku": "4792024011348"
  }
}
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Port Already in Use
```bash
# Kill existing processes
pkill -f stream_server
pkill -f app.py
lsof -ti:8765 | xargs kill -9
```

#### 2. No Detection Events Generated
- Check if POS transactions are being streamed
- Verify product data is loaded correctly
- Ensure RFID data has valid SKU values

#### 3. CSV Reading Errors
- The system automatically handles CSV files with blank first lines
- Ensure the products CSV has the correct column headers

#### 4. Memory Issues
- Reduce `--detection-interval` to run detections less frequently
- Use `--limit` to process only a subset of events
- The system has built-in buffer management

### Debug Mode
```bash
# Run server with debug logging
python stream_server.py --port 8765 --loop --speed 1.0 --log-level DEBUG

# Run detection app with verbose output
python app.py --host 127.0.0.1 --port 8765 --verbose --limit 50
```

## 📈 Performance

### Typical Performance
- **Processing Rate**: 2-5 events per second
- **Memory Usage**: ~50MB for 1000 events
- **Detection Latency**: 3-5 seconds (configurable)
- **Output**: Clean, deduplicated events

### Optimization Tips
- Use `--speed 2.0` for faster replay
- Set `--detection-interval 2` for more frequent detection
- Use `--limit 100` for testing with limited data

## 🔄 Data Flow

1. **Stream Server** reads historical data from `data/input/`
2. **Stream Server** replays data as real-time events over TCP
3. **Detection Application** connects and receives events
4. **Detection Application** buffers events by type (RFID, POS, Queue, etc.)
5. **Detection Application** runs detection algorithms periodically
6. **Detection Application** saves detected events to `src/logs/`

## 📝 Examples

### Basic Usage
```bash
# Start server
cd data/streaming-server
python stream_server.py --port 8765 --loop --speed 1.0

# Run detection
cd src
python app.py --host 127.0.0.1 --port 8765 --verbose
```

### Testing with Limited Data
```bash
# Process only 50 events
python app.py --host 127.0.0.1 --port 8765 --limit 50 --verbose
```

### Fast Processing
```bash
# High speed with frequent detection
python stream_server.py --port 8765 --loop --speed 5.0
python app.py --host 127.0.0.1 --port 8765 --detection-interval 1 --verbose
```

## 🎉 Success Indicators

When the system is working correctly, you should see:
- ✅ Stream server shows "Server ready" message
- ✅ Detection app shows "PROJECT SENTINEL" banner
- ✅ Events are processed in real-time
- ✅ Detection events are generated and saved
- ✅ Log files appear in `src/logs/` directory

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify all data files exist in `data/input/`
3. Ensure Python dependencies are installed
4. Check that ports 8765 is available

---

**Project Sentinel** - Real-time retail anomaly detection system
