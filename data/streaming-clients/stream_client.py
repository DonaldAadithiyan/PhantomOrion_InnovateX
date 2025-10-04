#!/usr/bin/env python3
"""Real-time detection client for Project Sentinel event stream.

Connects to the stream server and processes each incoming event through
the detection pipeline, logging anomalies as they occur.
"""

import argparse
import json
import socket
import sys
from typing import Iterator
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import real-time detection functions
from src.detection.realtime_detection import (
    process_incoming_row,
    load_product_data,
    pos_skus_cache,
    rfid_cache,
    product_recognition_cache,
    product_data_cache,
    error_aggregation,
    queue_state,
    sales_count
)
from src.detection.utils import read_csv


class StreamDetectionClient:
    """Client that processes events from stream server through detection pipeline."""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.event_count = 0
        self.detection_count = 0
        self.dataset_map = {
            'POS_Transactions': 'pos',
            'RFID_data': 'rfid',
            'Queue_monitor': 'queue',
            'Product_recognism': 'product_recognition',
            'Current_inventory_data': 'inventory'
        }
    
    def connect_and_process(self):
        """Connect to stream server and process events in real-time."""
        print("="*70)
        print("REAL-TIME DETECTION CLIENT")
        print("="*70)
        print(f"Connecting to stream server at {self.host}:{self.port}...")
        
        try:
            with socket.create_connection((self.host, self.port)) as conn:
                print("✓ Connected to stream server\n")
                
                with conn.makefile("r", encoding="utf-8") as stream:
                    for line in stream:
                        if not line.strip():
                            continue
                        
                        frame = json.loads(line)
                        
                        # First message is the banner
                        if 'service' in frame:
                            self._print_banner(frame)
                            continue
                        
                        # Process event through detection pipeline
                        self._process_event(frame)
        
        except ConnectionRefusedError:
            print(f"✗ Error: Could not connect to {self.host}:{self.port}")
            print("  Make sure the stream server is running:")
            print("  python streaming-server/stream_server.py --port 8765 --speed 10 --loop")
            sys.exit(1)
        except KeyboardInterrupt:
            print("\n\n" + "="*70)
            print("STREAM INTERRUPTED BY USER")
            self._print_summary()
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def _print_banner(self, banner: dict):
        """Print server banner information."""
        print("Server Information:")
        print(f"  Datasets: {', '.join(banner.get('datasets', []))}")
        print(f"  Total events: {banner.get('events', 0)}")
        print(f"  Loop mode: {banner.get('loop', False)}")
        print(f"  Speed: {banner.get('speed_factor', 1.0)}x")
        print(f"  Cycle duration: {banner.get('cycle_seconds', 0):.1f}s")
        print("\n" + "="*70)
        print("PROCESSING EVENTS...")
        print("="*70 + "\n")
    
    def _process_event(self, frame: dict):
        """Process a single event frame through detection pipeline."""
        self.event_count += 1
        
        dataset = frame.get('dataset')
        sequence = frame.get('sequence')
        event = frame.get('event')
        
        if not event:
            return
        
        # Map dataset name to detection source type
        source_type = self.dataset_map.get(dataset)
        
        if not source_type:
            # Unknown dataset, skip
            return
        
        # Process through detection pipeline
        result = process_incoming_row(event, source_type)
        
        # Count detections
        if result:
            if isinstance(result, list):
                self.detection_count += len(result)
            else:
                self.detection_count += 1
        
        # Print progress every 100 events
        if self.event_count % 100 == 0:
            print(f"[Progress] Processed {self.event_count} events, "
                  f"detected {self.detection_count} anomalies")
    
    def _print_summary(self):
        """Print processing summary."""
        print("PROCESSING SUMMARY")
        print("="*70)
        print(f"Total events processed: {self.event_count}")
        print(f"Total anomalies detected: {self.detection_count}")
        if self.event_count > 0:
            print(f"Detection rate: {(self.detection_count/self.event_count*100):.2f}%")
        print("="*70)
        print("\n✓ Events logged to: logs/realtime_events.jsonl")


def setup_detection_system():
    """Initialize detection system with product catalog."""
    print("Initializing detection system...")
    
    try:
        # Get project root directory
        project_root = Path(__file__).parent.parent
        
        # Load product catalog
        product_data_path = project_root / 'data' / 'input' / 'products_list.csv'
        
        if not product_data_path.exists():
            print(f"✗ Error: Product catalog not found at {product_data_path}")
            print("  Please ensure products_list.csv exists in data/input/")
            sys.exit(1)
        
        product_data = read_csv(str(product_data_path))
        load_product_data(product_data)
        print(f"✓ Loaded {len(product_data)} products into cache")
        
        # Clear previous event log
        import os
        logs_dir = project_root / 'logs'
        os.makedirs(logs_dir, exist_ok=True)
        
        log_file = logs_dir / 'realtime_events.jsonl'
        if log_file.exists():
            os.remove(log_file)
            print(f"✓ Cleared previous event log\n")
        else:
            print(f"✓ Created logs directory\n")
        
        return True
    
    except Exception as e:
        print(f"✗ Error during setup: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Real-time detection client for Project Sentinel event stream"
    )
    parser.add_argument(
        "--host", 
        default="127.0.0.1",
        help="Stream server host address"
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=8765,
        help="Stream server port"
    )
    args = parser.parse_args()
    
    # Setup detection system
    if not setup_detection_system():
        sys.exit(1)
    
    # Create and run client
    client = StreamDetectionClient(args.host, args.port)
    client.connect_and_process()


if __name__ == "__main__":
    main()