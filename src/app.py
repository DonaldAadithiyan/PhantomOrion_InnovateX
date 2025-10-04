#!/usr/bin/env python3
"""
Main application for Project Sentinel real-time detection system.

This application connects to the streaming server and processes events in real-time,
running detection algorithms on the streaming data instead of static CSV files.
"""

import argparse
import json
import socket
import threading
import time
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Iterator, Dict, List, Any, Optional

# Import detection functions
from detection.all_detections import (
    detect_scan_avoidance,
    detect_barcode_switch,
    detect_weight_discrepancy,
    detect_system_errors,
    detect_long_queue,
    detect_extended_wait,
    detect_inventory_discrepancy
)
from detection.utils import read_csv


class RealTimeDetectionApp:
    """Main application class for real-time detection processing."""
    
    def __init__(self, product_data_path: str, output_dir: str = "logs"):
        """Initialize the detection application."""
        self.product_data = read_csv(product_data_path)
        print(f"Loaded {len(self.product_data)} products from {product_data_path}")
        
        # Data buffers for different event types
        self.rfid_data = []
        self.pos_data = []
        self.queue_data = []
        self.product_recognition = []
        self.inventory_data = []
        
        # Detection results
        self.all_events = []
        self.event_lock = threading.Lock()
        
        # Configuration
        self.buffer_size = 1000  # Maximum events to keep in memory
        self.detection_interval = 5  # Run detection every N seconds
        self.output_dir = output_dir
        
        # Statistics
        self.stats = {
            'total_events_processed': 0,
            'detection_runs': 0,
            'events_detected': 0,
            'start_time': None
        }
        
        # Track processed events to avoid duplicates
        self.processed_events = set()
        
    def add_event(self, event: Dict[str, Any]) -> None:
        """Add an event to the appropriate buffer based on dataset type."""
        dataset = event.get('dataset', '')
        payload = event.get('event', {})
        
        # Add timestamp and station_id to payload for compatibility
        payload['timestamp'] = event.get('timestamp', '')
        if 'station_id' not in payload:
            payload['station_id'] = payload.get('station_id', 'Unknown')
        
        with self.event_lock:
            if dataset == 'RFID_data':
                self.rfid_data.append(payload)
            elif dataset == 'POS_Transactions':
                self.pos_data.append(payload)
            elif dataset == 'Queue_monitor':
                self.queue_data.append(payload)
            elif dataset == 'Product_recognism':
                self.product_recognition.append(payload)
            elif dataset == 'Current_inventory_data':
                self.inventory_data.append(payload)
            
            # Keep only recent events to manage memory
            self._trim_buffers()
            
        self.stats['total_events_processed'] += 1
    
    def _trim_buffers(self) -> None:
        """Keep only the most recent events in each buffer."""
        for buffer_name in ['rfid_data', 'pos_data', 'queue_data', 'product_recognition']:
            buffer = getattr(self, buffer_name)
            if len(buffer) > self.buffer_size:
                setattr(self, buffer_name, buffer[-self.buffer_size:])
    
    def _deduplicate_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate events based on event content."""
        new_events = []
        for event in events:
            # Create a unique key for the event
            event_name = event['event_data']['event_name']
            timestamp = event['timestamp']
            station_id = event['event_data'].get('station_id', '')
            
            # Create specific keys based on event type
            if event_name == "Inventory Discrepancy":
                sku = event['event_data'].get('SKU', '')
                event_key = f"{event_name}_{sku}_{timestamp}"
            elif event_name == "Barcode Switching":
                customer_id = event['event_data'].get('customer_id', '')
                actual_sku = event['event_data'].get('actual_sku', '')
                event_key = f"{event_name}_{station_id}_{customer_id}_{actual_sku}_{timestamp}"
            else:
                event_key = f"{event_name}_{station_id}_{timestamp}"
            
            if event_key not in self.processed_events:
                self.processed_events.add(event_key)
                new_events.append(event)
        
        return new_events
    
    def run_detections(self) -> List[Dict[str, Any]]:
        """Run all detection algorithms on current data."""
        self.stats['detection_runs'] += 1
        
        print(f"\n--- Detection Run #{self.stats['detection_runs']} ---")
        print(f"Processing: {len(self.rfid_data)} RFID, {len(self.pos_data)} POS, {len(self.queue_data)} queue events")
        
        events = []
        
        try:
            # Scanner Avoidance Detection
            scan_avoid = detect_scan_avoidance(self.rfid_data, self.pos_data)
            print(f"  → Scanner Avoidance: {len(scan_avoid)} events")
            events.extend(scan_avoid)
            
            # Barcode Switching Detection
            barcode_switch = detect_barcode_switch(self.pos_data, self.rfid_data, self.product_data, self.product_recognition)
            print(f"  → Barcode Switching: {len(barcode_switch)} events")
            events.extend(barcode_switch)
            
            # Weight Discrepancy Detection
            weight_disc = detect_weight_discrepancy(self.pos_data, self.product_data)
            print(f"  → Weight Discrepancies: {len(weight_disc)} events")
            events.extend(weight_disc)
            
            # System Errors Detection
            sys_errors = detect_system_errors(self.pos_data, self.product_recognition, self.queue_data)
            print(f"  → System Errors: {len(sys_errors)} events")
            events.extend(sys_errors)
            
            # Long Queue Detection
            long_queues = detect_long_queue(self.queue_data)
            print(f"  → Long Queues: {len(long_queues)} events")
            events.extend(long_queues)
            
            # Extended Wait Detection
            extended_waits = detect_extended_wait(self.queue_data)
            print(f"  → Extended Wait Times: {len(extended_waits)} events")
            events.extend(extended_waits)
            
            # Inventory Discrepancy Detection - only run if we have product data
            if self.product_data:
                inventory_disc = detect_inventory_discrepancy(self.inventory_data, self.pos_data, self.product_data)
                print(f"  → Inventory Discrepancies: {len(inventory_disc)} events")
                events.extend(inventory_disc)
            else:
                print(f"  → Inventory Discrepancies: 0 events (no product data)")
            
            # Only add new events (deduplicate)
            new_events = self._deduplicate_events(events)
            self.stats['events_detected'] += len(new_events)
            print(f"  → New events detected: {len(new_events)} (total: {len(events)})")
            
        except Exception as e:
            print(f"  → Error running detections: {e}")
            import traceback
            traceback.print_exc()
        
        return events
    
    def save_events(self, events: List[Dict[str, Any]]) -> str:
        """Save events to a single JSONL file."""
        if not events:
            return ""
        
        # Create output directory if it doesn't exist
        output_dir = "output/test"
        os.makedirs(output_dir, exist_ok=True)
        
        # Use single file for all events
        output_file = os.path.join(output_dir, "events.jsonl")
        
        try:
            # Append to the file instead of overwriting
            with open(output_file, 'a') as f:
                for event in events:
                    f.write(json.dumps(event) + "\n")
            print(f"  → Appended {len(events)} events to {output_file}")
            return output_file
        except Exception as e:
            print(f"  → Error saving events: {e}")
            return ""
    
    def print_stats(self) -> None:
        """Print current statistics."""
        if self.stats['start_time']:
            runtime = time.time() - self.stats['start_time']
            print(f"\n--- Statistics ---")
            print(f"Runtime: {runtime:.1f} seconds")
            print(f"Events processed: {self.stats['total_events_processed']}")
            print(f"Detection runs: {self.stats['detection_runs']}")
            print(f"Events detected: {self.stats['events_detected']}")
            if self.stats['detection_runs'] > 0:
                print(f"Avg events per run: {self.stats['events_detected'] / self.stats['detection_runs']:.1f}")


def read_events(host: str, port: int) -> Iterator[dict]:
    """Read events from the streaming server."""
    with socket.create_connection((host, port)) as conn:
        # Treat socket as file-like for convenient line iteration.
        with conn.makefile("r", encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                yield json.loads(line)


def main() -> None:
    parser = argparse.ArgumentParser(description="Project Sentinel Real-time Detection System")
    parser.add_argument("--host", default="127.0.0.1", help="Stream server host")
    parser.add_argument("--port", type=int, default=8765, help="Stream server port")
    parser.add_argument("--product-data", default="../data/input/products_list.csv", 
                       help="Path to products CSV file")
    parser.add_argument("--output-dir", default="output/test", 
                       help="Directory to save detection results")
    parser.add_argument("--detection-interval", type=int, default=5,
                       help="Run detections every N seconds")
    parser.add_argument("--limit", type=int, default=0, 
                       help="Stop after N events (0 = unlimited)")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose output")
    args = parser.parse_args()

    # Initialize application
    app = RealTimeDetectionApp(args.product_data, args.output_dir)
    app.stats['start_time'] = time.time()
    
    print("="*60)
    print("PROJECT SENTINEL - REAL-TIME DETECTION SYSTEM")
    print("="*60)
    print(f"Stream server: {args.host}:{args.port}")
    print(f"Detection interval: {args.detection_interval} seconds")
    print(f"Output directory: {args.output_dir}")
    print(f"Product data: {args.product_data}")
    print("="*60)
    
    event_count = 0
    last_detection_time = time.time()
    
    try:
        for event in read_events(args.host, args.port):
            event_count += 1
            
            # Print event info if verbose
            if args.verbose:
                dataset = event.get('dataset', 'Unknown')
                sequence = event.get('sequence', 0)
                timestamp = event.get('timestamp', 'No timestamp')
                print(f"[{sequence:4d}] {dataset:20s} {timestamp}")
            
            # Add event to processor
            app.add_event(event)
            
            # Run detections periodically
            current_time = time.time()
            if current_time - last_detection_time >= args.detection_interval:
                events = app.run_detections()
                
                if events:
                    app.save_events(events)
                
                last_detection_time = current_time
            
            # Print periodic stats
            if event_count % 100 == 0:
                app.print_stats()
            
            # Check limit
            if args.limit and event_count >= args.limit:
                print(f"\nReached event limit of {args.limit}, stopping...")
                break
                
    except KeyboardInterrupt:
        print("\n\nStopping detection system...")
    except Exception as e:
        print(f"\nError processing stream: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Run final detection
        print("\n--- Final detection run ---")
        events = app.run_detections()
        if events:
            app.save_events(events)
        
        # Print final statistics
        app.print_stats()
        print("\nDetection system stopped.")


if __name__ == "__main__":
    main()
