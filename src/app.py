#!/usr/bin/env python3
"""
Data Streaming and Batching Application

Reads data from streaming server, groups by timestamp and station_id, creates batches.
"""

import argparse
import json
import socket
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, Tuple


def connect_to_stream(host="localhost", port=8765):
    """Connect to the streaming server."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        print(f"Connected to streaming server at {host}:{port}")
        return sock
    except Exception as e:
        print(f"Failed to connect to streaming server: {e}")
        return None


def process_stream_realtime(sock, max_events=None):
    """Process stream data in real-time, creating batches as data arrives."""
    buffer = ""
    event_count = 0
    current_batches = {}  # (timestamp, station_id) -> batch_data
    batch_counter = 0
    last_activity = time.time()
    
    print("Starting real-time stream processing...")
    print("Press Ctrl+C to stop processing\n")
    
    # Set socket timeout for non-blocking reads
    sock.settimeout(1.0)  # 1 second timeout
    
    try:
        while True:
            try:
                data = sock.recv(1024).decode('utf-8')
                if not data:
                    break
                    
                last_activity = time.time()
                buffer += data
                
                if '\n' in buffer:
                    lines = buffer.split('\n')
                    buffer = lines[-1]
                    
                    for line in lines[:-1]:
                        if line.strip():
                            try:
                                event = json.loads(line)
                                # Skip banner messages
                                if 'service' in event:
                                    print(f"Server info: {event.get('datasets', [])} datasets, {event.get('events', 0)} events")
                                    continue
                                
                                # Process this event immediately
                                process_single_event(event, current_batches, batch_counter)
                                event_count += 1
                                
                                # Check if we've reached the max_events limit
                                if max_events is not None and event_count >= max_events:
                                    print(f"\nReached max events limit ({max_events}). Processing remaining batches...")
                                    # Process any remaining batches
                                    process_remaining_batches(current_batches, batch_counter)
                                    return
                                    
                            except json.JSONDecodeError:
                                continue
                                
            except socket.timeout:
                # No data received, check if we should process pending batches
                current_time = time.time()
                if current_time - last_activity > 2.0:  # 2 seconds of inactivity
                    # Process any pending batches
                    if current_batches:
                        print(f"\nProcessing {len(current_batches)} pending batches due to inactivity...")
                        batch_counter = process_remaining_batches(current_batches, batch_counter)
                        current_batches.clear()
                continue
                
    except KeyboardInterrupt:
        print(f"\nProcessing interrupted by user after {event_count} events")
    except Exception as e:
        print(f"Error reading from stream: {e}")
    finally:
        # Process any remaining batches
        if current_batches:
            print(f"\nProcessing remaining {len(current_batches)} batches...")
            process_remaining_batches(current_batches, batch_counter)
        
        print(f"\nTotal events processed: {event_count}")


def process_single_event(event, current_batches, batch_counter):
    """Process a single event and add it to the appropriate batch."""
    # Extract timestamp and station_id
    timestamp = event.get('timestamp', '')
    event_data = event.get('event', {})
    station_id = event_data.get('station_id', 'unknown')
    
    # Create batch key
    batch_key = (timestamp, station_id)
    
    # Initialize batch if it doesn't exist
    if batch_key not in current_batches:
        current_batches[batch_key] = {
            'timestamp': timestamp,
            'station_id': station_id,
            'events': [],
            'datasets': set()
        }
    
    # Add event to batch
    current_batches[batch_key]['events'].append(event)
    current_batches[batch_key]['datasets'].add(event.get('dataset', 'unknown'))
    
    # Check if this batch is complete (has events from all expected datasets for this timestamp)
    # For now, we'll process batches when they have multiple events or after a short delay
    # This is a simple heuristic - you could make it more sophisticated
    batch = current_batches[batch_key]
    
    # If this is a new timestamp, process any previous batches
    if len(current_batches) > 1:
        # Find batches from previous timestamps
        current_timestamp = timestamp
        batches_to_process = []
        
        for key, batch_data in list(current_batches.items()):
            if batch_data['timestamp'] != current_timestamp:
                batches_to_process.append((key, batch_data))
                del current_batches[key]
        
        # Process completed batches
        for key, batch_data in batches_to_process:
            print_batch_realtime(batch_data, batch_counter)
            batch_counter += 1


def process_remaining_batches(current_batches, batch_counter):
    """Process any remaining batches."""
    for batch_data in current_batches.values():
        print_batch_realtime(batch_data, batch_counter)
        batch_counter += 1
    return batch_counter


def print_batch_realtime(batch, batch_number):
    """Print a single batch in real-time."""
    print(f"BATCH {batch_number + 1}:")
    print(f"  Timestamp: {batch['timestamp']}")
    print(f"  Station ID: {batch['station_id']}")
    print(f"  Event Count: {len(batch['events'])}")
    print(f"  Datasets: {', '.join(batch['datasets'])}")
    print(f"  Events:")
    
    for j, event in enumerate(batch['events'], 1):
        dataset = event.get('dataset', 'unknown')
        event_data = event.get('event', {})
        print(f"    {j}. Dataset: {dataset}")
        # print(f"       Data: {json.dumps(event_data, indent=8)}")
    
    print(f"  {'-'*60}")
    print()




def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Data Streaming and Batching Application",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Maximum number of events to process (unlimited if not specified)"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Streaming server host"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Streaming server port"
    )
    return parser.parse_args()


def main():
    """Main function."""
    args = parse_args()
    
    print("Starting real-time data stream processing...")
    if args.max_events:
        print(f"Processing up to {args.max_events} events")
    else:
        print("Processing unlimited events (until stream ends)")
    
    # Connect to streaming server
    sock = connect_to_stream(host=args.host, port=args.port)
    if not sock:
        print("Cannot connect to streaming server. Make sure it's running.")
        print("Run: python data/streaming-server/stream_server.py")
        return
    
    try:
        # Process stream in real-time
        process_stream_realtime(sock, max_events=args.max_events)
        
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        sock.close()
        print("Connection closed")


if __name__ == "__main__":
    main()