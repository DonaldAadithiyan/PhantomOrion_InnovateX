import json
from datetime import datetime, timedelta
from collections import defaultdict
from .utils import read_jsonl, read_csv
import uuid
import os

# ---------------- Detection Functions ----------------

def detect_scan_avoidance(rfid_data, pos_data):
    """
    Detects items that left scan area without being scanned at POS.
    """
    pos_skus = {t['data']['sku']: t['data'].get('customer_id') for t in pos_data}
    events = []
    for tag in rfid_data:
        sku = tag['data']['sku']
        location = tag['data']['location']
        timestamp = tag['timestamp']
        station_id = tag.get("station_id", "Unknown")
        if location == "OUT_SCAN_AREA" and sku not in pos_skus:
            customer_id = pos_skus.get(sku)
            events.append({
                "timestamp": timestamp,
                "event_id": f"E{str(uuid.uuid4())[:5]}",
                "event_data": {
                    "event_name": "Scanner Avoidance",
                    "station_id": station_id,
                    "customer_id": customer_id,
                    "product_sku": sku
                }
            })
    return events


def detect_barcode_switch(pos_data, rfid_data, product_data, product_recognition):
    """
    Detects barcode switching by comparing:
    1. Scanned barcode price vs predicted product price
    2. RFID confirmation of actual item
    
    Deduplicates events by transaction timestamp + customer + SKU
    """
    # Create price lookup from product data
    product_prices = {p['SKU']: float(p['price']) for p in product_data}
    product_prices.update({p['barcode']: float(p['price']) for p in product_data})
    
    # Create RFID lookup: timestamp + station -> actual SKU
    rfid_by_time_station = {}
    for tag in rfid_data:
        timestamp = tag['timestamp']
        sku = tag['data']['sku']
        station_id = tag.get('station_id', 'Unknown')
        key = f"{timestamp}_{station_id}"
        rfid_by_time_station[key] = sku
    
    # Create product recognition lookup: timestamp + station -> predicted SKU
    predicted_by_time_station = {}
    for rec in product_recognition:
        timestamp = rec['timestamp']
        predicted_sku = rec['data']['predicted_product']
        station_id = rec.get('station_id', 'Unknown')
        key = f"{timestamp}_{station_id}"
        predicted_by_time_station[key] = predicted_sku
    
    events = []
    processed_transactions = set()
    
    for tx in pos_data:
        timestamp = tx['timestamp']
        scanned_barcode = tx['data']['barcode']
        scanned_price = float(tx['data']['price'])
        sku_pos = tx['data']['sku']
        station_id = tx.get("station_id", "Unknown")
        customer_id = tx['data'].get("customer_id", "Unknown")
        
        # Create unique transaction key to avoid duplicates
        tx_key = f"{timestamp}_{station_id}_{customer_id}_{sku_pos}"
        if tx_key in processed_transactions:
            continue
        processed_transactions.add(tx_key)
        
        # Get predicted product from recognition system
        lookup_key = f"{timestamp}_{station_id}"
        predicted_sku = predicted_by_time_station.get(lookup_key)
        actual_sku = rfid_by_time_station.get(lookup_key)
        
        # Method 1: Compare with RFID and predicted product
        if predicted_sku and actual_sku and scanned_barcode:
            predicted_price = product_prices.get(predicted_sku, 0)
            actual_price = product_prices.get(actual_sku, 0)
            barcode_price = product_prices.get(scanned_barcode, scanned_price)
            
            # Flag if scanned price < predicted/actual price (cheap barcode on expensive item)
            if barcode_price < predicted_price and actual_sku != scanned_barcode:
                events.append({
                    "timestamp": timestamp,
                    "event_id": f"E{str(uuid.uuid4())[:8]}",
                    "event_data": {
                        "event_name": "Barcode Switching",
                        "station_id": station_id,
                        "customer_id": customer_id,
                        "actual_sku": actual_sku,
                        "scanned_barcode": scanned_barcode,
                        "scanned_price": barcode_price,
                        "actual_price": actual_price,
                        "price_difference": actual_price - barcode_price
                    }
                })
        # Method 2: Fallback - simple SKU vs barcode comparison
        elif sku_pos and scanned_barcode and sku_pos != scanned_barcode:
            sku_price = product_prices.get(sku_pos, 0)
            barcode_price = product_prices.get(scanned_barcode, scanned_price)
            
            # Only flag if there's a price difference (not just different codes)
            if sku_price != barcode_price:
                events.append({
                    "timestamp": timestamp,
                    "event_id": f"E{str(uuid.uuid4())[:8]}",
                    "event_data": {
                        "event_name": "Barcode Switching",
                        "station_id": station_id,
                        "customer_id": customer_id,
                        "actual_sku": sku_pos,
                        "scanned_barcode": scanned_barcode,
                        "scanned_price": barcode_price,
                        "actual_price": sku_price,
                        "price_difference": sku_price - barcode_price
                    }
                })
    
    print(f"  → Processed {len(processed_transactions)} unique transactions")
    return events


def detect_weight_discrepancy(pos_data, product_data, threshold=5):
    """
    Detects when actual weight differs from expected weight by more than threshold.
    """
    product_weights = {p['SKU']: float(p['weight']) for p in product_data}
    events = []
    for tx in pos_data:
        sku = tx['data']['sku']
        actual_weight = float(tx['data']['weight_g'])
        expected_weight = product_weights.get(sku, 0)
        timestamp = tx['timestamp']
        station_id = tx.get("station_id", "Unknown")
        customer_id = tx['data'].get("customer_id")
        if abs(actual_weight - expected_weight) > threshold:
            events.append({
                "timestamp": timestamp,
                "event_id": f"E{str(uuid.uuid4())[:5]}",
                "event_data": {
                    "event_name": "Weight Discrepancies",
                    "station_id": station_id,
                    "customer_id": customer_id,
                    "product_sku": sku,
                    "expected_weight": expected_weight,
                    "actual_weight": actual_weight,
                    "difference": abs(actual_weight - expected_weight)
                }
            })
    return events


def detect_system_errors(*datasets, interval_minutes=10, recurring_threshold=3):
    """
    Detects system errors and aggregates them to identify recurring failures.
    
    Args:
        datasets: Multiple datasets to check for errors
        interval_minutes: Time window for aggregation (default 10 minutes)
        recurring_threshold: Minimum errors in interval to flag as recurring (default 3)
    """
    events = []
    error_aggregation = defaultdict(lambda: defaultdict(int))
    
    # Collect all errors
    for data in datasets:
        for record in data:
            if record['status'] in ["Read Error", "System Crash"]:
                timestamp = record['timestamp']
                station_id = record.get("station_id", "Unknown")
                
                # Parse timestamp and round to interval
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    interval_key = dt.replace(second=0, microsecond=0)
                    interval_key = interval_key - timedelta(minutes=interval_key.minute % interval_minutes)
                    
                    # Aggregate by station and interval
                    error_aggregation[station_id][interval_key] += 1
                except:
                    pass
                
                # Log individual error
                events.append({
                    "timestamp": timestamp,
                    "event_id": f"E{str(uuid.uuid4())[:5]}",
                    "event_data": {
                        "event_name": "System Error",
                        "station_id": station_id,
                        "error_type": record['status'],
                        "duration_seconds": record.get("duration_seconds", None)
                    }
                })
    
    # Identify recurring failures
    for station_id, intervals in error_aggregation.items():
        for interval_time, error_count in intervals.items():
            if error_count >= recurring_threshold:
                events.append({
                    "timestamp": interval_time.isoformat(),
                    "event_id": f"E{str(uuid.uuid4())[:5]}",
                    "event_data": {
                        "event_name": "Recurring System Failures",
                        "station_id": station_id,
                        "error_count": error_count,
                        "interval_minutes": interval_minutes,
                        "severity": "HIGH" if error_count >= recurring_threshold * 2 else "MEDIUM"
                    }
                })
    
    return events


def detect_long_queue(queue_data, count_threshold=5, duration_threshold_seconds=120):
    """
    Detects long queues that persist for a continuous duration.
    
    Args:
        queue_data: Queue monitoring data
        count_threshold: Minimum customers to consider "long" (default 5)
        duration_threshold_seconds: Minimum duration to flag (default 120s = 2 min)
    """
    events = []
    
    # Group by station
    stations = defaultdict(list)
    for q in queue_data:
        station_id = q['station_id']
        stations[station_id].append(q)
    
    # Check each station for continuous long queues
    for station_id, records in stations.items():
        # Sort by timestamp
        records.sort(key=lambda x: x['timestamp'])
        
        long_queue_start = None
        long_queue_duration = 0
        flagged_intervals = set()
        
        for record in records:
            customer_count = record['data']['customer_count']
            timestamp = record['timestamp']
            
            if customer_count > count_threshold:
                if long_queue_start is None:
                    # Start of long queue period
                    try:
                        long_queue_start = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except:
                        continue
                else:
                    # Calculate duration
                    try:
                        current_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        long_queue_duration = (current_time - long_queue_start).total_seconds()
                        
                        # Flag if exceeds duration threshold and not already flagged
                        interval_key = (station_id, long_queue_start.isoformat())
                        if long_queue_duration >= duration_threshold_seconds and interval_key not in flagged_intervals:
                            events.append({
                                "timestamp": timestamp,
                                "event_id": f"E{str(uuid.uuid4())[:5]}",
                                "event_data": {
                                    "event_name": "Long Queue Length",
                                    "station_id": station_id,
                                    "num_of_customers": customer_count,
                                    "queue_duration_seconds": long_queue_duration,
                                    "average_dwell_time": record['data'].get('average_dwell_time')
                                }
                            })
                            flagged_intervals.add(interval_key)
                    except:
                        continue
            else:
                # Queue dropped below threshold, reset
                long_queue_start = None
                long_queue_duration = 0
    
    return events


def detect_extended_wait(queue_data, dwell_threshold=300):
    """
    Detects when customers experience extended wait times.
    Correlates with queue length to prioritize staffing.
    """
    events = []
    for q in queue_data:
        avg_dwell = q['data']['average_dwell_time']
        if avg_dwell > dwell_threshold:
            customer_count = q['data']['customer_count']
            # Calculate priority based on both wait time and queue length
            priority = "HIGH" if (avg_dwell > dwell_threshold * 1.5 and customer_count > 5) else "MEDIUM"
            
            events.append({
                "timestamp": q['timestamp'],
                "event_id": f"E{str(uuid.uuid4())[:5]}",
                "event_data": {
                    "event_name": "Long Wait Time",
                    "station_id": q['station_id'],
                    "wait_time_seconds": avg_dwell,
                    "customer_count": customer_count,
                    "priority": priority
                }
            })
    return events


def detect_inventory_discrepancy(current_inventory, pos_data, product_data, tolerance=1):
    """
    Detects inventory discrepancies by comparing expected vs actual inventory.
    Expected = initial_inventory - sold_qty
    Note: This simplified version doesn't track received stock (would need additional data source)
    
    IMPORTANT: Only processes the MOST RECENT inventory snapshot to avoid duplicates
    """
    # current_inventory is a list, get the most recent snapshot
    if not current_inventory:
        return []
    
    # Use the last (most recent) inventory snapshot ONLY
    latest_inventory = current_inventory[-1]
    
    # Start with initial inventory from product data
    expected_inventory = {p['SKU']: int(p['quantity']) for p in product_data}
    
    # Subtract POS transactions (sold quantities)
    sales_count = defaultdict(int)
    for tx in pos_data:
        sku = tx['data']['sku']
        sales_count[sku] += 1
    
    for sku, sold_qty in sales_count.items():
        if sku in expected_inventory:
            expected_inventory[sku] -= sold_qty

    events = []
    processed_skus = set()
    
    for sku, actual_qty in latest_inventory['data'].items():
        # Avoid duplicate processing
        if sku in processed_skus:
            continue
        processed_skus.add(sku)
        
        expected_qty = expected_inventory.get(sku, 0)
        discrepancy = abs(expected_qty - actual_qty)
        
        if discrepancy > tolerance:
            # Determine if it's shrinkage (missing) or overage
            discrepancy_type = "Shrinkage" if actual_qty < expected_qty else "Overage"
            
            events.append({
                "timestamp": latest_inventory.get("timestamp", "Unknown"),
                "event_id": f"E{str(uuid.uuid4())[:8]}",
                "event_data": {
                    "event_name": "Inventory Discrepancy",
                    "SKU": sku,
                    "Expected_Inventory": expected_qty,
                    "Actual_Inventory": actual_qty,
                    "Discrepancy": discrepancy,
                    "Type": discrepancy_type,
                    "Units_Sold": sales_count.get(sku, 0)
                }
            })
    
    print(f"  → Processed {len(processed_skus)} unique SKUs from inventory snapshot")
    return events


# ---------------- Main Execution ----------------

if __name__ == "__main__":
    print("Loading data files...")
    rfid_data = read_jsonl('data/input/rfid_readings.jsonl')
    queue_data = read_jsonl('data/input/queue_monitoring.jsonl')
    pos_data = read_jsonl('data/input/pos_transactions.jsonl')
    product_recognition = read_jsonl('data/input/product_recognition.jsonl')
    current_inventory = read_jsonl('data/input/inventory_snapshots.jsonl')
    product_data = read_csv('data/input/products_list.csv')
    
    print(f"Loaded: {len(rfid_data)} RFID records")
    print(f"Loaded: {len(queue_data)} queue records")
    print(f"Loaded: {len(pos_data)} POS transactions")
    print(f"Loaded: {len(product_recognition)} product recognition records")
    print(f"Loaded: {len(current_inventory)} inventory snapshots")
    print(f"Loaded: {len(product_data)} products")
    
    print("\nRunning detection algorithms...")
    all_events = []
    
    scan_avoid = detect_scan_avoidance(rfid_data, pos_data)
    print(f"- Scanner Avoidance: {len(scan_avoid)} events")
    all_events.extend(scan_avoid)
    
    barcode_switch = detect_barcode_switch(pos_data, rfid_data, product_data, product_recognition)
    print(f"- Barcode Switching: {len(barcode_switch)} events")
    all_events.extend(barcode_switch)
    
    weight_disc = detect_weight_discrepancy(pos_data, product_data)
    print(f"- Weight Discrepancies: {len(weight_disc)} events")
    all_events.extend(weight_disc)
    
    sys_errors = detect_system_errors(pos_data, product_recognition, queue_data)
    print(f"- System Errors: {len(sys_errors)} events")
    all_events.extend(sys_errors)
    
    long_queues = detect_long_queue(queue_data)
    print(f"- Long Queues: {len(long_queues)} events")
    all_events.extend(long_queues)
    
    extended_waits = detect_extended_wait(queue_data)
    print(f"- Extended Wait Times: {len(extended_waits)} events")
    all_events.extend(extended_waits)
    
    inventory_disc = detect_inventory_discrepancy(current_inventory, pos_data, product_data)
    print(f"- Inventory Discrepancies: {len(inventory_disc)} events")
    all_events.extend(inventory_disc)
    
    print(f"\nTotal events detected: {len(all_events)}")
    
    # Deduplicate events based on timestamp + event_name + key identifiers
    print("\nDeduplicating events...")
    unique_events = []
    seen_events = set()
    
    for event in all_events:
        # Create a unique key for each event
        event_name = event['event_data']['event_name']
        timestamp = event['timestamp']
        station_id = event['event_data'].get('station_id', '')
        
        # Create specific keys based on event type
        if event_name == "Inventory Discrepancy":
            sku = event['event_data'].get('SKU', '')
            event_key = f"{timestamp}_{event_name}_{sku}"
        elif event_name == "Barcode Switching":
            customer_id = event['event_data'].get('customer_id', '')
            actual_sku = event['event_data'].get('actual_sku', '')
            event_key = f"{timestamp}_{event_name}_{station_id}_{customer_id}_{actual_sku}"
        else:
            event_key = f"{timestamp}_{event_name}_{station_id}"
        
        if event_key not in seen_events:
            seen_events.add(event_key)
            unique_events.append(event)
    
    print(f"After deduplication: {len(unique_events)} unique events")
    
    # Sort events by timestamp
    try:
        unique_events.sort(key=lambda x: x['timestamp'])
        print("Events sorted by timestamp")
    except:
        print("Warning: Could not sort events by timestamp")
    
    # Export as JSON lines
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create logs directory if it doesn't exist
    logs_dir = 'logs'
    os.makedirs(logs_dir, exist_ok=True)
    
    output_file = f"{logs_dir}/detection_events_{timestamp}.jsonl"
    with open(output_file, 'w') as f:
        for event in unique_events:
            f.write(json.dumps(event) + "\n")
    
    print(f"\n✓ Detection events exported to {output_file}")
    
    # Print summary by event type
    print("\n" + "="*60)
    print("EVENT SUMMARY BY TYPE:")
    print("="*60)
    event_counts = defaultdict(int)
    for event in unique_events:
        event_name = event['event_data']['event_name']
        event_counts[event_name] += 1
    
    for event_name, count in sorted(event_counts.items()):
        print(f"  {event_name}: {count} events")
    print("="*60)