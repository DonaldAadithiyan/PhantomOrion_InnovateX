import json
from datetime import datetime, timedelta
from collections import defaultdict
import uuid
import os

# Global state for real-time processing
pos_skus_cache = {}
rfid_cache = {}
product_recognition_cache = {}
product_data_cache = {}
error_aggregation = defaultdict(lambda: defaultdict(int))
queue_state = defaultdict(lambda: {'start_time': None, 'flagged': set()})
sales_count = defaultdict(int)

# ============================================================================
# REAL-TIME DETECTION FUNCTIONS (Process one row at a time)
# ============================================================================

def detect_scan_avoidance_row(rfid_row):
    """
    Process one RFID reading row in real-time.
    Flags if item leaves scan area without POS record.
    """
    sku = rfid_row['data']['sku']
    location = rfid_row['data']['location']
    timestamp = rfid_row['timestamp']
    station_id = rfid_row.get("station_id", "Unknown")
    
    # Check if item left scan area and not in POS
    if location == "OUT_SCAN_AREA" and sku not in pos_skus_cache:
        event = {
            "timestamp": timestamp,
            "event_id": f"E{str(uuid.uuid4())[:8]}",
            "event_data": {
                "event_name": "Scanner Avoidance",
                "station_id": station_id,
                "customer_id": pos_skus_cache.get(sku),
                "product_sku": sku
            }
        }
        log_event(event)
        return event
    
    return None


def detect_barcode_switch_row(pos_row):
    """
    Process one POS transaction row in real-time.
    Flags if barcode price < actual product price.
    """
    timestamp = pos_row['timestamp']
    scanned_barcode = pos_row['data']['barcode']
    scanned_price = float(pos_row['data']['price'])
    sku_pos = pos_row['data']['sku']
    station_id = pos_row.get("station_id", "Unknown")
    customer_id = pos_row['data'].get("customer_id", "Unknown")
    
    # Update POS cache
    pos_skus_cache[sku_pos] = customer_id
    
    # Get prices from cache
    sku_price = product_data_cache.get(sku_pos, {}).get('price', 0)
    barcode_price = product_data_cache.get(scanned_barcode, {}).get('price', scanned_price)
    
    # Get RFID/predicted data
    lookup_key = f"{timestamp}_{station_id}"
    predicted_sku = product_recognition_cache.get(lookup_key)
    actual_sku = rfid_cache.get(lookup_key)
    
    # Method 1: Compare with RFID and predicted product
    if predicted_sku and actual_sku:
        predicted_price = product_data_cache.get(predicted_sku, {}).get('price', 0)
        actual_price = product_data_cache.get(actual_sku, {}).get('price', 0)
        
        if barcode_price < predicted_price and actual_sku != scanned_barcode:
            event = {
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
            }
            log_event(event)
            return event
    
    # Method 2: Fallback comparison
    elif sku_pos != scanned_barcode and sku_price != barcode_price and sku_price > 0:
        event = {
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
        }
        log_event(event)
        return event
    
    return None


def detect_weight_discrepancy_row(pos_row):
    """
    Process one POS transaction row in real-time.
    Flags if weight differs from expected by > threshold.
    """
    sku = pos_row['data']['sku']
    actual_weight = float(pos_row['data']['weight_g'])
    timestamp = pos_row['timestamp']
    station_id = pos_row.get("station_id", "Unknown")
    customer_id = pos_row['data'].get("customer_id")
    
    expected_weight = product_data_cache.get(sku, {}).get('weight', 0)
    threshold = 5
    
    if abs(actual_weight - expected_weight) > threshold:
        event = {
            "timestamp": timestamp,
            "event_id": f"E{str(uuid.uuid4())[:8]}",
            "event_data": {
                "event_name": "Weight Discrepancies",
                "station_id": station_id,
                "customer_id": customer_id,
                "product_sku": sku,
                "expected_weight": expected_weight,
                "actual_weight": actual_weight,
                "difference": abs(actual_weight - expected_weight)
            }
        }
        log_event(event)
        return event
    
    return None


def detect_system_error_row(data_row, interval_minutes=10, recurring_threshold=3):
    """
    Process one data row (from any source) in real-time.
    Flags system errors and identifies recurring failures.
    """
    if data_row['status'] not in ["Read Error", "System Crash"]:
        return None
    
    timestamp = data_row['timestamp']
    station_id = data_row.get("station_id", "Unknown")
    
    # Log individual error
    event = {
        "timestamp": timestamp,
        "event_id": f"E{str(uuid.uuid4())[:8]}",
        "event_data": {
            "event_name": "System Error",
            "station_id": station_id,
            "error_type": data_row['status'],
            "duration_seconds": data_row.get("duration_seconds", None)
        }
    }
    log_event(event)
    
    # Aggregate for recurring detection
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        interval_key = dt.replace(second=0, microsecond=0)
        interval_key = interval_key - timedelta(minutes=interval_key.minute % interval_minutes)
        
        error_aggregation[station_id][interval_key] += 1
        
        # Check if recurring threshold reached
        if error_aggregation[station_id][interval_key] == recurring_threshold:
            recurring_event = {
                "timestamp": interval_key.isoformat(),
                "event_id": f"E{str(uuid.uuid4())[:8]}",
                "event_data": {
                    "event_name": "Recurring System Failures",
                    "station_id": station_id,
                    "error_count": recurring_threshold,
                    "interval_minutes": interval_minutes,
                    "severity": "MEDIUM"
                }
            }
            log_event(recurring_event)
    except:
        pass
    
    return event


def detect_long_queue_row(queue_row, count_threshold=5, duration_threshold_seconds=120):
    """
    Process one queue monitoring row in real-time.
    Flags if queue exceeds threshold for continuous duration.
    """
    customer_count = queue_row['data']['customer_count']
    timestamp = queue_row['timestamp']
    station_id = queue_row['station_id']
    
    try:
        current_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    except:
        return None
    
    if customer_count > count_threshold:
        # Start tracking if not already
        if queue_state[station_id]['start_time'] is None:
            queue_state[station_id]['start_time'] = current_time
        else:
            # Calculate duration
            duration = (current_time - queue_state[station_id]['start_time']).total_seconds()
            
            # Flag if exceeds threshold and not already flagged
            interval_key = queue_state[station_id]['start_time'].isoformat()
            if duration >= duration_threshold_seconds and interval_key not in queue_state[station_id]['flagged']:
                event = {
                    "timestamp": timestamp,
                    "event_id": f"E{str(uuid.uuid4())[:8]}",
                    "event_data": {
                        "event_name": "Long Queue Length",
                        "station_id": station_id,
                        "num_of_customers": customer_count,
                        "queue_duration_seconds": duration,
                        "average_dwell_time": queue_row['data'].get('average_dwell_time')
                    }
                }
                log_event(event)
                queue_state[station_id]['flagged'].add(interval_key)
                return event
    else:
        # Reset tracking
        queue_state[station_id]['start_time'] = None
    
    return None


def detect_extended_wait_row(queue_row, dwell_threshold=300):
    """
    Process one queue monitoring row in real-time.
    Flags if average dwell time exceeds threshold.
    """
    avg_dwell = queue_row['data']['average_dwell_time']
    
    if avg_dwell > dwell_threshold:
        customer_count = queue_row['data']['customer_count']
        priority = "HIGH" if (avg_dwell > dwell_threshold * 1.5 and customer_count > 5) else "MEDIUM"
        
        event = {
            "timestamp": queue_row['timestamp'],
            "event_id": f"E{str(uuid.uuid4())[:8]}",
            "event_data": {
                "event_name": "Long Wait Time",
                "station_id": queue_row['station_id'],
                "wait_time_seconds": avg_dwell,
                "customer_count": customer_count,
                "priority": priority
            }
        }
        log_event(event)
        return event
    
    return None


def detect_inventory_discrepancy_row(inventory_row, tolerance=1):
    """
    Process one inventory snapshot row in real-time.
    Compares expected vs actual inventory per SKU.
    """
    events = []
    timestamp = inventory_row.get("timestamp", "Unknown")
    
    for sku, actual_qty in inventory_row['data'].items():
        # Get expected quantity from product cache
        expected_qty = product_data_cache.get(sku, {}).get('quantity', 0)
        
        # Subtract sales
        expected_qty -= sales_count.get(sku, 0)
        
        discrepancy = abs(expected_qty - actual_qty)
        
        if discrepancy > tolerance:
            discrepancy_type = "Shrinkage" if actual_qty < expected_qty else "Overage"
            
            event = {
                "timestamp": timestamp,
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
            }
            log_event(event)
            events.append(event)
    
    return events if events else None


# ============================================================================
# CACHE UPDATE FUNCTIONS (Update state as data arrives)
# ============================================================================

def update_rfid_cache(rfid_row):
    """Update RFID cache when new reading arrives."""
    timestamp = rfid_row['timestamp']
    sku = rfid_row['data']['sku']
    station_id = rfid_row.get('station_id', 'Unknown')
    key = f"{timestamp}_{station_id}"
    rfid_cache[key] = sku


def update_product_recognition_cache(recognition_row):
    """Update product recognition cache when new prediction arrives."""
    timestamp = recognition_row['timestamp']
    predicted_sku = recognition_row['data']['predicted_product']
    station_id = recognition_row.get('station_id', 'Unknown')
    key = f"{timestamp}_{station_id}"
    product_recognition_cache[key] = predicted_sku


def update_pos_cache(pos_row):
    """Update sales count when new POS transaction arrives."""
    sku = pos_row['data']['sku']
    sales_count[sku] += 1


def load_product_data(product_data):
    """Load product catalog into cache (one-time setup)."""
    for p in product_data:
        sku = p['SKU']
        product_data_cache[sku] = {
            'price': float(p.get('price', 0)),
            'weight': float(p.get('weight', 0)),
            'quantity': int(p.get('quantity', 0))
        }
        # Also cache by barcode
        barcode = p.get('barcode')
        if barcode:
            product_data_cache[barcode] = product_data_cache[sku].copy()


# ============================================================================
# EVENT LOGGING
# ============================================================================

def log_event(event):
    """
    Log detected event to file in real-time.
    Appends to log file immediately when event is flagged.
    """
    logs_dir = 'logs'
    os.makedirs(logs_dir, exist_ok=True)
    
    log_file = f"{logs_dir}/realtime_events.jsonl"
    
    with open(log_file, 'a') as f:
        f.write(json.dumps(event) + "\n")
    
    # Also print to console for monitoring
    event_name = event['event_data']['event_name']
    station = event['event_data'].get('station_id', 'N/A')
    print(f"[{event['timestamp']}] {event_name} at {station}")


# ============================================================================
# REAL-TIME PROCESSING PIPELINE
# ============================================================================

def process_incoming_row(row, data_source):
    """
    Main entry point for processing incoming data rows in real-time.
    
    Args:
        row: Single data row (dict)
        data_source: Type of data ('rfid', 'pos', 'queue', 'product_recognition', 'inventory')
    """
    if data_source == 'rfid':
        update_rfid_cache(row)
        return detect_scan_avoidance_row(row)
    
    elif data_source == 'pos':
        update_pos_cache(row)
        events = []
        barcode_event = detect_barcode_switch_row(row)
        weight_event = detect_weight_discrepancy_row(row)
        error_event = detect_system_error_row(row)
        if barcode_event: events.append(barcode_event)
        if weight_event: events.append(weight_event)
        if error_event: events.append(error_event)
        return events if events else None
    
    elif data_source == 'queue':
        events = []
        long_queue = detect_long_queue_row(row)
        extended_wait = detect_extended_wait_row(row)
        error_event = detect_system_error_row(row)
        if long_queue: events.append(long_queue)
        if extended_wait: events.append(extended_wait)
        if error_event: events.append(error_event)
        return events if events else None
    
    elif data_source == 'product_recognition':
        update_product_recognition_cache(row)
        return detect_system_error_row(row)
    
    elif data_source == 'inventory':
        return detect_inventory_discrepancy_row(row)
    
    return None


# ============================================================================
# EXAMPLE USAGE (Simulating real-time stream)
# ============================================================================

if __name__ == "__main__":
    from utils import read_jsonl, read_csv
    
    print("="*70)
    print("REAL-TIME DETECTION SYSTEM - Processing rows as they arrive")
    print("="*70)
    
    # One-time setup: Load product catalog
    print("\n[SETUP] Loading product catalog...")
    product_data = read_csv('data/input/products_list.csv')
    load_product_data(product_data)
    print(f"[SETUP] Loaded {len(product_data_cache)} products into cache")
    
    # Clear previous log
    if os.path.exists('logs/realtime_events.jsonl'):
        os.remove('logs/realtime_events.jsonl')
        print("[SETUP] Cleared previous event log\n")
    
    print("="*70)
    print("PROCESSING INCOMING DATA STREAM...")
    print("="*70 + "\n")
    
    # Simulate real-time processing (in production, this would be a stream)
    rfid_data = read_jsonl('data/input/rfid_readings.jsonl')
    pos_data = read_jsonl('data/input/pos_transactions.jsonl')
    queue_data = read_jsonl('data/input/queue_monitoring.jsonl')
    product_recognition = read_jsonl('data/input/product_recognition.jsonl')
    inventory_data = read_jsonl('data/input/inventory_snapshots.jsonl')
    
    # Process each row as it "arrives"
    event_count = 0
    
    for row in rfid_data:
        result = process_incoming_row(row, 'rfid')
        if result: event_count += 1
    
    for row in pos_data:
        result = process_incoming_row(row, 'pos')
        if result: event_count += (len(result) if isinstance(result, list) else 1)
    
    for row in queue_data:
        result = process_incoming_row(row, 'queue')
        if result: event_count += (len(result) if isinstance(result, list) else 1)
    
    for row in product_recognition:
        result = process_incoming_row(row, 'product_recognition')
        if result: event_count += 1
    
    for row in inventory_data:
        result = process_incoming_row(row, 'inventory')
        if result: event_count += (len(result) if isinstance(result, list) else 1)
    
    print("\n" + "="*70)
    print(f"PROCESSING COMPLETE - {event_count} events logged")
    print("="*70)
    print(f"\n✓ Events logged to: logs/realtime_events.jsonl")