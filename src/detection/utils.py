import json
import csv


def read_jsonl(filepath):
    """
    Read a JSONL (JSON Lines) file and return a list of dictionaries.
    
    Args:
        filepath (str): Path to the JSONL file
        
    Returns:
        list: List of dictionaries, one per line
    """
    data = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    data.append(json.loads(line))
        return data
    except FileNotFoundError:
        print(f"Error: File not found - {filepath}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {filepath} - {e}")
        return []


def read_csv(filepath):
    """
    Read a CSV file and return a list of dictionaries.
    
    Args:
        filepath (str): Path to the CSV file
        
    Returns:
        list: List of dictionaries with column headers as keys
    """
    data = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        return data
    except FileNotFoundError:
        print(f"Error: File not found - {filepath}")
        return []
    except Exception as e:
        print(f"Error reading CSV file {filepath} - {e}")
        return []


def write_jsonl(filepath, data):
    """
    Write a list of dictionaries to a JSONL file.
    
    Args:
        filepath (str): Path to the output JSONL file
        data (list): List of dictionaries to write
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')
        print(f"Successfully wrote {len(data)} records to {filepath}")
    except Exception as e:
        print(f"Error writing to file {filepath} - {e}")