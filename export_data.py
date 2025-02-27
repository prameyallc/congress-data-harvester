#!/usr/bin/env python3

"""
Export congressional data from DynamoDB to JSON or CSV files.
This tool provides quick access to downloaded data for external analysis.
"""

import os
import sys
import json
import csv
import argparse
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from dynamo_handler import DynamoHandler, DecimalEncoder

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('export_data')

def load_config():
    """Load application configuration"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Error: config.json not found")
        return None
    except json.JSONDecodeError:
        logger.error("Error: Invalid JSON in config.json")
        return None

def export_to_json(data, output_file):
    """Export data to a JSON file"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(data, f, cls=DecimalEncoder, indent=2)
        
        logger.info(f"Successfully exported {len(data)} records to {output_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to export to JSON: {str(e)}")
        return False

def export_to_csv(data, output_file):
    """Export data to a CSV file"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        if not data:
            logger.warning("No data to export to CSV")
            return False
        
        # Get all possible field names from all records
        fieldnames = set()
        for item in data:
            fieldnames.update(item.keys())
        
        # Prioritize and order important fields first
        priority_fields = ['id', 'type', 'congress', 'update_date', 'title', 'number', 'chamber']
        ordered_fields = []
        
        # Add priority fields first (if they exist in the data)
        for field in priority_fields:
            if field in fieldnames:
                ordered_fields.append(field)
                fieldnames.remove(field)
        
        # Add remaining fields in alphabetical order
        ordered_fields.extend(sorted(fieldnames))
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_fields)
            writer.writeheader()
            
            # Convert complex nested objects to strings for CSV compatibility
            for item in data:
                row = {}
                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        row[key] = json.dumps(value, cls=DecimalEncoder)
                    else:
                        row[key] = value
                writer.writerow(row)
        
        logger.info(f"Successfully exported {len(data)} records to {output_file}")
        return True
    except Exception as e:
        logger.error(f"Failed to export to CSV: {str(e)}")
        return False

def get_data_from_dynamodb(config, data_type=None, congress=None, start_date=None, end_date=None):
    """Get data from DynamoDB with optional filtering"""
    try:
        db_handler = DynamoHandler(config)
        
        if data_type and congress:
            logger.info(f"Querying data by congress {congress} and type {data_type}")
            return db_handler.query_by_congress_and_type(congress, data_type)
        elif data_type and start_date and end_date:
            logger.info(f"Querying {data_type} data from {start_date} to {end_date}")
            return db_handler.query_by_type_and_date_range(data_type, start_date, end_date)
        elif data_type:
            logger.info(f"Scanning data by type {data_type}")
            return db_handler.scan_by_type(data_type)
        else:
            logger.warning("No specific query parameters provided, using broader scan (this may be slow)")
            # This would be a full table scan - potentially expensive and slow
            return db_handler.scan_by_type(None)
    except Exception as e:
        logger.error(f"Failed to query DynamoDB: {str(e)}")
        return []

def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD format.")
        return None

def main():
    """Main entry point for the export utility"""
    parser = argparse.ArgumentParser(description='Export congressional data from DynamoDB')
    parser.add_argument('--type', choices=['bill', 'amendment', 'committee', 'hearing', 'nomination', 'treaty'], 
                        help='Type of data to export')
    parser.add_argument('--congress', type=int, help='Congress number (e.g., 117)')
    parser.add_argument('--start-date', help='Start date for filtering (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for filtering (YYYY-MM-DD)')
    parser.add_argument('--format', choices=['json', 'csv'], default='json', 
                        help='Output format (default: json)')
    parser.add_argument('--output', help='Output file path (default: ./exports/[type]_[date].json)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config()
    if not config:
        sys.exit(1)
    
    # Process dates if provided
    start_date = None
    end_date = None
    if args.start_date:
        start_date = parse_date(args.start_date)
        if not start_date:
            sys.exit(1)
    if args.end_date:
        end_date = parse_date(args.end_date)
        if not end_date:
            sys.exit(1)
    
    # Generate default output filename if not specified
    if not args.output:
        # Create exports directory if it doesn't exist
        os.makedirs('exports', exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        type_str = args.type if args.type else 'all'
        args.output = f"exports/{type_str}_{timestamp}.{args.format}"
    
    # Query data from DynamoDB
    data = get_data_from_dynamodb(config, args.type, args.congress, start_date, end_date)
    
    if not data:
        logger.warning("No data found matching the criteria")
        sys.exit(1)
    
    # Export data to the specified format
    success = False
    if args.format == 'json':
        success = export_to_json(data, args.output)
    elif args.format == 'csv':
        success = export_to_csv(data, args.output)
    
    if success:
        print(f"Successfully exported {len(data)} records to {args.output}")
    else:
        print("Export failed")
        sys.exit(1)

if __name__ == "__main__":
    main()