#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import datetime, timedelta

from congress_api import CongressAPI
from dynamo_handler import DynamoHandler
from logger_config import setup_logger
from utils import parse_date, validate_date_range

def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print("Error: Invalid JSON in config.json")
        sys.exit(1)

def main():
    config = load_config()
    logger = setup_logger(config['logging'])
    
    parser = argparse.ArgumentParser(description='Congress.gov Data Downloader')
    parser.add_argument('--mode', choices=['bulk', 'incremental', 'refresh'],
                       required=True, help='Download mode')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--lookback-days', type=int,
                       default=config['download']['default_lookback_days'],
                       help='Days to look back for incremental update')
    
    args = parser.parse_args()
    
    try:
        api_client = CongressAPI(config['api'])
        db_handler = DynamoHandler(config['dynamodb'])
        
        if args.mode == 'bulk':
            logger.info("Starting bulk download")
            process_bulk_download(api_client, db_handler, logger)
            
        elif args.mode == 'incremental':
            logger.info(f"Starting incremental download for past {args.lookback_days} days")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=args.lookback_days)
            process_date_range(api_client, db_handler, start_date, end_date, logger)
            
        elif args.mode == 'refresh':
            if not args.start_date or not args.end_date:
                logger.error("Start and end dates required for refresh mode")
                sys.exit(1)
                
            start = parse_date(args.start_date)
            end = parse_date(args.end_date)
            
            if not validate_date_range(start, end):
                logger.error("Invalid date range")
                sys.exit(1)
                
            logger.info(f"Starting refresh from {start} to {end}")
            process_date_range(api_client, db_handler, start, end, logger)
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

def process_bulk_download(api_client, db_handler, logger):
    """Process complete bulk download of available data"""
    try:
        # Get earliest available date from API
        start_date = api_client.get_earliest_date()
        end_date = datetime.now()
        
        process_date_range(api_client, db_handler, start_date, end_date, logger)
    except Exception as e:
        logger.error(f"Bulk download failed: {str(e)}", exc_info=True)
        raise

def process_date_range(api_client, db_handler, start_date, end_date, logger):
    """Process data for a specific date range"""
    current_date = start_date
    
    while current_date <= end_date:
        try:
            logger.info(f"Processing date: {current_date.strftime('%Y-%m-%d')}")
            
            # Get data from API
            data = api_client.get_data_for_date(current_date)
            
            # Store in DynamoDB
            for item in data:
                db_handler.store_item(item)
            
            logger.info(f"Successfully processed {len(data)} items for {current_date.strftime('%Y-%m-%d')}")
            
        except Exception as e:
            logger.error(f"Error processing date {current_date}: {str(e)}")
            # Continue to next date even if current one fails
            
        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()
