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
    failed_dates = []
    total_items_processed = 0

    while current_date <= end_date:
        try:
            logger.info(f"Processing date: {current_date.strftime('%Y-%m-%d')}")

            # Get data from API
            data = api_client.get_data_for_date(current_date)

            if not data:
                logger.info(f"No data found for date {current_date.strftime('%Y-%m-%d')}")
                current_date += timedelta(days=1)
                continue

            # Store in DynamoDB
            successful_items, failed_items = db_handler.batch_store_items(data)
            total_items_processed += successful_items

            if failed_items:
                logger.warning(f"{len(failed_items)} items failed for {current_date.strftime('%Y-%m-%d')}")
                failed_dates.append({
                    'date': current_date,
                    'failed_items': failed_items
                })
            else:
                logger.info(f"Successfully processed {len(data)} items for {current_date.strftime('%Y-%m-%d')}")

        except Exception as e:
            logger.error(f"Error processing date {current_date}: {str(e)}")
            failed_dates.append({
                'date': current_date,
                'error': str(e)
            })
            # Continue to next date even if current one fails

        current_date += timedelta(days=1)

    # Report final statistics
    logger.info(f"Date range processing completed:")
    logger.info(f"Total items processed: {total_items_processed}")
    logger.info(f"Failed dates: {len(failed_dates)}")

    if failed_dates:
        logger.info("Failed dates details:")
        for failed in failed_dates:
            logger.info(f"Date: {failed['date'].strftime('%Y-%m-%d')}")
            if 'error' in failed:
                logger.info(f"Error: {failed['error']}")
            if 'failed_items' in failed:
                logger.info(f"Failed items count: {len(failed['failed_items'])}")

    return total_items_processed, failed_dates

if __name__ == "__main__":
    main()