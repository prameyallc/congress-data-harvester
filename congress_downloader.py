#!/usr/bin/env python3
import argparse
import json
import sys
from datetime import datetime, timedelta
import time
import boto3
from congress_api import CongressAPI
from dynamo_handler import DynamoHandler
from logger_config import setup_logger
from utils import parse_date, validate_date_range
from monitoring import metrics
import threading
import signal
import concurrent.futures
from queue import Queue
from typing import List, Dict, Any, Tuple

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

def verify_aws_credentials(logger):
    """Verify AWS credentials are properly configured"""
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        logger.info(f"AWS credentials verified. Using IAM identity: {identity['Arn']}")
        return True
    except Exception as e:
        logger.error(f"AWS credential verification failed: {str(e)}")
        return False

def monitor_resources():
    """Background thread to monitor system resources"""
    while True:
        try:
            metrics.track_resource_usage()
            time.sleep(60)  # Collect metrics every minute
        except Exception as e:
            logger.error(f"Error collecting resource metrics: {str(e)}")

def start_monitoring():
    """Start the resource monitoring thread"""
    monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
    monitor_thread.start()
    return monitor_thread

def cleanup(signum, frame):
    """Cleanup function for graceful shutdown"""
    logger.info("Shutting down Congress Downloader...")
    metrics.flush_metrics()
    sys.exit(0)

def process_date_chunk(api_client: CongressAPI, db_handler: DynamoHandler, 
                      dates: List[datetime], logger) -> Tuple[int, List[Dict]]:
    """Process a chunk of dates in parallel"""
    total_items = 0
    chunk_failed_dates = []

    for date in dates:
        try:
            date_str = date.strftime('%Y-%m-%d')
            logger.info(f"Processing date: {date_str}")

            data = api_client.get_data_for_date(date)
            if not data:
                logger.info(f"No data found for date {date_str}")
                continue

            successful_items, failed_items = db_handler.batch_store_items(data)
            total_items += successful_items

            if failed_items:
                logger.warning(f"{len(failed_items)} items failed for {date_str}")
                chunk_failed_dates.append({
                    'date': date,
                    'failed_items': failed_items
                })
            else:
                logger.info(f"Successfully processed {len(data)} items for {date_str}")

        except Exception as e:
            logger.error(f"Error processing date {date_str}: {str(e)}")
            chunk_failed_dates.append({
                'date': date,
                'error': str(e)
            })

    return total_items, chunk_failed_dates

def process_date_range(api_client: CongressAPI, db_handler: DynamoHandler, 
                      start_date: datetime, end_date: datetime, logger,
                      max_workers: int = 3) -> Tuple[int, List[Dict]]:
    """Process data for a specific date range using parallel processing"""
    current_date = start_date
    dates = []
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)

    # Split dates into chunks for parallel processing
    chunk_size = max(1, len(dates) // max_workers)
    date_chunks = [dates[i:i + chunk_size] for i in range(0, len(dates), chunk_size)]

    total_items_processed = 0
    all_failed_dates = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunks for processing
        future_to_chunk = {
            executor.submit(process_date_chunk, api_client, db_handler, chunk, logger): chunk 
            for chunk in date_chunks
        }

        # Process completed futures as they finish
        for future in concurrent.futures.as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            try:
                chunk_items, chunk_failures = future.result()
                total_items_processed += chunk_items
                all_failed_dates.extend(chunk_failures)
            except Exception as e:
                logger.error(f"Error processing chunk: {str(e)}")
                all_failed_dates.extend([{
                    'date': date,
                    'error': str(e)
                } for date in chunk])

    # Report final statistics
    logger.info("Date range processing completed:")
    logger.info(f"Total items processed: {total_items_processed}")
    logger.info(f"Failed dates: {len(all_failed_dates)}")

    if all_failed_dates:
        logger.info("Failed dates details:")
        for failed in all_failed_dates:
            logger.info(f"Date: {failed['date'].strftime('%Y-%m-%d')}")
            if 'error' in failed:
                logger.info(f"Error: {failed['error']}")
            if 'failed_items' in failed:
                logger.info(f"Failed items count: {len(failed['failed_items'])}")

    return total_items_processed, all_failed_dates

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

def main():
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    config = load_config()
    logger = setup_logger(config['logging'])

    # Start resource monitoring
    monitor_thread = start_monitoring()

    # Verify AWS credentials before proceeding
    if not verify_aws_credentials(logger):
        logger.error("Unable to proceed without valid AWS credentials")
        sys.exit(1)

    parser = argparse.ArgumentParser(description='Congress.gov Data Downloader')
    parser.add_argument('--mode', choices=['bulk', 'incremental', 'refresh'],
                       required=True, help='Download mode')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--lookback-days', type=int,
                       default=config['download']['default_lookback_days'],
                       help='Days to look back for incremental update')
    parser.add_argument('--parallel-workers', type=int, default=3,
                       help='Number of parallel workers for processing')

    args = parser.parse_args()

    try:
        logger.info("Initializing Congress API client...")
        api_client = CongressAPI(config['api'])

        logger.info("Initializing DynamoDB handler...")
        db_handler = DynamoHandler(config['dynamodb'])

        if args.mode == 'bulk':
            logger.info("Starting bulk download")
            start_date = api_client.get_earliest_date()
            end_date = datetime.now()
            process_date_range(api_client, db_handler, start_date, end_date, 
                             logger, args.parallel_workers)

        elif args.mode == 'incremental':
            logger.info(f"Starting incremental download for past {args.lookback_days} days")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=args.lookback_days)
            process_date_range(api_client, db_handler, start_date, end_date, 
                             logger, args.parallel_workers)

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
            process_date_range(api_client, db_handler, start, end, 
                             logger, args.parallel_workers)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        metrics.flush_metrics()  # Ensure metrics are sent before exit
        sys.exit(1)

    # Ensure final metrics are sent
    metrics.flush_metrics()

if __name__ == "__main__":
    main()