#!/usr/bin/env python3
import argparse
import json
import sys
import os
from datetime import datetime, timedelta
import time
import boto3
from congress_api import CongressAPI
from dynamo_handler import DynamoHandler
from logger_config import setup_logger
from utils import parse_date
from monitoring import metrics
import threading
import signal
import concurrent.futures
from queue import Queue
from typing import List, Dict, Any, Tuple
from export_data import export_to_json, export_to_csv, get_data_from_dynamodb

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

    # Generate and log final metrics reports
    try:
        api_report = metrics.generate_api_metrics_report()
        logger.info("\n" + api_report)

        ingestion_report = metrics.generate_ingestion_report()
        logger.info("\n" + ingestion_report)
    except Exception as e:
        logger.error(f"Failed to generate metrics reports: {str(e)}")

    # Flush metrics
    metrics.flush_metrics()
    sys.exit(0)

def process_date_chunk(api_client: CongressAPI, db_handler: DynamoHandler, 
                       dates: List[datetime], logger) -> Tuple[int, List[Dict]]:
    """Process a chunk of dates in parallel"""
    total_items = 0
    chunk_failed_dates = []

    for date in dates:
        try:
            # Reset processed IDs tracking for each date to prevent duplicates
            # while still maintaining clean state for each date
            db_handler.reset_processed_ids()

            date_str = date.strftime('%Y-%m-%d')
            logger.info(f"Processing date: {date_str}")

            # Get raw data and log it for debugging
            data = api_client.get_data_for_date(date)
            if not data:
                logger.info(f"No data found for date {date_str}")
                continue

            # Log data statistics before storage
            type_counts = {}
            for item in data:
                item_type = item.get('type', 'unknown')
                type_counts[item_type] = type_counts.get(item_type, 0) + 1

            logger.info(f"Retrieved data for {date_str}:")
            for item_type, count in type_counts.items():
                logger.info(f"  - {item_type}: {count} items")
                # Track endpoint-specific metrics for reporting
                metrics.track_items_processed(item_type, count)

            # If we have committee data, log a sample for debugging
            committee_items = [item for item in data if item.get('type') == 'committee']
            if committee_items:
                logger.info(f"Sample committee data structure:")
                logger.info(f"{json.dumps(committee_items[0], indent=2)}")

            successful_items, failed_items = db_handler.batch_store_items(data)
            total_items += successful_items

            if failed_items:
                logger.warning(f"{len(failed_items)} items failed for {date_str}")
                logger.warning("Failed items by type:")
                failed_by_type = {}
                for item in failed_items:
                    item_type = item['item'].get('type', 'unknown')
                    failed_by_type[item_type] = failed_by_type.get(item_type, 0) + 1
                    if item_type == 'committee':
                        logger.warning(f"Failed committee item: {json.dumps(item['item'], indent=2)}")
                        logger.warning(f"Error: {item['error']}")

                for item_type, count in failed_by_type.items():
                    logger.warning(f"  - {item_type}: {count} failed items")
                    # Track failed items by type
                    metrics.track_items_processed(item_type, 0, 0, count)

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
    # Reset processed IDs tracking at the beginning of each date range
    # This ensures we start with a clean slate for each range
    db_handler.reset_processed_ids()

    # Reset metrics tracking for a new session
    metrics.reset_stats()

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

    # Generate and log metrics reports at the end of processing
    api_report = metrics.generate_api_metrics_report()
    logger.info("\n" + api_report)

    ingestion_report = metrics.generate_ingestion_report()
    logger.info("\n" + ingestion_report)

    # Get API client statistics
    api_stats = api_client.get_api_stats()
    logger.info(f"API Client Statistics:")
    logger.info(f"  Total requests: {api_stats['request_count']}")
    logger.info(f"  Error rate: {api_stats['error_rate']:.2f}%")
    logger.info(f"  Uptime: {api_stats['uptime_formatted']}")
    logger.info(f"  Requests per second: {api_stats['requests_per_second']:.2f}")

    return total_items_processed, all_failed_dates

def process_bulk_download(api_client, db_handler, logger):
    """Process complete bulk download of available data"""
    try:
        # Reset processed IDs tracking at the beginning of bulk download
        db_handler.reset_processed_ids()

        # Get earliest available date from API
        start_date = api_client.get_earliest_date()
        end_date = datetime.now()

        process_date_range(api_client, db_handler, start_date, end_date, logger)
    except Exception as e:
        logger.error(f"Bulk download failed: {str(e)}", exc_info=True)
        raise

def validate_date_range(start_date: datetime, end_date: datetime, config: Dict) -> Tuple[bool, str]:
    """Validate the date range against configuration limits.

    Args:
        start_date: Start date for data download
        end_date: End date for data download
        config: Application configuration dictionary

    Returns:
        Tuple of (is_valid: bool, error_message: str)
    """
    try:
        # Get date range limits from config
        date_config = config['download']['date_ranges']
        min_date = datetime.strptime(date_config['min_date'], '%Y-%m-%d')
        max_range_days = date_config['max_range_days']

        # Validate date range
        if start_date > end_date:
            return False, "Start date must be before end date"

        # Check against minimum allowed date
        if start_date < min_date:
            return False, f"Start date cannot be before {min_date.strftime('%Y-%m-%d')}"

        # Check range size
        date_range = (end_date - start_date).days
        if date_range > max_range_days:
            return False, f"Date range ({date_range} days) exceeds maximum allowed ({max_range_days} days)"

        # Ensure not in future
        if end_date > datetime.now():
            return False, "End date cannot be in the future"

        return True, ""

    except KeyError as e:
        return False, f"Missing configuration key: {str(e)}"
    except ValueError as e:
        return False, f"Date validation error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error in date validation: {str(e)}"

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
    parser.add_argument('--mode', choices=['bulk', 'incremental', 'refresh', 'export'],
                       required=True, help='Download mode or export data')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--lookback-days', type=int,
                       default=config['download']['default_lookback_days'],
                       help='Days to look back for incremental update')
    parser.add_argument('--parallel-workers', type=int, default=3,
                       help='Number of parallel workers for processing')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    # Export-specific arguments
    parser.add_argument('--format', choices=['json', 'csv'], default='json',
                       help='Export format (for export mode)')
    parser.add_argument('--data-type', choices=['bill', 'committee', 'hearing', 'amendment', 'nomination', 'treaty'],
                       help='Type of data to export (for export mode)')
    parser.add_argument('--congress', type=int, help='Congress number (for export mode, e.g., 117)')
    parser.add_argument('--output', help='Output file path (for export mode)')

    args = parser.parse_args()

    # Set log level based on verbose flag
    if args.verbose:
        logger.setLevel('DEBUG')
        logger.info("Verbose logging enabled")

    try:
        logger.info("Initializing Congress API client...")
        api_client = CongressAPI(config['api'])

        logger.info("Initializing DynamoDB handler...")
        db_handler = DynamoHandler(config['dynamodb'])

        # Reset processed IDs tracking at the start of a new session
        db_handler.reset_processed_ids()

        if args.mode == 'export':
            logger.info("Starting data export")
            
            # Process dates if provided
            start_date = None
            end_date = None
            if args.start_date:
                start_date = parse_date(args.start_date)
                if not start_date:
                    logger.error("Invalid start date format. Use YYYY-MM-DD.")
                    sys.exit(1)
            if args.end_date:
                end_date = parse_date(args.end_date)
                if not end_date:
                    logger.error("Invalid end date format. Use YYYY-MM-DD.")
                    sys.exit(1)
            
            # Convert congress to int if provided
            congress = None
            if args.data_type:
                # Handle congress argument if provided
                try:
                    if hasattr(args, 'congress') and args.congress is not None:
                        congress = int(args.congress)
                except ValueError:
                    logger.error("Congress must be an integer value")
                    sys.exit(1)
                    
            # Generate default output filename if not specified
            output_file = args.output
            if not output_file:
                # Create exports directory if it doesn't exist
                os.makedirs('exports', exist_ok=True)
                
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                type_str = args.data_type if args.data_type else 'all'
                output_file = f"exports/{type_str}_{timestamp}.{args.format}"
                
            # Query data from DynamoDB
            logger.info(f"Querying data from DynamoDB: type={args.data_type}, congress={congress}, dates={start_date}-{end_date}")
            data = get_data_from_dynamodb(config['dynamodb'], args.data_type, congress, start_date, end_date)
            
            if not data:
                logger.warning("No data found matching the criteria")
                sys.exit(1)
            
            # Export data to the specified format
            success = False
            if args.format == 'json':
                logger.info(f"Exporting {len(data)} records to JSON: {output_file}")
                success = export_to_json(data, output_file)
            elif args.format == 'csv':
                logger.info(f"Exporting {len(data)} records to CSV: {output_file}")
                success = export_to_csv(data, output_file)
            
            if success:
                logger.info(f"Successfully exported {len(data)} records to {output_file}")
            else:
                logger.error("Export failed")
                sys.exit(1)
                
        elif args.mode == 'bulk':
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
                # Use defaults from config if not specified
                date_config = config['download']['date_ranges']
                start = parse_date(args.start_date or date_config['default_start_date'])
                end = parse_date(args.end_date or date_config['default_end_date'])
            else:
                start = parse_date(args.start_date)
                end = parse_date(args.end_date)

            # Validate date range
            is_valid, error_msg = validate_date_range(start, end, config)
            if not is_valid:
                logger.error(f"Invalid date range: {error_msg}")
                sys.exit(1)

            logger.info(f"Starting refresh from {start} to {end}")
            process_date_range(api_client, db_handler, start, end, 
                             logger, args.parallel_workers)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)

        # Generate final metrics reports even on error
        try:
            api_report = metrics.generate_api_metrics_report()
            logger.info("\n" + api_report)

            ingestion_report = metrics.generate_ingestion_report()
            logger.info("\n" + ingestion_report)
        except Exception as report_err:
            logger.error(f"Failed to generate metrics reports: {str(report_err)}")

        metrics.flush_metrics()
        sys.exit(1)

    # Generate final metrics reports on success
    try:
        api_report = metrics.generate_api_metrics_report()
        logger.info("\n" + api_report)

        ingestion_report = metrics.generate_ingestion_report()
        logger.info("\n" + ingestion_report)
    except Exception as e:
        logger.error(f"Failed to generate metrics reports: {str(e)}")

    # Ensure final metrics are sent
    metrics.flush_metrics()

if __name__ == "__main__":
    main()