#!/usr/bin/env python3
import json
from dynamo_handler import DynamoHandler
from congress_api import CongressAPI
from datetime import datetime, timedelta
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('congress_test')

def analyze_committee_data(committee):
    """Analyze a committee record and return key information"""
    try:
        # Log raw committee data for debugging
        logger.debug(f"Raw committee data: {json.dumps(committee, indent=2)}")

        # Extract basic fields with fallbacks
        analysis = {
            'id': committee.get('id', 'N/A'),
            'name': committee.get('name', committee.get('committeeName', 'N/A')),
            'type': committee.get('type', committee.get('committee_type', 'N/A')),
            'chamber': committee.get('chamber', committee.get('originChamber', 'N/A')),
            'congress': committee.get('congress', 'N/A'),
            'last_updated': committee.get('update_date', committee.get('lastUpdated', 'N/A')),
            'url': committee.get('url', 'N/A'),
            'parent_committee': committee.get('parentCommittee', {}).get('name', 'N/A')
        }

        # Add optional fields if present
        if 'subcommittees' in committee:
            analysis['subcommittee_count'] = len(committee['subcommittees'])

        if 'members' in committee:
            analysis['member_count'] = len(committee['members'])

        logger.info(f"Analyzed committee data: {json.dumps(analysis, indent=2)}")
        return analysis
    except Exception as e:
        logger.error(f"Error analyzing committee data: {str(e)}")
        logger.error(f"Problematic committee data: {json.dumps(committee, indent=2)}")
        return {
            'name': 'Error analyzing committee',
            'type': 'N/A',
            'chamber': 'N/A',
            'error': str(e)
        }

def test_api_endpoints(api, start_date, end_date):
    """Test data retrieval from all API endpoints with enhanced monitoring"""
    try:
        # Get available endpoints
        endpoints = api.get_available_endpoints()
        logger.info(f"\nTesting {len(endpoints)} API endpoints from {start_date} to {end_date}")

        results = {}
        for endpoint_name, endpoint_info in endpoints.items():
            logger.info(f"\nTesting endpoint: {endpoint_name}")
            data_count = 0
            error_count = 0
            success_count = 0
            rate_limit_waits = 0
            total_wait_time = 0
            current_date = start_date

            while current_date <= end_date:
                try:
                    data = api._get_endpoint_data(
                        endpoint_name,
                        current_date.strftime('%Y-%m-%d'),
                        api.get_current_congress()
                    )
                    if data:
                        data_count += len(data)
                        success_count += len(data)
                        logger.info(f"Retrieved {len(data)} items for {endpoint_name} on {current_date.strftime('%Y-%m-%d')}")

                        # Log sample data structure for debugging
                        if len(data) > 0:
                            logger.debug(f"Sample {endpoint_name} data structure:")
                            logger.debug(json.dumps(data[0], indent=2))

                        # Track specific details for bills
                        if endpoint_name == 'bill':
                            bill_types = {}
                            for bill in data:
                                bill_type = bill.get('bill_type', 'unknown')
                                bill_types[bill_type] = bill_types.get(bill_type, 0) + 1
                            logger.info(f"Bill type distribution: {json.dumps(bill_types, indent=2)}")

                except Exception as e:
                    error_count += 1
                    logger.error(f"Error retrieving {endpoint_name} data for {current_date}: {str(e)}")

                current_date += timedelta(days=1)

            # Calculate rate limiting statistics
            avg_wait_time = total_wait_time / rate_limit_waits if rate_limit_waits > 0 else 0

            results[endpoint_name] = {
                'total_items': data_count,
                'success_count': success_count,
                'error_count': error_count,
                'rate_limit_stats': {
                    'total_waits': rate_limit_waits,
                    'total_wait_time': total_wait_time,
                    'avg_wait_time': avg_wait_time
                }
            }

            logger.info(f"Endpoint {endpoint_name} summary:")
            logger.info(f"  Total items: {data_count}")
            logger.info(f"  Successful retrievals: {success_count}")
            logger.info(f"  Errors: {error_count}")
            logger.info(f"  Rate limiting:")
            logger.info(f"    Total wait events: {rate_limit_waits}")
            logger.info(f"    Total wait time: {total_wait_time:.2f}s")
            logger.info(f"    Average wait time: {avg_wait_time:.2f}s")

        return results

    except Exception as e:
        logger.error(f"Error testing API endpoints: {str(e)}")
        return {}

def analyze_stored_data(dynamo_handler, start_date, end_date):
    """Analyze data stored in DynamoDB with enhanced metrics"""
    try:
        # Get counts by type
        type_counts = {}
        validation_errors = {}
        storage_errors = {}

        # Query each data type
        data_types = [
            'bill', 'amendment', 'nomination', 'treaty',
            'committee', 'hearing', 'committee-report',
            'congressional-record', 'house-communication',
            'house-requirement', 'senate-communication',
            'member', 'summary'
        ]

        for data_type in data_types:
            try:
                items = dynamo_handler.query_by_type_and_date_range(
                    data_type,
                    start_date.strftime('%Y-%m-%d'),
                    end_date.strftime('%Y-%m-%d')
                )
                type_counts[data_type] = len(items)

                if items:
                    logger.info(f"\nData type {data_type} analysis:")
                    logger.info(f"  Total items: {len(items)}")

                    # Analyze sample item
                    logger.info(f"Sample {data_type} data structure:")
                    logger.info(json.dumps(items[0], indent=2))

                    # Track specific metrics for bills
                    if data_type == 'bill':
                        bill_chambers = {}
                        bill_types = {}
                        for bill in items:
                            chamber = bill.get('origin_chamber', 'unknown')
                            bill_type = bill.get('bill_type', 'unknown')
                            bill_chambers[chamber] = bill_chambers.get(chamber, 0) + 1
                            bill_types[bill_type] = bill_types.get(bill_type, 0) + 1

                        logger.info("Bill statistics:")
                        logger.info(f"  Chamber distribution: {json.dumps(bill_chambers, indent=2)}")
                        logger.info(f"  Type distribution: {json.dumps(bill_types, indent=2)}")

            except Exception as e:
                storage_errors[data_type] = str(e)
                logger.error(f"Error analyzing {data_type} data: {str(e)}")

        analysis_results = {
            'type_counts': type_counts,
            'validation_errors': validation_errors,
            'storage_errors': storage_errors
        }

        return analysis_results

    except Exception as e:
        logger.error(f"Error analyzing stored data: {str(e)}")
        return {}

def main():
    # Load config
    with open('config.json', 'r') as f:
        config = json.load(f)

    # Initialize API and DynamoDB clients
    logger.info("Initializing API and DynamoDB connections...")
    api = CongressAPI(config['api'])
    dynamo = DynamoHandler(config['dynamodb'])

    try:
        # Set date range for 60 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=60)

        logger.info(f"\nTesting data flow for date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        # Test API endpoints
        api_results = test_api_endpoints(api, start_date, end_date)

        logger.info("\nAPI Endpoint Results:")
        for endpoint, stats in api_results.items():
            logger.info(f"  {endpoint}:")
            logger.info(f"    Total items: {stats['total_items']}")
            logger.info(f"    Successful: {stats['success_count']}")
            logger.info(f"    Errors: {stats['error_count']}")
            logger.info(f"    Rate limiting:")
            logger.info(f"      Total waits: {stats['rate_limit_stats']['total_waits']}")
            logger.info(f"      Total wait time: {stats['rate_limit_stats']['total_wait_time']:.2f}s")
            logger.info(f"      Average wait time: {stats['rate_limit_stats']['avg_wait_time']:.2f}s")

        # Analyze stored data
        logger.info("\nAnalyzing stored data in DynamoDB...")
        stored_data = analyze_stored_data(dynamo, start_date, end_date)

        logger.info("\nStored Data Analysis:")
        for data_type, count in stored_data['type_counts'].items():
            logger.info(f"  {data_type}: {count} items")

        # Compare API results with stored data
        logger.info("\nData Flow Analysis:")
        for endpoint, stats in api_results.items():
            stored_type = endpoint.replace('-', '')  # Match stored data type format
            api_count = stats['total_items']
            stored_count = stored_data['type_counts'].get(stored_type, 0)

            if api_count > 0 and stored_count == 0:
                logger.warning(f"  {endpoint}: Data retrieved but not stored (API: {api_count}, Stored: {stored_count})")
            elif stored_count < api_count:
                logger.warning(f"  {endpoint}: Potential data loss (API: {api_count}, Stored: {stored_count})")
            else:
                logger.info(f"  {endpoint}: Data flow verified (API: {api_count}, Stored: {stored_count})")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()