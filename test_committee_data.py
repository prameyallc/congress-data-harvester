#!/usr/bin/env python3
import json
from datetime import datetime, timedelta
import logging
import os
from congress_api import CongressAPI
from dynamo_handler import DynamoHandler

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('congress_test')

def get_test_config():
    """Load base config and add test-specific settings"""
    with open('config.json', 'r') as f:
        config = json.load(f)

    # Add test-specific settings
    config['api']['test_mode'] = True
    config['api']['rate_limit'] = {
        'requests_per_second': 10,  # Higher limit for tests
        'max_retries': 2,
        'retry_delay': 0.5
    }

    return config

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

                except Exception as e:
                    error_count += 1
                    logger.error(f"Error retrieving {endpoint_name} data for {current_date}: {str(e)}")

                current_date += timedelta(days=1)

            results[endpoint_name] = {
                'total_items': data_count,
                'success_count': success_count,
                'error_count': error_count
            }

            logger.info(f"Endpoint {endpoint_name} summary:")
            logger.info(f"  Total items: {data_count}")
            logger.info(f"  Successful retrievals: {success_count}")
            logger.info(f"  Errors: {error_count}")

        return results

    except Exception as e:
        logger.error(f"Error testing API endpoints: {str(e)}")
        return {}

def analyze_committee_data(committee):
    """Analyze a committee record and return key information"""
    try:
        # Extract basic fields with fallbacks
        analysis = {
            'id': committee.get('id', 'N/A'),
            'name': committee.get('name', committee.get('committeeName', 'N/A')),
            'type': committee.get('type', committee.get('committee_type', 'N/A')),
            'chamber': committee.get('chamber', committee.get('originChamber', 'N/A')),
            'congress': committee.get('congress', 'N/A'),
            'last_updated': committee.get('update_date', committee.get('lastUpdated', 'N/A')),
            'url': committee.get('url', 'N/A')
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
        return {
            'name': 'Error analyzing committee',
            'type': 'N/A',
            'chamber': 'N/A',
            'error': str(e)
        }

def main():
    try:
        # Load test config
        config = get_test_config()

        # Initialize API and DynamoDB clients
        logger.info("Initializing API and DynamoDB connections...")
        api = CongressAPI(config['api'])
        dynamo = DynamoHandler(config['dynamodb'])

        # Set date range for testing (7 days instead of 60)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        logger.info(f"\nTesting data flow for date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        # Test API endpoints
        api_results = test_api_endpoints(api, start_date, end_date)

        logger.info("\nAPI Endpoint Results:")
        for endpoint, stats in api_results.items():
            logger.info(f"  {endpoint}:")
            logger.info(f"    Total items: {stats['total_items']}")
            logger.info(f"    Successful: {stats['success_count']}")
            logger.info(f"    Errors: {stats['error_count']}")

        return 0

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())