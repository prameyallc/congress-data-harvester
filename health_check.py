#!/usr/bin/env python3
import json
import sys
import boto3
import requests
from congress_api import CongressAPI
from logger_config import setup_logger
import os
import time
from botocore.exceptions import ClientError
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('health_check')

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

def check_aws_credentials():
    """Verify AWS credentials are properly configured"""
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        return {
            'status': 'healthy',
            'identity': identity['Arn']
        }
    except Exception as e:
        logger.error(f"AWS credential verification failed: {str(e)}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

def check_congress_api(config):
    """Verify Congress.gov API access"""
    try:
        # Make a simple API request to verify connectivity
        api = CongressAPI(config['api'])
        base_url = config['api']['base_url']
        api_key = os.environ.get('CONGRESS_API_KEY')

        if not api_key:
            logger.error("CONGRESS_API_KEY environment variable not found")
            return {
                'status': 'unhealthy',
                'error': 'API key not configured'
            }

        # Test with bill endpoint as it's most likely to be available
        response = requests.get(
            f"{base_url}/bill",
            params={'api_key': api_key, 'limit': 1}
        )
        response.raise_for_status()

        return {
            'status': 'healthy',
            'endpoint': f"{base_url}/bill"
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Congress API request failed: {str(e)}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error checking Congress API: {str(e)}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

def check_dynamodb(config):
    """Verify DynamoDB table access"""
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(config['dynamodb']['table_name'])
        table.get_item(
            Key={
                'id': 'health_check'
            }
        )
        return {
            'status': 'healthy',
            'table': config['dynamodb']['table_name']
        }
    except Exception as e:
        logger.error(f"DynamoDB check failed: {str(e)}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

def check_environment():
    """Verify required environment variables"""
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_DEFAULT_REGION',
        'CONGRESS_API_KEY'
    ]

    missing = [var for var in required_vars if not os.environ.get(var)]
    return {
        'status': 'healthy' if not missing else 'unhealthy',
        'missing_variables': missing
    }

def check_congress_api_endpoints(config):
    """Get and validate available Congress.gov API endpoints"""
    try:
        api = CongressAPI(config['api'])
        logger.info("Checking available Congress.gov API endpoints...")
        endpoints = api.get_available_endpoints()

        if not endpoints:
            logger.warning("No endpoints were found to be available")
            return {
                'status': 'unhealthy',
                'error': 'No endpoints available'
            }

        return {
            'status': 'healthy',
            'available_endpoints': endpoints,
            'endpoint_count': len(endpoints)
        }
    except Exception as e:
        logger.error(f"Failed to get Congress.gov API endpoints: {str(e)}")
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

def main():
    """Run all health checks including endpoint discovery"""
    config = load_config()
    if not config:
        sys.exit(1)

    logger.info("Starting health checks...")

    # Initialize health status without overall
    health_status = {
        'environment': check_environment(),
        'aws_credentials': check_aws_credentials(),
        'congress_api': check_congress_api(config),
        'congress_api_endpoints': check_congress_api_endpoints(config),
        'dynamodb': check_dynamodb(config)
    }

    # Calculate overall status separately
    all_healthy = all(
        component['status'] == 'healthy'
        for component in health_status.values()
    )

    # Add overall status
    health_status['overall'] = {
        'status': 'healthy' if all_healthy else 'unhealthy'
    }

    print(json.dumps(health_status, indent=2))
    return 0 if health_status['overall']['status'] == 'healthy' else 1

if __name__ == "__main__":
    sys.exit(main())