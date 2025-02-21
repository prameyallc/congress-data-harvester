#!/usr/bin/env python3
import json
import sys
import boto3
import requests
from congress_api import CongressAPI
from logger_config import setup_logger
import os

def load_config():
    """Load application configuration"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON in config.json")
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

        response = requests.get(
            f"{base_url}/bill",
            params={'api_key': api_key, 'limit': 1}
        )
        response.raise_for_status()

        return {
            'status': 'healthy',
            'endpoint': f"{base_url}/bill"
        }
    except Exception as e:
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

def main():
    """Run all health checks"""
    config = load_config()
    if not config:
        sys.exit(1)

    health_status = {
        'environment': check_environment(),
        'aws_credentials': check_aws_credentials(),
        'congress_api': check_congress_api(config),
        'dynamodb': check_dynamodb(config)
    }

    # Overall status is healthy only if all components are healthy
    health_status['overall'] = {
        'status': 'healthy' if all(
            component['status'] == 'healthy' 
            for component in health_status.values()
        ) else 'unhealthy'
    }

    print(json.dumps(health_status, indent=2))
    return 0 if health_status['overall']['status'] == 'healthy' else 1

if __name__ == "__main__":
    sys.exit(main())