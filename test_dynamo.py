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
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

# Setup logging configuration
log_config = {
    'level': logging.DEBUG,
    'file': 'logs/dynamo_test.log',
    'max_size': 10485760,
    'backup_count': 5
}
logger = setup_logger(log_config)

def test_aws_credentials():
    """Test AWS credentials and print identity"""
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        logger.info(f"AWS credentials verified. Using IAM identity: {identity['Arn']}")
        return True
    except Exception as e:
        logger.error(f"AWS credentials verification failed: {str(e)}")
        return False

def describe_table(dynamodb, table_name):
    """Get and log table schema details"""
    try:
        response = dynamodb.meta.client.describe_table(TableName=table_name)
        table_desc = response['Table']

        logger.info("=== Table Schema ===")
        logger.info(f"Primary Key Schema: {table_desc['KeySchema']}")

        if 'GlobalSecondaryIndexes' in table_desc:
            logger.info("=== Global Secondary Indexes ===")
            for gsi in table_desc['GlobalSecondaryIndexes']:
                logger.info(f"Index: {gsi['IndexName']}")
                logger.info(f"Key Schema: {gsi['KeySchema']}")

        logger.info("=== Attribute Definitions ===")
        for attr in table_desc['AttributeDefinitions']:
            logger.info(f"Attribute: {attr['AttributeName']}, Type: {attr['AttributeType']}")

        return table_desc
    except Exception as e:
        logger.error(f"Error describing table: {str(e)}")
        raise

def test_dynamo_permissions():
    """Test specific DynamoDB permissions"""
    try:
        # First verify AWS credentials
        if not test_aws_credentials():
            logger.error("AWS credentials verification failed")
            return False

        # Initialize DynamoDB client with debug logging
        logger.info("Initializing DynamoDB client...")
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table_name = 'prameya-development-dynamodb-table'

        # Check if table exists and describe its schema
        logger.info(f"Checking if table {table_name} exists...")
        try:
            table = dynamodb.Table(table_name)
            table_desc = describe_table(dynamodb, table_name)
            logger.info(f"Table {table_name} is {table_desc['TableStatus']}")
            logger.info("Table accessed successfully")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            logger.error(f"Error accessing table: Code={error_code}, Message={error_msg}")
            return False

        # Create test timestamp
        timestamp = int(time.time())

        # Test Bill
        bill_item = {
            'id': f'test_bill_{timestamp}',
            'type': 'bill',
            'congress': 117,
            'title': 'Test Bill for DynamoDB Permissions',
            'update_date': '2024-02-22',
            'bill_type': 'hr',
            'bill_number': 3076,
            'version': 1,
            'origin_chamber': 'House',
            'origin_chamber_code': 'H',
            'latest_action': {
                'text': 'Test action',
                'action_date': '2024-02-22'
            },
            'timestamp': timestamp,
            'expiry_time': int(time.time()) + 3600  # 1 hour TTL
        }

        # Write test item
        logger.info("Testing write operations...")
        try:
            logger.debug(f"Attempting to write item: {json.dumps(bill_item, indent=2)}")
            table.put_item(Item=bill_item)
            logger.info(f"Successfully wrote test bill item with ID: {bill_item['id']}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            logger.error(f"Failed to write bill item: Code={error_code}, Message={error_msg}")
            return False

        # Read test item
        logger.info("Testing read operations...")
        try:
            response = table.get_item(
                Key={'id': bill_item['id']}
            )
            retrieved_item = response.get('Item')
            if retrieved_item:
                logger.info(f"Successfully retrieved test bill item: {json.dumps(retrieved_item, indent=2, cls=DecimalEncoder)}")
            else:
                logger.error(f"Item not found: {bill_item['id']}")
                return False
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            logger.error(f"Failed to read bill item: Code={error_code}, Message={error_msg}")
            return False

        # Clean up test item
        logger.info("Cleaning up test item...")
        try:
            table.delete_item(
                Key={'id': bill_item['id']}
            )
            logger.info("Successfully deleted test bill item")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            logger.warning(f"Failed to delete test item {bill_item['id']}: Code={error_code}, Message={error_msg}")

        logger.info("All DynamoDB permission tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"Permission test failed: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting DynamoDB permission tests...")
    if test_dynamo_permissions():
        logger.info("All DynamoDB permission tests completed successfully")
        sys.exit(0)
    else:
        logger.error("DynamoDB permission tests failed")
        sys.exit(1)