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
logger = logging.getLogger('dynamo_test')

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
        # Initialize DynamoDB client
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table_name = 'prameya-development-dynamodb-table'

        # Check if table exists and describe its schema
        logger.info("Checking if table exists...")
        try:
            table = dynamodb.Table(table_name)
            table_desc = describe_table(dynamodb, table_name)
            logger.info(f"Table {table_name} is {table_desc['TableStatus']}")
            logger.info("Table accessed successfully")

        except ClientError as e:
            logger.error(f"Error accessing table: {str(e)}")
            return False

        # Create test timestamp
        timestamp = int(time.time())

        # Test Bill
        bill_item = {
            'id': f'test_bill_{timestamp}',
            'type': 'bill',
            'congress': 117,
            'title': 'Postal Service Reform Act of 2022',
            'update_date': '2022-09-29',
            'bill_type': 'hr',
            'bill_number': 3076,
            'version': 1,
            'origin_chamber': 'House',
            'origin_chamber_code': 'H',
            'latest_action': {
                'text': 'Became Public Law No: 117-108.',
                'action_date': '2022-04-06'
            },
            'timestamp': timestamp,
            'expiry_time': int(time.time()) + 3600  # 1 hour TTL
        }

        # Write test item
        logger.info("Testing write operations...")
        try:
            table.put_item(Item=bill_item)
            logger.info(f"Successfully wrote test bill item")
        except ClientError as e:
            logger.error(f"Failed to write bill item: {str(e)}")
            return False

        # Read test item
        logger.info("Testing read operations...")
        try:
            response = table.get_item(
                Key={'id': bill_item['id']}
            )
            retrieved_item = response.get('Item')
            if retrieved_item:
                logger.info(f"Successfully retrieved test bill item")
                logger.info(f"Retrieved item: {retrieved_item}")
            else:
                logger.error(f"Item not found: {bill_item['id']}")
                return False
        except ClientError as e:
            logger.error(f"Failed to read bill item: {str(e)}")
            return False

        # Test scan by type
        logger.info("Testing scan operations...")
        try:
            response = table.scan(
                FilterExpression='#type = :type',
                ExpressionAttributeNames={'#type': 'type'},
                ExpressionAttributeValues={':type': 'bill'}
            )
            items = response.get('Items', [])
            logger.info(f"Successfully scanned {len(items)} bill items")
        except ClientError as e:
            logger.error(f"Scan failed: {str(e)}")
            return False

        # Clean up test item
        logger.info("Cleaning up test item...")
        try:
            table.delete_item(
                Key={'id': bill_item['id']}
            )
            logger.info("Successfully deleted test bill item")
        except ClientError as e:
            logger.warning(f"Failed to delete test item {bill_item['id']}: {str(e)}")

        logger.info("All DynamoDB permission tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"Permission test failed: {str(e)}")
        return False

if __name__ == "__main__":
    if test_dynamo_permissions():
        logger.info("All DynamoDB permission tests completed successfully")
    else:
        logger.error("DynamoDB permission tests failed")