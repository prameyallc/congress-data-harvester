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
from dynamo_handler import DynamoHandler

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

def test_deduplication_mechanism():
    """Test the deduplication mechanism in DynamoHandler"""
    try:
        logger.info("Starting deduplication mechanism tests...")
        # Initialize DynamoDB handler
        dynamodb_config = {
            'table_name': 'prameya-development-dynamodb-table',
            'region': 'us-west-2'
        }

        handler = DynamoHandler(dynamodb_config)

        # Test reset_processed_ids
        logger.info("Testing reset_processed_ids method...")
        handler.processed_item_ids.add("test_id_1")
        handler.processed_item_ids.add("test_id_2")

        # Verify items are in the set
        assert "test_id_1" in handler.processed_item_ids
        assert "test_id_2" in handler.processed_item_ids
        logger.info("Successfully added test IDs to processed_item_ids set")

        # Reset the set
        handler.reset_processed_ids()

        # Verify the set is empty
        assert len(handler.processed_item_ids) == 0
        logger.info("Successfully reset processed_item_ids set")

        # Create test timestamp
        timestamp = int(time.time())

        # Create test items with duplicate IDs
        test_items = []
        for i in range(5):
            # Add original items
            test_items.append({
                'id': f'test_dedup_{i}_{timestamp}',
                'type': 'test',
                'data': f'Original item {i}',
                'update_date': '2024-02-22'
            })

            # Add duplicate items with same ID but different data
            if i % 2 == 0:  # Add duplicates for even-numbered items
                test_items.append({
                    'id': f'test_dedup_{i}_{timestamp}',
                    'type': 'test',
                    'data': f'Duplicate item {i}',
                    'update_date': '2024-02-22'
                })

        logger.info(f"Created {len(test_items)} test items with duplicates")

        # Test batch_store_items with deduplication
        successful_items, failed_items = handler.batch_store_items(test_items)

        # Should have stored 5 unique items and skipped the duplicates
        logger.info(f"Stored {successful_items} items, expected 5")
        logger.info(f"Failed items: {len(failed_items)}, expected 0")

        # Verify that all original item IDs are in the processed set
        for i in range(5):
            item_id = f'test_dedup_{i}_{timestamp}'
            assert item_id in handler.processed_item_ids
            logger.info(f"Verified item {item_id} is in processed set")

        # Now try to store the same items again - all should be duplicates
        successful_items_2, failed_items_2 = handler.batch_store_items(test_items)

        # Should have skipped all items as duplicates
        logger.info(f"Second batch: stored {successful_items_2} items, expected 0")
        logger.info(f"Second batch failed items: {len(failed_items_2)}, expected 0")

        # Test reset_processed_ids again
        handler.reset_processed_ids()

        # Now try again after reset - should write items again
        successful_items_3, failed_items_3 = handler.batch_store_items(test_items[:5])  # Using only unique items

        # Should have stored 5 unique items again
        logger.info(f"After reset: stored {successful_items_3} items, expected 5")
        logger.info(f"After reset failed items: {len(failed_items_3)}, expected 0")

        # Cleanup test items
        logger.info("Cleaning up test items...")
        for i in range(5):
            try:
                handler.table.delete_item(
                    Key={'id': f'test_dedup_{i}_{timestamp}'}
                )
                logger.info(f"Deleted test item test_dedup_{i}_{timestamp}")
            except Exception as e:
                logger.warning(f"Failed to delete test item: {str(e)}")

        logger.info("Deduplication mechanism tests completed successfully")
        return True

    except Exception as e:
        logger.error(f"Deduplication test failed: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting DynamoDB permission tests...")
    tests_passed = True

    if not test_aws_credentials():
        logger.error("AWS credentials test failed")
        tests_passed = False

    if not test_dynamo_permissions():
        logger.error("DynamoDB permission tests failed")
        tests_passed = False

    if not test_deduplication_mechanism():
        logger.error("Deduplication mechanism tests failed")
        tests_passed = False

    if tests_passed:
        logger.info("All tests completed successfully")
        sys.exit(0)
    else:
        logger.error("Some tests failed")
        sys.exit(1)