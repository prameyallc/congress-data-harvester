import time
import boto3
from botocore.exceptions import ClientError
import logging

class DynamoHandler:
    def __init__(self, config):
        self.table_name = config['table_name']
        self.dynamodb = boto3.resource('dynamodb', region_name=config['region'])
        self.table = None
        self.logger = logging.getLogger('congress_downloader')
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Ensure DynamoDB table exists and is ready"""
        try:
            # Check if table exists
            existing_table = None
            try:
                existing_table = self.dynamodb.Table(self.table_name)
                existing_table.load()
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    self.logger.info(f"Table {self.table_name} does not exist, creating...")
                else:
                    raise

            if existing_table is None:
                # Create table with GSI for timestamp-based queries
                table = self.dynamodb.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {'AttributeName': 'id', 'KeyType': 'HASH'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'id', 'AttributeType': 'S'},
                        {'AttributeName': 'timestamp', 'AttributeType': 'N'}
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'timestamp-index',
                            'KeySchema': [
                                {'AttributeName': 'timestamp', 'KeyType': 'HASH'}
                            ],
                            'Projection': {'ProjectionType': 'ALL'},
                            'ProvisionedThroughput': {
                                'ReadCapacityUnits': 5,
                                'WriteCapacityUnits': 5
                            }
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                )
                self.logger.info(f"Waiting for table {self.table_name} to be created...")
                table.wait_until_exists()
                self.table = table
            else:
                self.table = existing_table

            self.logger.info(f"DynamoDB table {self.table_name} is ready")

        except ClientError as e:
            self.logger.error(f"Failed to ensure table exists: {str(e)}")
            raise Exception(f"DynamoDB table setup failed: {str(e)}")

    def store_item(self, item):
        """Store a single item in DynamoDB with improved error handling"""
        try:
            # Ensure item has required attributes
            if 'id' not in item:
                raise ValueError("Item must have an 'id' attribute")

            # Add timestamp for tracking
            item['timestamp'] = int(time.time())

            # Additional validation for bill-specific fields
            required_fields = ['congress', 'bill_type', 'bill_number']
            missing_fields = [field for field in required_fields if not item.get(field)]
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")

            self.table.put_item(
                Item=item,
                # Implement conditional write to avoid duplicates or ensure newer versions
                ConditionExpression='attribute_not_exists(id) OR (attribute_exists(update_date) AND update_date < :new_update_date)',
                ExpressionAttributeValues={
                    ':new_update_date': item.get('update_date', '0')
                }
            )
            self.logger.debug(f"Successfully stored item with ID: {item['id']}")

        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # Item exists with newer or same version - skip
                self.logger.info(f"Skipping item {item['id']} as a newer version exists")
                return
            self.logger.error(f"DynamoDB operation failed for item {item['id']}: {str(e)}")
            raise Exception(f"DynamoDB operation failed: {str(e)}")
        except ValueError as e:
            self.logger.error(f"Validation error for item {item.get('id', 'unknown')}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error storing item {item.get('id', 'unknown')}: {str(e)}")
            raise

    def batch_store_items(self, items):
        """Store multiple items in DynamoDB using batch write with improved error handling"""
        try:
            successful_items = 0
            failed_items = []

            # Process items in batches of 25 (DynamoDB batch write limit)
            batch_size = 25
            for i in range(0, len(items), batch_size):
                batch_items = items[i:i + batch_size]
                self.logger.info(f"Processing batch of {len(batch_items)} items (total processed: {successful_items})")

                try:
                    with self.table.batch_writer() as batch:
                        for item in batch_items:
                            try:
                                if 'id' not in item:
                                    self.logger.warning(f"Skipping item without ID: {item}")
                                    continue

                                # Add timestamp for tracking
                                item['timestamp'] = int(time.time())

                                # Validate required fields
                                required_fields = ['congress', 'bill_type', 'bill_number']
                                if not all(item.get(field) for field in required_fields):
                                    self.logger.warning(f"Skipping item {item.get('id')} due to missing required fields")
                                    continue

                                batch.put_item(Item=item)
                                successful_items += 1

                            except Exception as e:
                                self.logger.error(f"Failed to write item {item.get('id', 'unknown')}: {str(e)}")
                                failed_items.append({
                                    'id': item.get('id', 'unknown'),
                                    'error': str(e),
                                    'item': item
                                })

                    self.logger.debug(f"Completed batch: {len(batch_items)} items processed")

                except ClientError as e:
                    self.logger.error(f"Batch write failed: {str(e)}")
                    # If batch write fails, add all items to failed_items
                    failed_items.extend([{
                        'id': item.get('id', 'unknown'),
                        'error': str(e),
                        'item': item
                    } for item in batch_items])

            self.logger.info(f"Batch write completed: {successful_items} items successful, {len(failed_items)} failed")
            if failed_items:
                self.logger.debug(f"Failed items details: {failed_items}")

            return successful_items, failed_items

        except Exception as e:
            self.logger.error(f"Unexpected error in batch_store_items: {str(e)}")
            raise

    def get_item(self, item_id):
        """Retrieve a single item from DynamoDB"""
        try:
            response = self.table.get_item(
                Key={'id': item_id}
            )
            return response.get('Item')

        except ClientError as e:
            self.logger.error(f"DynamoDB get operation failed for item {item_id}: {str(e)}")
            raise Exception(f"DynamoDB get operation failed: {str(e)}")

    def query_by_date_range(self, start_timestamp, end_timestamp):
        """Query items within a date range"""
        try:
            response = self.table.query(
                IndexName='timestamp-index',
                KeyConditionExpression='#ts BETWEEN :start AND :end',
                ExpressionAttributeNames={
                    '#ts': 'timestamp'
                },
                ExpressionAttributeValues={
                    ':start': start_timestamp,
                    ':end': end_timestamp
                }
            )
            items = response.get('Items', [])
            self.logger.info(f"Retrieved {len(items)} items for date range {start_timestamp} to {end_timestamp}")
            return items

        except ClientError as e:
            self.logger.error(f"DynamoDB query operation failed for range {start_timestamp} to {end_timestamp}: {str(e)}")
            raise Exception(f"DynamoDB query operation failed: {str(e)}")