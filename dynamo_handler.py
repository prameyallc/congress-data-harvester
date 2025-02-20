import time
import boto3
from botocore.exceptions import ClientError
import logging

class DynamoHandler:
    def __init__(self, config):
        self.table_name = config['table_name']
        self.dynamodb = boto3.resource('dynamodb', region_name=config['region'])
        self.table = self.dynamodb.Table(self.table_name)
        self.logger = logging.getLogger('congress_downloader')

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
                            failed_items.append(item)

                self.logger.debug(f"Processed batch of {len(batch_items)} items")

            self.logger.info(f"Batch write completed: {successful_items} items successful, {len(failed_items)} failed")
            if failed_items:
                self.logger.debug(f"Failed items: {failed_items}")

            return successful_items, failed_items

        except ClientError as e:
            self.logger.error(f"DynamoDB batch operation failed: {str(e)}")
            raise Exception(f"DynamoDB batch operation failed: {str(e)}")

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