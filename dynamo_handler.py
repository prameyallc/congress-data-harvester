import time
import boto3
from botocore.exceptions import ClientError
import logging
from monitoring import metrics
from typing import Dict, List, Any, Optional, Tuple

class DynamoHandler:
    def __init__(self, config):
        self.table_name = config['table_name']
        self.dynamodb = boto3.resource('dynamodb', region_name=config['region'])
        self.table = None
        self.logger = logging.getLogger('congress_downloader')
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Ensure DynamoDB table exists and is ready with optimized indexes"""
        try:
            self.logger.info(f"Attempting to access DynamoDB table {self.table_name}")

            try:
                # Try to describe the table first
                dynamodb_client = self.dynamodb.meta.client
                table_desc = dynamodb_client.describe_table(TableName=self.table_name)
                self.table = self.dynamodb.Table(self.table_name)
                self.logger.info(f"Successfully connected to table {self.table_name}")
                return

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    self._create_table_with_indexes()
                else:
                    raise

        except Exception as e:
            self.logger.error(f"Failed to ensure table exists: {str(e)}")
            raise

    def _create_table_with_indexes(self):
        """Create table with optimized indexes for Congress.gov data"""
        try:
            self.logger.info("Creating new table with optimized indexes...")

            # Create the table with GSIs and LSIs
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'update_date', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'},
                    {'AttributeName': 'update_date', 'AttributeType': 'S'},
                    {'AttributeName': 'type', 'AttributeType': 'S'},
                    {'AttributeName': 'congress', 'AttributeType': 'N'},
                    {'AttributeName': 'number', 'AttributeType': 'N'}
                ],
                GlobalSecondaryIndexes=[
                    # GSI for querying by type and update date
                    {
                        'IndexName': 'type-update_date-index',
                        'KeySchema': [
                            {'AttributeName': 'type', 'KeyType': 'HASH'},
                            {'AttributeName': 'update_date', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    },
                    # GSI for querying by congress
                    {
                        'IndexName': 'congress-number-index',
                        'KeySchema': [
                            {'AttributeName': 'congress', 'KeyType': 'HASH'},
                            {'AttributeName': 'number', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ],
                BillingMode='PROVISIONED',
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                },
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                },
                TimeToLiveSpecification={
                    'Enabled': True,
                    'AttributeName': 'expiry_time'
                }
            )

            self.logger.info("Waiting for table creation...")
            table.wait_until_exists()
            self.table = table
            self.logger.info(f"Table {self.table_name} created successfully with optimized indexes")

        except Exception as e:
            self.logger.error(f"Failed to create table: {str(e)}")
            raise

    def query_by_type_and_date_range(self, item_type: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Query items by type and date range using GSI"""
        try:
            response = self.table.query(
                IndexName='type-update_date-index',
                KeyConditionExpression='#type = :type AND #update_date BETWEEN :start_date AND :end_date',
                ExpressionAttributeNames={
                    '#type': 'type',
                    '#update_date': 'update_date'
                },
                ExpressionAttributeValues={
                    ':type': item_type,
                    ':start_date': start_date,
                    ':end_date': end_date
                }
            )
            items = response.get('Items', [])
            self.logger.info(f"Retrieved {len(items)} items of type {item_type} between {start_date} and {end_date}")
            return items

        except ClientError as e:
            self.logger.error(f"DynamoDB query operation failed: {str(e)}")
            raise Exception(f"DynamoDB query operation failed: {str(e)}")

    def query_by_congress_and_number(self, congress: int, number: int) -> List[Dict[str, Any]]:
        """Query items by congress and number using GSI"""
        try:
            response = self.table.query(
                IndexName='congress-number-index',
                KeyConditionExpression='congress = :congress AND #number = :number',
                ExpressionAttributeNames={
                    '#number': 'number'
                },
                ExpressionAttributeValues={
                    ':congress': congress,
                    ':number': number
                }
            )
            items = response.get('Items', [])
            self.logger.info(f"Retrieved {len(items)} items for congress {congress} and number {number}")
            return items

        except ClientError as e:
            self.logger.error(f"DynamoDB query operation failed: {str(e)}")
            raise Exception(f"DynamoDB query operation failed: {str(e)}")

    def store_item(self, item: Dict[str, Any], ttl_hours: int = 0) -> None:
        """Store a single item in DynamoDB"""
        if not self.table:
            raise Exception("DynamoDB table not initialized")

        start_time = time.time()
        try:
            if 'id' not in item:
                raise ValueError("Item must have an 'id' attribute")

            # Add timestamp for tracking
            item['timestamp'] = int(time.time())
            if ttl_hours > 0:
                item['expiry_time'] = int(time.time()) + (ttl_hours * 3600)

            # Add type if not present (for filtering)
            if 'type' not in item:
                item['type'] = 'unknown'

            self.table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(id) OR (attribute_exists(update_date) AND update_date < :new_update_date)',
                ExpressionAttributeValues={
                    ':new_update_date': item.get('update_date', '0')
                }
            )

            duration = time.time() - start_time
            metrics.track_dynamo_operation(
                operation='PutItem',
                table=self.table_name,
                success=True,
                duration=duration
            )

            self.logger.debug(f"Successfully stored item with ID: {item['id']}")

        except ClientError as e:
            duration = time.time() - start_time
            metrics.track_dynamo_operation(
                operation='PutItem',
                table=self.table_name,
                success=False,
                duration=duration
            )

            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                self.logger.info(f"Skipping item {item['id']} as a newer version exists")
                return

            self.logger.error(f"DynamoDB operation failed for item {item['id']}: {str(e)}")
            raise

    def batch_store_items(self, items: List[Dict[str, Any]], ttl_hours: int = 0) -> Tuple[int, List[Dict[str, Any]]]:
        """Store multiple items in DynamoDB using batch write"""
        if not self.table:
            raise Exception("DynamoDB table not initialized")

        successful_items = 0
        failed_items = []

        # Process items in batches of 25 (DynamoDB limit)
        batch_size = 25
        for i in range(0, len(items), batch_size):
            batch_items = items[i:i + batch_size]
            self.logger.info(f"Processing batch of {len(batch_items)} items")

            start_time = time.time()
            try:
                with self.table.batch_writer() as batch:
                    for item in batch_items:
                        try:
                            if 'id' not in item:
                                self.logger.warning(f"Skipping item without ID: {item}")
                                continue

                            # Add timestamp and TTL
                            item['timestamp'] = int(time.time())
                            if ttl_hours > 0:
                                item['expiry_time'] = int(time.time()) + (ttl_hours * 3600)

                            # Add type if not present
                            if 'type' not in item:
                                item['type'] = 'unknown'

                            batch.put_item(Item=item)
                            successful_items += 1

                        except Exception as e:
                            self.logger.error(f"Failed to write item {item.get('id', 'unknown')}: {str(e)}")
                            failed_items.append({
                                'id': item.get('id', 'unknown'),
                                'error': str(e),
                                'item': item
                            })

                duration = time.time() - start_time
                metrics.track_dynamo_operation(
                    operation='BatchWriteItem',
                    table=self.table_name,
                    success=True,
                    duration=duration
                )

            except ClientError as e:
                duration = time.time() - start_time
                metrics.track_dynamo_operation(
                    operation='BatchWriteItem',
                    table=self.table_name,
                    success=False,
                    duration=duration
                )

                error_code = e.response['Error']['Code']
                error_msg = e.response['Error']['Message']
                self.logger.error(f"Batch write failed - Code: {error_code}, Message: {error_msg}")
                failed_items.extend([{
                    'id': item.get('id', 'unknown'),
                    'error': error_msg,
                    'item': item
                } for item in batch_items])

        self.logger.info(f"Batch write completed: {successful_items} items successful, {len(failed_items)} failed")
        return successful_items, failed_items

    def get_item(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single item from DynamoDB"""
        try:
            response = self.table.get_item(
                Key={'id': item_id}
            )
            return response.get('Item')

        except ClientError as e:
            self.logger.error(f"DynamoDB get operation failed for item {item_id}: {str(e)}")
            raise Exception(f"DynamoDB get operation failed: {str(e)}")

    def scan_by_type(self, item_type: str) -> List[Dict[str, Any]]:
        """Scan items by type attribute"""
        try:
            response = self.table.scan(
                FilterExpression='#type = :type',
                ExpressionAttributeNames={
                    '#type': 'type'
                },
                ExpressionAttributeValues={
                    ':type': item_type
                }
            )
            items = response.get('Items', [])
            self.logger.info(f"Retrieved {len(items)} items of type {item_type}")
            return items

        except ClientError as e:
            self.logger.error(f"DynamoDB scan operation failed for type {item_type}: {str(e)}")
            raise Exception(f"DynamoDB scan operation failed: {str(e)}")