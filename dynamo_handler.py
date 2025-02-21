import time
import boto3
from botocore.exceptions import ClientError
import logging
from monitoring import metrics

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
            self.logger.info(f"Attempting to access DynamoDB table {self.table_name}")

            try:
                # Try to describe the table first
                dynamodb_client = self.dynamodb.meta.client
                try:
                    dynamodb_client.describe_table(TableName=self.table_name)
                    self.table = self.dynamodb.Table(self.table_name)
                    self.logger.info(f"Successfully connected to table {self.table_name}")
                except dynamodb_client.exceptions.ResourceNotFoundException:
                    # Table doesn't exist, create it
                    self.logger.info(f"Table {self.table_name} not found, creating...")
                    self.table = self.dynamodb.create_table(
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
                        BillingMode='PAY_PER_REQUEST'
                    )
                    self.table.wait_until_exists()
                    self.logger.info(f"Created table {self.table_name}")

                # Test table access with a simple operation
                test_item = {
                    'id': 'permission_test',
                    'timestamp': int(time.time())
                }
                self.table.put_item(Item=test_item)
                self.logger.info(f"Successfully connected to table {self.table_name}")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']

                if error_code == 'ResourceNotFoundException':
                    self.logger.error(f"""
Table {self.table_name} does not exist. 
Required permission: dynamodb:CreateTable
Resource: arn:aws:dynamodb:{self.dynamodb.meta.client.meta.region_name}:*:table/{self.table_name}
""")
                    raise Exception(f"Table {self.table_name} does not exist")

                elif error_code == 'AccessDeniedException':
                    self.logger.error(f"""
Insufficient permissions to access DynamoDB table {self.table_name}
Required permissions:
- dynamodb:DescribeTable
- dynamodb:GetItem
- dynamodb:PutItem
- dynamodb:BatchWriteItem
- dynamodb:Query
Resource: arn:aws:dynamodb:{self.dynamodb.meta.client.meta.region_name}:*:table/{self.table_name}
""")
                    raise Exception("Insufficient DynamoDB permissions")

                else:
                    self.logger.error(f"Unexpected DynamoDB error: {error_code} - {error_message}")
                    raise

        except Exception as e:
            self.logger.error(f"Failed to ensure table exists: {str(e)}")
            raise

    def store_item(self, item):
        """Store a single item in DynamoDB"""
        if not self.table:
            raise Exception("DynamoDB table not initialized")

        start_time = time.time()
        try:
            if 'id' not in item:
                raise ValueError("Item must have an 'id' attribute")

            # Add timestamp for the secondary index
            item['timestamp'] = int(time.time())

            self.table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(id) OR (attribute_exists(update_date) AND update_date < :new_update_date)',
                ExpressionAttributeValues={
                    ':new_update_date': item.get('update_date', '0')
                }
            )

            duration = time.time() - start_time
            try:
                metrics.track_dynamo_operation(
                    operation='PutItem',
                    table=self.table_name,
                    success=True,
                    duration=duration
                )
            except Exception as metric_error:
                self.logger.warning(f"Failed to track metrics: {str(metric_error)}")

            self.logger.debug(f"Successfully stored item with ID: {item['id']}")

        except ClientError as e:
            duration = time.time() - start_time
            try:
                metrics.track_dynamo_operation(
                    operation='PutItem',
                    table=self.table_name,
                    success=False,
                    duration=duration
                )
            except Exception as metric_error:
                self.logger.warning(f"Failed to track metrics: {str(metric_error)}")

            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                self.logger.info(f"Skipping item {item['id']} as a newer version exists")
                return

            if e.response['Error']['Code'] == 'AccessDeniedException':
                self.logger.error(f"""
Failed to write item to DynamoDB table {self.table_name}
Required permission: dynamodb:PutItem
Resource: arn:aws:dynamodb:{self.dynamodb.meta.client.meta.region_name}:*:table/{self.table_name}
""")

            self.logger.error(f"DynamoDB operation failed for item {item['id']}: {str(e)}")
            raise

    def batch_store_items(self, items):
        """Store multiple items in DynamoDB using batch write"""
        if not self.table:
            raise Exception("DynamoDB table not initialized")

        try:
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

                                item['timestamp'] = int(time.time())
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
                    try:
                        metrics.track_dynamo_operation(
                            operation='BatchWriteItem',
                            table=self.table_name,
                            success=True,
                            duration=duration
                        )
                    except Exception as metric_error:
                        self.logger.warning(f"Failed to track metrics: {str(metric_error)}")

                except ClientError as e:
                    duration = time.time() - start_time
                    try:
                        metrics.track_dynamo_operation(
                            operation='BatchWriteItem',
                            table=self.table_name,
                            success=False,
                            duration=duration
                        )
                    except Exception as metric_error:
                        self.logger.warning(f"Failed to track metrics: {str(metric_error)}")

                    if e.response['Error']['Code'] == 'AccessDeniedException':
                        self.logger.error(f"""
Failed to batch write items to DynamoDB table {self.table_name}
Required permission: dynamodb:BatchWriteItem
Resource: arn:aws:dynamodb:{self.dynamodb.meta.client.meta.region_name}:*:table/{self.table_name}
""")

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