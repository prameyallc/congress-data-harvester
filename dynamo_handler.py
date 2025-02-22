import time
import boto3
from botocore.exceptions import ClientError
import logging
from monitoring import metrics
from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal
import json

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

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

                # Check if the required index exists
                indexes = table_desc['Table'].get('GlobalSecondaryIndexes', [])
                required_index = 'type-update_date-index'

                if not any(idx['IndexName'] == required_index for idx in indexes):
                    self.logger.warning(f"Table {self.table_name} exists but missing required index {required_index}")
                    # Add the missing index to the existing table
                    self.logger.info(f"Adding required index {required_index} to existing table...")
                    try:
                        # Check if table uses PAY_PER_REQUEST billing
                        billing_mode = table_desc['Table'].get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
                        self.logger.info(f"Table billing mode: {billing_mode}")

                        # Prepare the index update request
                        index_update = {
                            'Create': {
                                'IndexName': required_index,
                                'KeySchema': [
                                    {'AttributeName': 'type', 'KeyType': 'HASH'},
                                    {'AttributeName': 'update_date', 'KeyType': 'RANGE'}
                                ],
                                'Projection': {'ProjectionType': 'ALL'}
                            }
                        }

                        # Only add ProvisionedThroughput if not using PAY_PER_REQUEST
                        if billing_mode != 'PAY_PER_REQUEST':
                            index_update['Create']['ProvisionedThroughput'] = {
                                'ReadCapacityUnits': 5,
                                'WriteCapacityUnits': 5
                            }

                        dynamodb_client.update_table(
                            TableName=self.table_name,
                            AttributeDefinitions=[
                                {'AttributeName': 'type', 'AttributeType': 'S'},
                                {'AttributeName': 'update_date', 'AttributeType': 'S'}
                            ],
                            GlobalSecondaryIndexUpdates=[index_update]
                        )

                        self.logger.info("Waiting for index creation to complete...")
                        waiter = dynamodb_client.get_waiter('table_exists')
                        waiter.wait(
                            TableName=self.table_name,
                            WaiterConfig={'Delay': 5, 'MaxAttempts': 20}
                        )
                    except ClientError as e:
                        if 'AccessDeniedException' in str(e):
                            self.logger.warning(f"Unable to add index {required_index} due to permissions. Using table without index.")
                        else:
                            self.logger.error(f"Failed to add index: {str(e)}")
                            raise

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
                    {'AttributeName': 'id', 'KeyType': 'HASH'}  # Partition key
                ],
                AttributeDefinitions=[
                    # Primary key
                    {'AttributeName': 'id', 'AttributeType': 'S'},
                    # GSI attributes
                    {'AttributeName': 'type', 'AttributeType': 'S'},
                    {'AttributeName': 'update_date', 'AttributeType': 'S'},
                    {'AttributeName': 'congress', 'AttributeType': 'N'},
                    {'AttributeName': 'chamber', 'AttributeType': 'S'},
                    {'AttributeName': 'date', 'AttributeType': 'S'},
                    {'AttributeName': 'version', 'AttributeType': 'N'}
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
                    # GSI for querying by congress and type
                    {
                        'IndexName': 'congress-type-index',
                        'KeySchema': [
                            {'AttributeName': 'congress', 'KeyType': 'HASH'},
                            {'AttributeName': 'type', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    },
                    # GSI for querying by chamber and date
                    {
                        'IndexName': 'chamber-date-index',
                        'KeySchema': [
                            {'AttributeName': 'chamber', 'KeyType': 'HASH'},
                            {'AttributeName': 'date', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    },
                    # GSI for querying by version and update_date
                    {
                        'IndexName': 'version-update_date-index',
                        'KeySchema': [
                            {'AttributeName': 'version', 'KeyType': 'HASH'},
                            {'AttributeName': 'update_date', 'KeyType': 'RANGE'}
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
        """Query items by type and date range using GSI or fallback to scan"""
        try:
            # First try using the GSI if it exists
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
                self.logger.info(f"Retrieved {len(items)} items of type {item_type} between {start_date} and {end_date} using GSI")
                return items

            except ClientError as e:
                if 'ValidationException' in str(e) and 'index' in str(e):
                    # Index doesn't exist, fall back to scan with filter
                    self.logger.warning(f"Index not available, falling back to scan operation for type {item_type}")
                    response = self.table.scan(
                        FilterExpression='#type = :type AND #update_date BETWEEN :start_date AND :end_date',
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
                    self.logger.info(f"Retrieved {len(items)} items of type {item_type} between {start_date} and {end_date} using scan")
                    return items
                else:
                    raise

        except ClientError as e:
            self.logger.error(f"DynamoDB operation failed: {str(e)}")
            raise Exception(f"DynamoDB operation failed: {str(e)}")

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

            self.logger.debug(f"Attempting to store item: {json.dumps(item, indent=2, cls=DecimalEncoder)}")

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

            self.logger.info(f"Successfully stored item with ID: {item['id']}")

        except ClientError as e:
            duration = time.time() - start_time
            metrics.track_dynamo_operation(
                operation='PutItem',
                table=self.table_name,
                success=False,
                duration=duration
            )

            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']

            if error_code == 'ConditionalCheckFailedException':
                self.logger.info(f"Skipping item {item['id']} as a newer version exists")
                return

            self.logger.error(f"DynamoDB operation failed for item {item['id']}: Code={error_code}, Message={error_msg}")
            raise

    def batch_store_items(self, items: List[Dict[str, Any]], ttl_hours: int = 0) -> Tuple[int, List[Dict[str, Any]]]:
        """Store multiple items in DynamoDB using batch write"""
        if not self.table:
            raise Exception("DynamoDB table not initialized")

        successful_items = 0
        failed_items = []

        # Process items in batches of 25 (DynamoDB limit)
        batch_size = 25
        total_batches = len(items) // batch_size + (1 if len(items) % batch_size > 0 else 0)

        for batch_num, i in enumerate(range(0, len(items), batch_size), 1):
            batch_items = items[i:i + batch_size]
            self.logger.info(f"Processing batch {batch_num}/{total_batches} with {len(batch_items)} items")

            start_time = time.time()
            try:
                with self.table.batch_writer() as batch:
                    for item in batch_items:
                        try:
                            if 'id' not in item:
                                self.logger.warning(f"Skipping item without ID: {json.dumps(item, cls=DecimalEncoder)}")
                                continue

                            # Add timestamp and TTL
                            item['timestamp'] = int(time.time())
                            if ttl_hours > 0:
                                item['expiry_time'] = int(time.time()) + (ttl_hours * 3600)

                            # Add type if not present
                            if 'type' not in item:
                                item['type'] = 'unknown'

                            self.logger.debug(f"Attempting to store item of type {item.get('type')} with ID: {item.get('id')}")
                            self.logger.debug(f"Item content: {json.dumps(item, indent=2, cls=DecimalEncoder)}")

                            batch.put_item(Item=item)
                            successful_items += 1
                            self.logger.info(f"Successfully stored item with ID: {item.get('id')}")

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
        if failed_items:
            self.logger.warning("Failed items summary:")
            failed_by_type = {}
            for item in failed_items:
                item_type = item['item'].get('type', 'unknown')
                failed_by_type[item_type] = failed_by_type.get(item_type, 0) + 1

            for item_type, count in failed_by_type.items():
                self.logger.warning(f"  - {item_type}: {count} failed items")

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

    def query_by_congress_and_type(self, congress: int, item_type: str) -> List[Dict[str, Any]]:
        """Query items by congress and type using congress-type-index"""
        try:
            response = self.table.query(
                IndexName='congress-type-index',
                KeyConditionExpression='congress = :congress AND #type = :type',
                ExpressionAttributeNames={
                    '#type': 'type'
                },
                ExpressionAttributeValues={
                    ':congress': congress,
                    ':type': item_type
                }
            )
            items = response.get('Items', [])
            self.logger.info(f"Retrieved {len(items)} items for congress {congress} and type {item_type}")
            return items

        except ClientError as e:
            self.logger.error(f"DynamoDB query operation failed: {str(e)}")
            raise Exception(f"DynamoDB query operation failed: {str(e)}")

    def query_by_chamber_and_date_range(self, chamber: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Query items by chamber and date range using chamber-date-index"""
        try:
            response = self.table.query(
                IndexName='chamber-date-index',
                KeyConditionExpression='chamber = :chamber AND #date BETWEEN :start_date AND :end_date',
                ExpressionAttributeNames={
                    '#date': 'date'
                },
                ExpressionAttributeValues={
                    ':chamber': chamber,
                    ':start_date': start_date,
                    ':end_date': end_date
                }
            )
            items = response.get('Items', [])
            self.logger.info(f"Retrieved {len(items)} items for chamber {chamber} between {start_date} and {end_date}")
            return items

        except ClientError as e:
            self.logger.error(f"DynamoDB query operation failed: {str(e)}")
            raise Exception(f"DynamoDB query operation failed: {str(e)}")

    def query_by_version_and_update_date(self, version: int, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Query items by version and update date range using version-update_date-index"""
        try:
            response = self.table.query(
                IndexName='version-update_date-index',
                KeyConditionExpression='version = :version AND update_date BETWEEN :start_date AND :end_date',
                ExpressionAttributeValues={
                    ':version': version,
                    ':start_date': start_date,
                    ':end_date': end_date
                }
            )
            items = response.get('Items', [])
            self.logger.info(f"Retrieved {len(items)} items for version {version} between {start_date} and {end_date}")
            return items

        except ClientError as e:
            self.logger.error(f"DynamoDB query operation failed: {str(e)}")
            raise Exception(f"DynamoDB query operation failed: {str(e)}")