import time
import boto3
from botocore.exceptions import ClientError

class DynamoHandler:
    def __init__(self, config):
        self.table_name = config['table_name']
        self.dynamodb = boto3.resource('dynamodb', region_name=config['region'])
        self.table = self.dynamodb.Table(self.table_name)

    def store_item(self, item):
        """Store a single item in DynamoDB"""
        try:
            # Ensure item has required attributes
            if 'id' not in item:
                raise ValueError("Item must have an 'id' attribute")

            # Add timestamp for tracking
            item['timestamp'] = int(time.time())

            self.table.put_item(
                Item=item,
                # Implement conditional write to avoid duplicates
                ConditionExpression='attribute_not_exists(id) OR version < :new_version',
                ExpressionAttributeValues={
                    ':new_version': item.get('version', 1)
                }
            )

        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # Item exists with newer or same version - skip
                return
            raise Exception(f"DynamoDB operation failed: {str(e)}")

    def batch_store_items(self, items):
        """Store multiple items in DynamoDB using batch write"""
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    if 'id' not in item:
                        continue

                    item['timestamp'] = int(time.time())
                    batch.put_item(Item=item)

        except ClientError as e:
            raise Exception(f"DynamoDB batch operation failed: {str(e)}")

    def get_item(self, item_id):
        """Retrieve a single item from DynamoDB"""
        try:
            response = self.table.get_item(
                Key={'id': item_id}
            )
            return response.get('Item')

        except ClientError as e:
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
            return response.get('Items', [])

        except ClientError as e:
            raise Exception(f"DynamoDB query operation failed: {str(e)}")