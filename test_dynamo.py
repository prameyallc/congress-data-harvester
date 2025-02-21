import boto3
import logging
import time
from botocore.exceptions import ClientError, WaiterError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('dynamo_test')

def wait_for_table_state(dynamodb, table_name, desired_state='ACTIVE', max_retries=10, delay=5):
    """Wait for DynamoDB table to reach desired state"""
    for attempt in range(max_retries):
        try:
            response = dynamodb.meta.client.describe_table(TableName=table_name)
            current_state = response['Table']['TableStatus']
            if current_state == desired_state:
                logger.info(f"Table {table_name} is {desired_state}")
                return True
            logger.info(f"Table {table_name} is {current_state}, waiting...")
            time.sleep(delay)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                if desired_state == 'DELETED':
                    logger.info(f"Table {table_name} is deleted")
                    return True
                logger.info(f"Table {table_name} not found")
                return False
            raise
    return False

def test_dynamo_permissions():
    """Test specific DynamoDB permissions"""
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table_name = 'prameya-development-dynamodb-table'

        # Test 1: Delete table if exists
        logger.info("Checking if table exists...")
        try:
            table = dynamodb.Table(table_name)
            # Wait for any existing operations to complete
            if not wait_for_table_state(dynamodb, table_name, 'ACTIVE'):
                logger.warning("Table is not in ACTIVE state, may affect deletion")

            table.delete()
            logger.info(f"Initiated deletion of table {table_name}")
            # Wait for table to be fully deleted
            if wait_for_table_state(dynamodb, table_name, 'DELETED'):
                logger.info(f"Successfully deleted table {table_name}")
            time.sleep(5)  # Additional wait to ensure AWS cleanup
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                logger.error(f"Error deleting table: {str(e)}")
                if e.response['Error']['Code'] == 'ResourceInUseException':
                    logger.info("Table is still being created or modified, proceeding with test")
                else:
                    raise

        # Test 2: Create table with correct schema
        logger.info("Creating table with new schema...")
        table = dynamodb.create_table(
            TableName=table_name,
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
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Wait for table to be ready
        if not wait_for_table_state(dynamodb, table_name, 'ACTIVE'):
            raise Exception("Table did not become active in the expected timeframe")
        logger.info("Table created successfully")

        # Test 3: Write test item
        logger.info("Testing PutItem permission...")
        test_item = {
            'id': '117-hr-3076',
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
            'update_date_including_text': '2022-09-29T03:27:05Z',
            'introduced_date': '2022-09-29',
            'sponsors': [],
            'committees': [],
            'url': 'https://api.congress.gov/v3/bill/117/hr/3076?format=json',
            'timestamp': int(time.time())
        }

        table.put_item(Item=test_item)
        logger.info("PutItem succeeded")

        # Test 4: Read the item back
        logger.info("Testing GetItem permission...")
        response = table.get_item(Key={'id': '117-hr-3076'})
        item = response.get('Item')

        if item:
            logger.info("GetItem succeeded")
            logger.info(f"Retrieved item: {item}")
        else:
            logger.error("GetItem succeeded but no item found")
            return False

        return True

    except Exception as e:
        logger.error(f"Permission test failed: {str(e)}")
        return False

if __name__ == "__main__":
    if test_dynamo_permissions():
        logger.info("All DynamoDB permission tests completed successfully")
    else:
        logger.error("DynamoDB permission tests failed")