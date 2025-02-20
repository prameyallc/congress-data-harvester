import boto3
import logging
import time
from botocore.exceptions import ClientError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('dynamo_test')

def test_dynamo_permissions():
    """Test specific DynamoDB permissions"""
    try:
        # Initialize DynamoDB client
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table_name = 'prameya-development-dynamodb-table'

        # Test 1: Verify we can list tables
        logger.info("Testing ListTables permission...")
        tables = list(dynamodb.tables.all())
        logger.info(f"ListTables succeeded, found {len(tables)} tables")

        # Test 2: Try to describe our table
        logger.info(f"Testing DescribeTable permission for {table_name}...")
        try:
            table = dynamodb.Table(table_name)
            table.load()
            logger.info("DescribeTable succeeded")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDeniedException':
                logger.error("Missing DescribeTable permission")
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info("Table does not exist, will test creation permission")
                try:
                    # Test 3: Try to create the table
                    logger.info("Testing CreateTable permission...")
                    table = dynamodb.create_table(
                        TableName=table_name,
                        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
                        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
                        BillingMode='PAY_PER_REQUEST'
                    )
                    logger.info("CreateTable succeeded")
                except ClientError as create_error:
                    logger.error(f"CreateTable failed: {create_error.response['Error']['Message']}")
            raise

        # Test 4: Try to write an item
        logger.info("Testing PutItem permission...")
        try:
            test_item = {
                'id': 'test',
                'data': 'test',
                'timestamp': int(time.time())
            }
            table.put_item(Item=test_item)
            logger.info("PutItem succeeded")
        except ClientError as e:
            logger.error(f"PutItem failed: {e.response['Error']['Message']}")
            raise

        # Test 5: Try to read the item
        logger.info("Testing GetItem permission...")
        try:
            table.get_item(Key={'id': 'test'})
            logger.info("GetItem succeeded")
        except ClientError as e:
            logger.error(f"GetItem failed: {e.response['Error']['Message']}")
            raise

        return True

    except Exception as e:
        logger.error(f"Permission test failed: {str(e)}")
        return False

if __name__ == "__main__":
    if test_dynamo_permissions():
        logger.info("All DynamoDB permission tests completed successfully")
    else:
        logger.error("DynamoDB permission tests failed")