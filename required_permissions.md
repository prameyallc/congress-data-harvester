# Required AWS Permissions

The service account `prameya-development-app-svc-account` needs the following DynamoDB permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeTable",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:Query"
            ],
            "Resource": "arn:aws:dynamodb:us-east-1:982235014033:table/prameya-development-dynamodb-table"
        }
    ]
}
```

This IAM policy needs to be attached to the service account to allow the application to:
1. Check if the table exists (DescribeTable)
2. Read individual items (GetItem)
3. Write individual items (PutItem)
4. Write multiple items in batch (BatchWriteItem)
5. Query items by date range (Query)

Once these permissions are granted, you can verify them by running:
```bash
python test_dynamo.py
```
