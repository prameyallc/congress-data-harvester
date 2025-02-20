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
            "Resource": "arn:aws:dynamodb:us-west-2:982235014033:table/prameya-development-dynamodb-table"
        }
    ]
}
```

## CloudWatch Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
```

## Verification
You can verify these permissions by running:
```bash
python test_dynamo.py
```

For CloudWatch permissions, the metrics collection will automatically happen when running the main application:
```bash
python congress_downloader.py --mode incremental --lookback-days 1