{
         "Version": "2012-10-17",
         "Statement": [
             {
                 "Effect": "Allow",
                 "Action": [
                     "dynamodb:CreateTable",
                     "dynamodb:DeleteTable",
                     "dynamodb:DescribeTable",
                     "dynamodb:PutItem",
                     "dynamodb:BatchWriteItem",
                     "dynamodb:Query",
                     "dynamodb:GetItem"
                 ],
                 "Resource": "arn:aws:dynamodb:*:*:table/congress-data-*"
             }
         ]
     }
     ```

## Setup

1. **Set Environment Variables**
```bash
# AWS Credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2  # or your preferred region

# Congress.gov API Key
export CONGRESS_API_KEY=your_congress_api_key
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Verify Setup**
```bash
python test_dynamo.py
```

## Usage

### 1. Incremental Download (Recommended)

Downloads recent updates (last 24 hours by default):

```bash
python congress_downloader.py --mode incremental --lookback-days 1
```

### 2. Bulk Download

Downloads all available data:

```bash
python congress_downloader.py --mode bulk
```

### 3. Date Range Download

Downloads data for a specific period:

```bash
python congress_downloader.py --mode refresh --start-date 2024-01-01 --end-date 2024-01-31
```

## Documentation

- [Configuration Guide](CONFIGURATION.md) - Detailed configuration options and environment setup
- [API Documentation](API.md) - Congress.gov API integration details
- [Architecture Overview](ARCHITECTURE.md) - System design and components
- [Contributing Guidelines](CONTRIBUTING.md) - Development and contribution guidelines

## Troubleshooting

### Common Issues

1. **Missing AWS Credentials**
   ```
   botocore.exceptions.NoCredentialsError: Unable to locate credentials
   ```
   - Ensure AWS environment variables are set
   - Verify IAM user has correct permissions
   - Check AWS region configuration

2. **DynamoDB Table Creation Fails**
   ```
   botocore.exceptions.ClientError: Table already exists
   ```
   - Verify table name is unique
   - Check IAM permissions
   - Review table configuration in config.json

3. **API Rate Limiting**
   ```
   HTTP 429: Too Many Requests