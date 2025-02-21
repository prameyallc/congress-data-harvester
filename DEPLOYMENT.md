# Deployment Guide

This guide covers deploying the Congress Data Downloader on various platforms.

## Prerequisites

### System Requirements
- Python 3.11 or higher
- 1GB RAM minimum (2GB recommended)
- AWS credentials with DynamoDB access
- Congress.gov API key

### Required Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
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

## Environment Setup

### Required Environment Variables
```bash
# AWS Configuration
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2  # or your preferred region

# Congress.gov API
export CONGRESS_API_KEY=your_congress_api_key
```

### Application Configuration
Create `config.json` in the application root:
```json
{
    "api": {
        "base_url": "https://api.congress.gov/v3",
        "rate_limit": {
            "requests_per_second": 5,
            "max_retries": 3,
            "retry_delay": 1
        }
    },
    "dynamodb": {
        "table_name": "congress-data-prod",
        "region": "us-west-2"
    },
    "logging": {
        "level": "INFO",
        "file": "logs/congress_downloader.log",
        "max_size": 10485760,
        "backup_count": 5
    },
    "download": {
        "batch_size": 100,
        "default_lookback_days": 30,
        "date_ranges": {
            "max_range_days": 365,
            "min_date": "1789-03-04",
            "default_start_date": "2024-01-01",
            "default_end_date": "2024-12-31"
        },
        "parallel": {
            "max_workers": 3,
            "chunk_size": 5
        }
    }
}
```

## Platform-Specific Deployment

### 1. Replit Deployment

1. Create a new Python repl
2. Upload project files or clone from repository
3. Set environment variables in Replit Secrets
4. Configure the run button in `.replit`:
```toml
[nix]
channel = "stable-24_05"

[deployment]
run = ["sh", "-c", "python congress_downloader.py --mode incremental --lookback-days 1"]

[[workflows.workflow]]
name = "Congress Data Downloader"
author = "agent"

[workflows.workflow.metadata]
agentRequireRestartOnSave = false

[[workflows.workflow.tasks]]
task = "packager.installForAll"

[[workflows.workflow.tasks]]
task = "shell.exec"
args = "python congress_downloader.py --mode refresh --start-date 2024-01-01 --end-date 2024-01-31"
```

### 2. AWS Lambda Deployment

1. Package application:
```bash
zip -r congress_downloader.zip . -x "*.pyc" -x "*.git*" -x "logs/*"
```

2. Create Lambda function:
- Runtime: Python 3.11
- Memory: 512MB minimum
- Timeout: 5 minutes
- Handler: `congress_downloader.lambda_handler`

3. Add Lambda wrapper (`lambda_handler.py`):
```python
def lambda_handler(event, context):
    import congress_downloader
    congress_downloader.main()
    return {'statusCode': 200}
```

4. Configure CloudWatch Events for scheduling:
```json
{
    "schedule": "rate(1 day)"
}
```

### 3. Heroku Deployment

1. Create `Procfile`:
```
worker: python congress_downloader.py --mode incremental --lookback-days 1
```

2. Create `runtime.txt`:
```
python-3.11.7
```

3. Deploy:
```bash
heroku create congress-downloader
heroku config:set AWS_ACCESS_KEY_ID=your_key
heroku config:set AWS_SECRET_ACCESS_KEY=your_secret
heroku config:set AWS_DEFAULT_REGION=us-west-2
heroku config:set CONGRESS_API_KEY=your_api_key
git push heroku main
```

4. Scale worker:
```bash
heroku ps:scale worker=1
```

### 4. Docker Deployment

1. Create `Dockerfile`:
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

CMD ["python", "congress_downloader.py", "--mode", "incremental", "--lookback-days", "1"]
```

2. Build and run:
```bash
docker build -t congress-downloader .
docker run -d \
    -e AWS_ACCESS_KEY_ID=your_key \
    -e AWS_SECRET_ACCESS_KEY=your_secret \
    -e AWS_DEFAULT_REGION=us-west-2 \
    -e CONGRESS_API_KEY=your_api_key \
    congress-downloader
```

## Scheduling and Automation

### Cron Configuration

For automated data updates, configure cron jobs:

1. Daily incremental update:
```crontab
0 0 * * * python /path/to/congress_downloader.py --mode incremental --lookback-days 1
```

2. Weekly refresh:
```crontab
0 0 * * 0 python /path/to/congress_downloader.py --mode refresh --start-date $(date -d "7 days ago" +%Y-%m-%d) --end-date $(date +%Y-%m-%d)
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: congress-downloader
spec:
  schedule: "0 0 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: congress-downloader
            image: congress-downloader:latest
            env:
              - name: AWS_ACCESS_KEY_ID
                valueFrom:
                  secretKeyRef:
                    name: aws-credentials
                    key: access-key
              - name: AWS_SECRET_ACCESS_KEY
                valueFrom:
                  secretKeyRef:
                    name: aws-credentials
                    key: secret-key
              - name: AWS_DEFAULT_REGION
                value: "us-west-2"
              - name: CONGRESS_API_KEY
                valueFrom:
                  secretKeyRef:
                    name: congress-api
                    key: api-key
          restartPolicy: OnFailure
```

## Monitoring and Logging

### CloudWatch Logging

1. Enable CloudWatch logging in `config.json`:
```json
{
    "logging": {
        "cloudwatch": {
            "enabled": true,
            "log_group": "/congress-downloader/prod",
            "retention_days": 30
        }
    }
}
```

2. Additional IAM permissions needed:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
```

### Health Checks

Create a health check endpoint (`health_check.py`):
```python
#!/usr/bin/env python3
import json
import sys
import boto3
import requests
from congress_api import CongressAPI
from logger_config import setup_logger
import os

def load_config():
    """Load application configuration"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON in config.json")
        return None

def check_aws_credentials():
    """Verify AWS credentials are properly configured"""
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        return {
            'status': 'healthy',
            'identity': identity['Arn']
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

def check_congress_api(config):
    """Verify Congress.gov API access"""
    try:
        api = CongressAPI(config['api'])
        base_url = config['api']['base_url']
        api_key = os.environ.get('CONGRESS_API_KEY')

        response = requests.get(
            f"{base_url}/bill",
            params={'api_key': api_key, 'limit': 1}
        )
        response.raise_for_status()

        return {
            'status': 'healthy',
            'endpoint': f"{base_url}/bill"
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

def check_dynamodb(config):
    """Verify DynamoDB table access"""
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(config['dynamodb']['table_name'])
        table.get_item(
            Key={
                'id': 'health_check'
            }
        )
        return {
            'status': 'healthy',
            'table': config['dynamodb']['table_name']
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }

def check_environment():
    """Verify required environment variables"""
    required_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_DEFAULT_REGION',
        'CONGRESS_API_KEY'
    ]

    missing = [var for var in required_vars if not os.environ.get(var)]
    return {
        'status': 'healthy' if not missing else 'unhealthy',
        'missing_variables': missing
    }

def main():
    """Run all health checks"""
    config = load_config()
    if not config:
        sys.exit(1)

    health_status = {
        'environment': check_environment(),
        'aws_credentials': check_aws_credentials(),
        'congress_api': check_congress_api(config),
        'dynamodb': check_dynamodb(config)
    }

    # Overall status is healthy only if all components are healthy
    health_status['overall'] = {
        'status': 'healthy' if all(
            component['status'] == 'healthy' 
            for component in health_status.values()
        ) else 'unhealthy'
    }

    print(json.dumps(health_status, indent=2))
    return 0 if health_status['overall']['status'] == 'healthy' else 1

if __name__ == "__main__":
    sys.exit(main())
```

## Troubleshooting

### Common Issues

1. Rate Limiting
```
Error: Rate limit exceeded
```
- Reduce parallel workers in config.json
- Increase retry delay
- Add exponential backoff

2. DynamoDB Throughput
```
ProvisionedThroughputExceededException
```
- Reduce batch size
- Implement retry with backoff
- Consider on-demand capacity

3. Memory Issues
```
MemoryError
```
- Reduce batch_size in config.json
- Decrease parallel workers
- Increase available memory

### Log Analysis

Monitor key metrics:
- API request success rate
- DynamoDB write throughput
- Processing time per date
- Memory usage

Example log patterns:
```
2024-02-21 00:00:01 - Processing date: 2024-02-20
2024-02-21 00:00:02 - Retrieved 150 bills
2024-02-21 00:00:03 - Successfully stored 150 items
```

## Security Considerations

1. Secrets Management
- Use platform-specific secret stores
- Never commit credentials to version control
- Rotate credentials regularly

2. Network Security
- Use VPC for AWS deployments
- Enable HTTPS/TLS
- Implement IP whitelisting if needed

3. Access Control
- Follow principle of least privilege
- Regular IAM audits
- Enable MFA for AWS accounts