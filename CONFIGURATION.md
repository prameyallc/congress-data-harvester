{
    "api": {
        "base_url": "https://api.congress.gov/v3",
        "api_key": "",  // Use environment variable instead
        "rate_limit": {
            "requests_per_second": 5,
            "max_retries": 3,
            "retry_delay": 1
        }
    },
    "dynamodb": {
        "table_name": "congress-data-dev",
        "region": "us-west-2"
    },
    "logging": {
        "level": "DEBUG",
        "file": "logs/congress_downloader.log",
        "max_size": 10485760,
        "backup_count": 5
    },
    "download": {
        "batch_size": 100,
        "default_lookback_days": 30,
        "parallel": {
            "max_workers": 3,
            "chunk_size": 5
        }
    }
}
```

## Configuration Sections

### API Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| base_url | Congress.gov API base URL | https://api.congress.gov/v3 | Valid URL |
| requests_per_second | Maximum API requests per second | 5 | 1-10 |
| max_retries | Maximum retry attempts | 3 | 1-10 |
| retry_delay | Base delay between retries (seconds) | 1 | 1-60 |

### DynamoDB Configuration

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| table_name | DynamoDB table name | congress-data-dev | Must be unique |
| region | AWS region | us-west-2 | Valid AWS region |

### Logging Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| level | Log level | DEBUG | DEBUG, INFO, WARNING, ERROR |
| file | Log file path | logs/congress_downloader.log | Valid file path |
| max_size | Maximum log file size (bytes) | 10MB (10485760) | >0 |
| backup_count | Number of backup files | 5 | ≥0 |

### Download Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| batch_size | Items per batch | 100 | 1-1000 |
| default_lookback_days | Default days for incremental mode | 30 | ≥1 |
| max_workers | Maximum parallel workers | 3 | 1-10 |
| chunk_size | Items per worker | 5 | 1-100 |

## Performance Tuning

### Resource Usage Profiles

#### Low Resource Usage
```json
{
    "download": {
        "batch_size": 50,
        "parallel": {
            "max_workers": 2,
            "chunk_size": 3
        }
    }
}
```

#### High Performance
```json
{
    "download": {
        "batch_size": 200,
        "parallel": {
            "max_workers": 5,
            "chunk_size": 10
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **DynamoDB Connection Errors**
   - Verify AWS credentials
   - Check table name uniqueness
   - Ensure proper IAM permissions
   - Verify region setting

2. **API Rate Limiting**
   - Reduce parallel workers
   - Increase retry delay
   - Verify API key validity

3. **Memory Issues**
   - Reduce batch size
   - Decrease parallel workers
   - Monitor system resources

### Required IAM Permissions

Minimum required permissions for DynamoDB:

```json
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

## Logging

The application uses rotating log files with configurable settings:

```python
# Example logging configuration
{
    "logging": {
        "level": "DEBUG",  # Options: DEBUG, INFO, WARNING, ERROR
        "file": "logs/congress_downloader.log",
        "max_size": 10485760,  # 10MB
        "backup_count": 5
    }
}