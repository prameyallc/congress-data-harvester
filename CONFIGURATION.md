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

### Date Range Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| max_range_days | Maximum days in a single request | 365 | ≥1 |
| min_date | Earliest allowed date | 1789-03-04 | Valid date string |
| default_start_date | Default start if not specified | 2024-01-01 | Valid date string |
| default_end_date | Default end if not specified | 2024-12-31 | Valid date string |

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

## Environment Variables

The following environment variables are required:

```bash
# AWS Credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2  # or your preferred region

# Congress.gov API Key
export CONGRESS_API_KEY=your_congress_api_key
```

## Required IAM Permissions

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