# Configuration Guide

This document details the configuration options and setup process for the Congress Data Downloader.

## Quick Start

1. **Essential Environment Variables**
```bash
# AWS Credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2

# Congress.gov API
export CONGRESS_API_KEY=your_congress_api_key
```

2. **Basic Configuration File**
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
        "table_name": "congress-data-dev",
        "region": "us-west-2"
    },
    "logging": {
        "level": "INFO",
        "file": "logs/congress_downloader.log"
    }
}
```

## Configuration Sections

### 1. API Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| base_url | Congress.gov API base URL | https://api.congress.gov/v3 | Valid URL |
| requests_per_second | Maximum API requests per second | 5 | 1-10 |
| max_retries | Maximum retry attempts | 3 | 1-10 |
| retry_delay | Base delay between retries (seconds) | 1 | 1-60 |

Example:
```json
{
    "api": {
        "base_url": "https://api.congress.gov/v3",
        "rate_limit": {
            "requests_per_second": 5,
            "max_retries": 3,
            "retry_delay": 1
        }
    }
}
```

### 2. DynamoDB Configuration

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| table_name | DynamoDB table name | congress-data-dev | Must be unique |
| region | AWS region | us-west-2 | Valid AWS region |

Example:
```json
{
    "dynamodb": {
        "table_name": "congress-data-dev",
        "region": "us-west-2",
        "endpoint": "http://localhost:8000"  # Optional, for local testing
    }
}
```

### 3. Logging Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| level | Log level | INFO | DEBUG, INFO, WARNING, ERROR |
| file | Log file path | logs/congress_downloader.log | Valid file path |
| max_size | Maximum log file size (bytes) | 10MB | >0 |
| backup_count | Number of backup files | 5 | ≥0 |

Example:
```json
{
    "logging": {
        "level": "DEBUG",
        "file": "logs/congress_downloader.log",
        "max_size": 10485760,
        "backup_count": 5,
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    }
}
```

### 4. Download Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| batch_size | Items per batch | 100 | 1-1000 |
| lookback_days | Default days for incremental mode | 30 | ≥1 |
| max_workers | Maximum parallel workers | 3 | 1-10 |
| chunk_size | Items per worker | 5 | 1-100 |

Example:
```json
{
    "download": {
        "batch_size": 100,
        "lookback_days": 30,
        "parallel": {
            "max_workers": 3,
            "chunk_size": 5
        }
    }
}
```

## Operating Modes

### 1. Incremental Mode
Updates recent data only:
```bash
python congress_downloader.py --mode incremental --lookback-days 7
```

### 2. Refresh Mode
Updates specific date range:
```bash
python congress_downloader.py --mode refresh --start-date 2024-01-01 --end-date 2024-01-31
```

### 3. Bulk Mode
Downloads historical data:
```bash
python congress_downloader.py --mode bulk
```

## Performance Tuning

### Low Resource Profile
```json
{
    "download": {
        "batch_size": 50,
        "parallel": {
            "max_workers": 2,
            "chunk_size": 3
        }
    },
    "api": {
        "rate_limit": {
            "requests_per_second": 3
        }
    }
}
```

### High Performance Profile
```json
{
    "download": {
        "batch_size": 200,
        "parallel": {
            "max_workers": 5,
            "chunk_size": 10
        }
    },
    "api": {
        "rate_limit": {
            "requests_per_second": 8
        }
    }
}
```

## AWS IAM Configuration

### Minimum Required Permissions
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

## Logging Configuration

### File-based Logging
```python
{
    "logging": {
        "level": "DEBUG",
        "file": "logs/congress_downloader.log",
        "max_size": 10485760,  # 10MB
        "backup_count": 5,
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    }
}
```

### CloudWatch Logging
```python
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

## Health Checking

Run the health check script to verify configuration:
```bash
python health_check.py
```

Example output:
```json
{
    "environment": {
        "status": "healthy",
        "missing_variables": []
    },
    "aws_credentials": {
        "status": "healthy"
    },
    "congress_api": {
        "status": "healthy"
    },
    "dynamodb": {
        "status": "healthy"
    }
}