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
            "max_retries": 5,
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
| max_retries | Maximum retry attempts | 5 | 1-10 |
| retry_delay | Base delay between retries (seconds) | 1 | 1-60 |
| endpoint_rate_limits | Per-endpoint rate limits | See below | Dict of endpoint:limit |

#### Endpoint-specific Rate Limits

The system supports endpoint-specific rate limits to adapt to different API endpoints:

```json
{
    "api": {
        "endpoint_rate_limits": {
            "bill": 3.0,
            "amendment": 3.0,
            "nomination": 3.0,
            "treaty": 4.0,
            "committee": 4.0,
            "default": 5.0
        }
    }
}
```

#### Timeout Configuration

Configure endpoint-specific timeouts (connect, read) in seconds:

```json
{
    "api": {
        "timeout_config": {
            "bill": [8, 45],
            "amendment": [8, 45],
            "committee": [5, 30],
            "default": [5, 30]
        }
    }
}
```

Example:
```json
{
    "api": {
        "base_url": "https://api.congress.gov/v3",
        "rate_limit": {
            "requests_per_second": 5,
            "max_retries": 5,
            "retry_delay": 1
        },
        "endpoint_rate_limits": {
            "bill": 3.0,
            "amendment": 3.0,
            "default": 5.0
        },
        "timeout_config": {
            "bill": [8, 45],
            "default": [5, 30]
        }
    }
}
```

### 2. DynamoDB Configuration

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| table_name | DynamoDB table name | congress-data-dev | Must be unique |
| region | AWS region | us-west-2 | Valid AWS region |
| deduplication | Deduplication settings | See below | Configuration for deduplication |

#### Deduplication Settings

```json
{
    "dynamodb": {
        "deduplication": {
            "enabled": true,
            "reset_frequency": "per_date",
            "memory_threshold_mb": 512
        }
    }
}
```

Available reset_frequency options:
- "per_date": Reset tracked IDs for each date processed
- "per_range": Reset only at the beginning of a date range
- "per_session": Reset only at the beginning of a download session

Example:
```json
{
    "dynamodb": {
        "table_name": "congress-data-dev",
        "region": "us-west-2",
        "endpoint": "http://localhost:8000",  # Optional, for local testing
        "deduplication": {
            "enabled": true,
            "reset_frequency": "per_date"
        }
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
| include_metrics | Include metrics in logs | true | true/false |
| metrics_format | Human-readable or JSON | human | human, json |

Example:
```json
{
    "logging": {
        "level": "DEBUG",
        "file": "logs/congress_downloader.log",
        "max_size": 10485760,
        "backup_count": 5,
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "include_metrics": true,
        "metrics_format": "human"
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
| memory_limit_mb | Memory threshold for scaling | 1024 | ≥256 |
| cpu_threshold | CPU usage threshold (%) | 80 | 1-99 |

Example:
```json
{
    "download": {
        "batch_size": 100,
        "lookback_days": 30,
        "parallel": {
            "max_workers": 3,
            "chunk_size": 5,
            "memory_limit_mb": 1024,
            "cpu_threshold": 80
        }
    }
}
```

### 5. Metrics Configuration

| Parameter | Description | Default | Valid Values |
|-----------|-------------|---------|--------------|
| enable_cloudwatch | Send metrics to CloudWatch | false | true/false |
| namespace | CloudWatch namespace | CongressDownloader | String |
| buffer_size | Metrics buffer size | 20 | ≥1 |
| flush_interval | Seconds between flushes | 60 | ≥10 |
| detailed_reporting | Generate detailed reports | true | true/false |

Example:
```json
{
    "metrics": {
        "enable_cloudwatch": true,
        "namespace": "CongressDownloader-Prod",
        "buffer_size": 50,
        "flush_interval": 30,
        "detailed_reporting": true
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
            "chunk_size": 3,
            "memory_limit_mb": 512
        }
    },
    "api": {
        "rate_limit": {
            "requests_per_second": 3
        },
        "endpoint_rate_limits": {
            "bill": 2.0,
            "amendment": 2.0,
            "default": 3.0
        }
    },
    "dynamodb": {
        "deduplication": {
            "reset_frequency": "per_range"
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
            "chunk_size": 10,
            "memory_limit_mb": 2048
        }
    },
    "api": {
        "rate_limit": {
            "requests_per_second": 8,
            "max_retries": 8,
            "retry_delay": 0.5
        },
        "endpoint_rate_limits": {
            "bill": 5.0,
            "amendment": 5.0,
            "default": 8.0
        },
        "timeout_config": {
            "bill": [10, 60],
            "amendment": [10, 60],
            "default": [8, 45]
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
    "congress_api_endpoints": {
        "status": "healthy",
        "available_endpoints": {
            "bill": "available",
            "amendment": "available",
            "nomination": "available",
            "treaty": "available",
            "committee": "available",
            "hearing": "available",
            "committee-report": "available",
            "congressional-record": "available",
            "house-communication": "available",
            "house-requirement": "available",
            "senate-communication": "available",
            "member": "available",
            "summaries": "available",
            "committee-print": "available",
            "committee-meeting": "available",
            "daily-congressional-record": "available",
            "bound-congressional-record": "available",
            "congress": "available"
        },
        "endpoint_count": 18
    },
    "dynamodb": {
        "status": "healthy"
    }
}
```

## Deduplication Mechanism

The Congress Data Downloader includes an advanced deduplication system to prevent duplicate items from being stored in DynamoDB. This feature:

1. Tracks processed item IDs in memory
2. Automatically skips duplicate items before batch operations
3. Resets tracking at configurable intervals to maintain memory efficiency

### Configuration Options

```json
{
    "dynamodb": {
        "deduplication": {
            "enabled": true,
            "reset_frequency": "per_date",
            "memory_threshold_mb": 512,
            "tracking_stats": true
        }
    }
}
```

### Metrics and Reporting

The deduplication system records detailed metrics about skipped duplicates:

- Per-endpoint duplicate counts
- Memory usage of ID tracking set
- Performance impact measurements

These metrics are included in the detailed ingestion report that's generated at the end of each processing session.