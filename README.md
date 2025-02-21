# Congress Data Downloader

A robust Python microservice for downloading and storing Congress.gov data in DynamoDB. This application provides efficient, scalable access to congressional data with configurable date ranges and multiple operation modes.

## Features

### Data Collection
- Automatic retrieval of congressional data from Congress.gov API
- Support for historical data back to March 4, 1789 (First Congress)
- Configurable date ranges for targeted data collection
- Rate-limited API requests to prevent throttling
- Intelligent retry mechanism with exponential backoff

### Operation Modes
1. **Incremental Download**
   - Retrieve recent updates (configurable lookback period)
   - Ideal for daily/weekly data synchronization
   ```bash
   python congress_downloader.py --mode incremental --lookback-days 7
   ```

2. **Refresh Mode**
   - Download data for specific date ranges
   - Perfect for backfilling or updating specific periods
   ```bash
   python congress_downloader.py --mode refresh --start-date 2024-01-01 --end-date 2024-01-31
   ```

3. **Bulk Download**
   - Retrieve all available historical data
   - Automatically handles pagination and rate limits
   ```bash
   python congress_downloader.py --mode bulk
   ```

### Data Storage
- Efficient DynamoDB storage with optimized schema
- Automatic table creation and management
- Conditional writes to prevent duplicate data
- Support for batch operations
- Secondary indices for efficient querying

### Performance Features
- Parallel processing for faster data retrieval
- Configurable batch sizes for optimal performance
- Automatic resource monitoring
- Graceful shutdown handling
- Progress tracking and detailed logging

### Monitoring & Reliability
- Comprehensive logging with rotation
- CloudWatch metrics integration (optional)
- Built-in health checks
- Resource usage monitoring
- Detailed error reporting and handling

## Quick Start

1. Set up environment variables:
```bash
# AWS Credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2

# Congress.gov API Key
export CONGRESS_API_KEY=your_congress_api_key
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run a test download:
```bash
python congress_downloader.py --mode incremental --lookback-days 1
```

## Usage

### 1. Download by Date Range

Download data for any specific time period:

```bash
# Last month
python congress_downloader.py --mode refresh --start-date 2024-01-01 --end-date 2024-01-31

# Specific congressional session
python congress_downloader.py --mode refresh --start-date 2023-01-03 --end-date 2024-01-03

# Historical data
python congress_downloader.py --mode refresh --start-date 1989-01-01 --end-date 1989-12-31
```

### 2. Incremental Download

Download recent updates (configurable lookback period):

```bash
# Last 24 hours
python congress_downloader.py --mode incremental --lookback-days 1

# Last week
python congress_downloader.py --mode incremental --lookback-days 7

# Last month
python congress_downloader.py --mode incremental --lookback-days 30
```

### 3. Bulk Download

Download all available data from the earliest date:

```bash
python congress_downloader.py --mode bulk
```

## Date Range Configuration

The date range functionality can be configured in `config.json`:

```json
{
    "download": {
        "date_ranges": {
            "max_range_days": 365,
            "min_date": "1789-03-04",
            "default_start_date": "2024-01-01",
            "default_end_date": "2024-12-31"
        }
    }
}
```

- `max_range_days`: Maximum number of days for a single download request
- `min_date`: Earliest allowed date (First Congress: March 4, 1789)
- `default_start_date`: Default start date if not specified
- `default_end_date`: Default end date if not specified

## Documentation

- [Configuration Guide](CONFIGURATION.md) - Detailed configuration options
- [API Documentation](API.md) - Congress.gov API integration details
- [Architecture Overview](ARCHITECTURE.md) - System design and components
- [Deployment Guide](DEPLOYMENT.md) - Deployment instructions for various platforms
- [Contributing Guidelines](CONTRIBUTING.md) - Development guidelines

## Troubleshooting

### Common Issues

1. **Date Range Too Large**
   ```
   Error: Date range exceeds maximum allowed (365 days)
   ```
   - Split your request into smaller date ranges
   - Adjust max_range_days in config.json
   - Use parallel processing for efficiency

2. **Invalid Date Format**
   ```
   Error: Invalid date format. Use YYYY-MM-DD
   ```
   - Ensure dates are in YYYY-MM-DD format
   - Check date validity
   - Verify dates are within allowed range

3. **Missing AWS Credentials**
   ```
   botocore.exceptions.NoCredentialsError: Unable to locate credentials
   ```
   - Set the required environment variables
   - Verify AWS credentials are valid
   - Check AWS region configuration

4. **Rate Limiting**
   ```
   Error: Rate limit exceeded
   ```
   - Reduce the number of concurrent requests
   - Implement exponential backoff for retries


## Requirements

- Python 3.11 or higher
- AWS account with DynamoDB access
- Congress.gov API key
- 1GB RAM minimum (2GB recommended)

## Support

For issues and feature requests, please refer to our [Contributing Guidelines](CONTRIBUTING.md).