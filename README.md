# Congress Data Downloader

A robust Python microservice for downloading and storing Congress.gov data in DynamoDB. This application provides efficient, scalable access to congressional data with configurable date ranges and multiple operation modes.

## Features

### Data Collection
- **Automated Data Retrieval**: Seamless integration with Congress.gov API
- **Historical Coverage**: Access data back to March 4, 1789 (First Congress)
- **Configurable Date Ranges**: Flexible data collection periods
- **Intelligent Rate Limiting**: 
  - Automatic request throttling and backoff
  - Endpoint-specific rate limits
  - Dynamic adjustment based on API response
  - Exponential backoff with jitter
- **Parallel Processing**: 
  - Multi-threaded data collection
  - Configurable worker pool size
  - Automatic workload distribution
  - Resource-aware scheduling
  - Memory and CPU usage monitoring for dynamic worker scaling
- **Error Recovery**: 
  - Automatic retries with exponential backoff
  - Failed item tracking and reporting
  - Transaction integrity protection
  - State recovery mechanisms
  - Improved error logging and reporting for better troubleshooting
- **Data Deduplication**:
  - Automatic detection and skipping of duplicate items
  - Memory-efficient tracking of processed IDs
  - Prevents DynamoDB duplicate key errors
  - Detailed duplicate metrics and reporting

### Storage & Performance
- **DynamoDB Integration**: 
  - On-demand capacity for cost-effective scaling
  - No provisioned capacity requirements
  - Automatic table creation and management
  - Optimized batch operations
  - Secondary indices for efficient querying
  - Conditional writes to prevent duplicates
  - Item deduplication to prevent errors

### Operation Modes
1. **Incremental Download**
   - Retrieve recent updates only
   - Configurable lookback period
   - Ideal for daily/weekly synchronization
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
   - Automatic pagination handling
   - Resource-aware processing
   ```bash
   python congress_downloader.py --mode bulk
   ```

### Monitoring & Reliability
- **Comprehensive Logging**:
  - Detailed operation tracking
  - Automatic log rotation
  - Configurable log levels
  - Transaction logging
  - Performance metrics
  - Endpoint-specific success/failure rates
- **Health Checks**:
  - API connectivity verification
  - DynamoDB access testing
  - Environment validation
  - Component status monitoring
  - Detailed endpoint health reporting
- **Resource Monitoring**:
  - Memory usage tracking
  - CPU utilization monitoring
  - Network throughput analysis
  - DynamoDB capacity tracking
- **Error Handling**:
  - Graceful failure recovery
  - Detailed error reporting
  - Automatic retry mechanisms
  - Data consistency verification
  - Enhanced timeout handling
- **Advanced Metrics**:
  - Per-endpoint statistics
  - Success/failure rates
  - Duplicate detection counts
  - Ingestion reporting
  - API latency tracking

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

## Configuration

The application uses a JSON configuration file (`config.json`) for customization:

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
        "level": "DEBUG",
        "file": "logs/congress_downloader.log",
        "max_size": 10485760,
        "backup_count": 5,
        "include_performance_metrics": true
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
            "chunk_size": 5,
            "memory_limit_mb": 1024,
            "cpu_threshold": 80
        }
    }
}
```

### Key Configuration Options

1. **API Settings**
   - `requests_per_second`: Control rate limiting (default: 5)
   - `max_retries`: Maximum retry attempts for failed requests (default: 5)
   - `retry_delay`: Base delay between retries in seconds

2. **DynamoDB Configuration**
   - Uses on-demand capacity for automatic scaling
   - No need to specify read/write capacity units
   - `table_name`: Your DynamoDB table name
   - `region`: AWS region for DynamoDB operations

3. **Download Settings**
   - `batch_size`: Number of items per batch operation
   - `default_lookback_days`: Default period for incremental updates
   - `max_workers`: Number of parallel download threads
   - `chunk_size`: Items processed per worker
   - `memory_limit_mb`: Memory threshold for worker scaling
   - `cpu_threshold`: CPU usage threshold percentage

4. **Logging Options**
   - `level`: Log detail level (DEBUG, INFO, WARNING, ERROR)
   - `max_size`: Maximum log file size in bytes
   - `backup_count`: Number of backup log files to keep
   - `include_performance_metrics`: Enable detailed performance logging

## Advanced Usage

### Command Line Options

```bash
# Enable verbose logging
python congress_downloader.py --mode refresh --start-date 2024-01-01 --end-date 2024-01-31 --verbose

# Specify parallel workers (overrides config file)
python congress_downloader.py --mode bulk --parallel-workers 5

# Quick incremental update with custom lookback period
python congress_downloader.py --mode incremental --lookback-days 3
```

### Deduplication System

The application includes automatic deduplication of items to prevent errors when writing to DynamoDB:

- Tracks processed item IDs in memory
- Automatically skips duplicate items before batch operations
- Resets tracking between processing sessions to maintain memory efficiency
- Reports detailed metrics on duplicates detected and skipped

### Metrics and Reporting

The application provides comprehensive metrics for monitoring and analysis:

- **API Metrics Report**: Details on API requests, success rates, and latency
- **Ingestion Report**: Information on items processed, stored, and duplicates skipped
- **Health Check**: Comprehensive verification of API endpoints and system components

View detailed metrics with:

```bash
# Run with verbose mode to see detailed metrics in logs
python congress_downloader.py --mode incremental --lookback-days 1 --verbose

# Run dedicated health check
python health_check.py
```

## Documentation

For detailed information about each component:
- [Configuration Guide](CONFIGURATION.md) - Detailed configuration options
- [API Documentation](API.md) - Congress.gov API integration details
- [Architecture Overview](ARCHITECTURE.md) - System design and components
- [Deployment Guide](DEPLOYMENT.md) - Deployment instructions for various platforms

## Requirements

- Python 3.11 or higher
- AWS account with DynamoDB access
- Congress.gov API key
- 1GB RAM minimum (2GB recommended)

## Support

For issues and feature requests, please refer to our [Contributing Guidelines](CONTRIBUTING.md).