# Congress Data Downloader

A Python microservice for downloading and storing Congress.gov data in DynamoDB.

## Overview

This project provides a robust system for fetching, processing, and storing legislative data from the official Congress.gov API. It handles rate limiting, data validation, and efficient storage with a focus on performance and reliability.

## Features

- **Complete Data Coverage**: Access all 18 Congress.gov API endpoints
- **Smart Rate Limiting**: Intelligent rate limiting with exponential backoff
- **Robust Error Handling**: Comprehensive error detection and recovery
- **Efficient Storage**: Optimized DynamoDB storage with deduplication
- **Comprehensive API**: REST API for querying stored congressional data
- **Flexible Export Options**: Export data to JSON or CSV formats
- **Detailed Monitoring**: Performance metrics and health reporting
- **Swagger Documentation**: Interactive API documentation

## Quick Start

### Prerequisites

- Python 3.8+
- AWS account with DynamoDB access
- Congress.gov API key

### Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/congress-downloader.git
cd congress-downloader
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2
export CONGRESS_API_KEY=your_congress_api_key
```

4. Run the health check:
```bash
python health_check.py
```

### Basic Usage

1. Download data for a specific date range:
```bash
python congress_downloader.py --mode refresh --start-date 2024-01-01 --end-date 2024-01-07
```

2. Export bills data to JSON format:
```bash
python congress_downloader.py --mode export --data-type bill --format json
```

3. Export committee data to CSV format:
```bash
python congress_downloader.py --mode export --data-type committee --format csv
```

4. Start the API server:
```bash
python api_server.py
```

5. Access the API documentation:
```
http://localhost:5000/swagger/
```

6. Export data via API:
```
GET http://localhost:5000/api/export?format=json&data_type=bill&congress=117
```

## Documentation

- [API Documentation](API_DOCS.md) - Detailed API documentation
- [Architecture](ARCHITECTURE.md) - System architecture and design
- [Configuration](CONFIGURATION.md) - Configuration options
- [Export Documentation](EXPORT_DOCUMENTATION.md) - Data export functionality
- [Deployment](DEPLOYMENT.md) - Deployment guides
- [Contributing](CONTRIBUTING.md) - How to contribute
- [Roadmap](ROADMAP.md) - Future development plans

## CLI Options

```
usage: congress_downloader.py [-h] --mode {bulk,incremental,refresh,export}
                             [--start-date START_DATE] [--end-date END_DATE]
                             [--lookback-days LOOKBACK_DAYS] [--parallel-workers PARALLEL_WORKERS]
                             [--verbose] [--format {json,csv}] [--data-type {bill,committee,hearing,amendment,nomination,treaty}]
                             [--congress CONGRESS] [--output OUTPUT]

Congress.gov Data Downloader

optional arguments:
  -h, --help            show this help message and exit
  --mode {bulk,incremental,refresh,export}
                        Download mode or export data
  --start-date START_DATE
                        Start date (YYYY-MM-DD)
  --end-date END_DATE   End date (YYYY-MM-DD)
  --lookback-days LOOKBACK_DAYS
                        Days to look back for incremental update
  --parallel-workers PARALLEL_WORKERS
                        Number of parallel workers for processing
  --verbose             Enable verbose logging
  --format {json,csv}   Export format (for export mode)
  --data-type {bill,committee,hearing,amendment,nomination,treaty}
                        Type of data to export (for export mode)
  --congress CONGRESS   Congress number (for export mode, e.g., 117)
  --output OUTPUT       Output file path (for export mode)
```

## Community

- [Issue Tracker](https://github.com/your-username/congress-downloader/issues)
- [Discussions](https://github.com/your-username/congress-downloader/discussions)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.