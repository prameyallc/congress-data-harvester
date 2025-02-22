# Congress.gov API Documentation

This document details the integration with the Congress.gov API and provides usage guidelines for the Congress Data Downloader.

## Overview

The Congress Data Downloader uses the Congress.gov API to fetch and store legislative data. The implementation is primarily handled by the `CongressAPI` class in `congress_api.py`.

### Key Features
- Automatic rate limiting and backoff
- Comprehensive error handling
- Data validation and transformation
- Detailed logging and monitoring

## Authentication

### API Key Setup
1. Obtain an API key from [Congress.gov](https://api.congress.gov/)
2. Set the key using environment variables:
```bash
export CONGRESS_API_KEY=your_api_key
```

### Verification
The application verifies the API key on startup:
```python
if not self.api_key:
    raise ValueError("Congress.gov API key not found in environment or config")
```

## Rate Limiting

The API client implements intelligent rate limiting to prevent quota exhaustion:

```python
# Default configuration (config.json)
{
    "api": {
        "rate_limit": {
            "requests_per_second": 5,
            "max_retries": 3,
            "retry_delay": 1
        }
    }
}
```

### Features
- Default limit: 5 requests per second
- Exponential backoff on errors
- Random jitter to prevent thundering herd
- Respect for API retry-after headers

## Error Handling

### Common Error Scenarios

1. Rate Limit Exceeded (429)
```python
if response.status_code == 429:
    retry_after = response.headers.get('Retry-After', 60)
    time.sleep(float(retry_after))
    raise Exception("Rate limit exceeded")
```

2. Authentication Failed (403)
```python
if response.status_code == 403:
    raise Exception("API authentication failed - please verify API key")
```

3. Network Timeouts
```python
except requests.exceptions.Timeout:
    self.consecutive_errors += 1
    raise Exception("Request timed out")
```

## Data Models

### Committee Data
```python
{
    'id': 'committee-id',
    'type': 'committee',
    'congress': 119,
    'update_date': '2024-02-21',
    'version': 1,
    'name': 'Committee Name',
    'chamber': 'House',
    'committee_type': 'standing',
    'url': 'https://api.congress.gov/v3/committee/...'
}
```

### Hearing Data
```python
{
    'id': 'hearing-id',
    'type': 'hearing',
    'congress': 119,
    'update_date': '2024-02-21',
    'version': 1,
    'chamber': 'Senate',
    'committee': 'Committee Name',
    'title': 'Hearing Title',
    'date': '2024-02-21',
    'url': 'https://api.congress.gov/v3/hearing/...'
}
```

## API Endpoints

### Available Endpoints
- `/bill` - Bill information
- `/amendment` - Amendment details
- `/committee` - Committee information
- `/hearing` - Hearing schedules
- `/member` - Member profiles
- `/summaries` - Bill summaries

### Common Parameters
- `fromDateTime`: Start date (ISO format)
- `toDateTime`: End date (ISO format)
- `congress`: Congress number
- `chamber`: House/Senate filter
- `limit`: Results per page
- `offset`: Pagination offset

## Example Usage

### Initialize Client
```python
from congress_api import CongressAPI

api = CongressAPI({
    'base_url': 'https://api.congress.gov/v3',
    'rate_limit': {
        'requests_per_second': 5,
        'max_retries': 3,
        'retry_delay': 1
    }
})
```

### Fetch Committee Data
```python
# Get committee data for a specific date
committees = api.get_data_for_date(date=datetime(2024, 2, 21))

# Process and store committee data
for committee in committees:
    if committee['type'] == 'committee':
        store_committee(committee)
```

## Best Practices

### 1. Rate Limit Compliance
- Respect the rate limits in config.json
- Implement proper backoff strategies
- Monitor API quota usage

### 2. Error Handling
- Always check response status codes
- Implement proper retries with backoff
- Log errors comprehensively

### 3. Data Validation
- Validate all API responses
- Transform data consistently
- Handle missing fields gracefully

### 4. Monitoring
- Track API request success rates
- Monitor rate limit status
- Log response times and errors

## Troubleshooting

### Common Issues

1. Rate Limit Errors
```
Error: Rate limit exceeded
Solution: Reduce requests_per_second in config.json
```

2. Authentication Errors
```
Error: API authentication failed
Solution: Verify CONGRESS_API_KEY environment variable
```

3. Timeout Errors
```
Error: Request timed out
Solution: Check network connectivity and increase timeout settings
```

## Future Improvements

1. Caching Layer
- Implement response caching
- Add cache invalidation
- Support partial updates

2. Enhanced Validation
- Add schema versioning
- Implement data migrations
- Support custom validators

3. Monitoring
- Add detailed metrics
- Implement alerting
- Create dashboards