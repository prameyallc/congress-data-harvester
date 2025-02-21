def _rate_limit_wait(self):
    current_time = time.time()
    time_since_last_request = current_time - self.last_request_time
    base_wait = 1.0 / self.rate_limit['requests_per_second']
    jitter = uniform(-0.1 * base_wait, 0.1 * base_wait)
    wait_time = base_wait + jitter
```

### Error Handling

The API client handles various error scenarios:
- Rate limit exceeded (429)
- Authentication failures (403)
- Not found errors (404)
- Server errors (500, 502, 503, 504)

Each error type has specific handling logic:
```python
if response.status_code == 429:
    retry_after = response.headers.get('Retry-After', 60)
    time.sleep(float(retry_after))
if response.status_code == 403:
    raise Exception("API authentication failed")
```

### CloudWatch Metrics Error Handling

The metrics collection system is designed to be fault-tolerant:

1. **Missing CloudWatch Permissions**
```python
try:
    metrics.track_api_request(endpoint, status_code, duration)
except ClientError as e:
    if e.response['Error']['Code'] == 'AccessDenied':
        logger.warning("CloudWatch metrics disabled due to permissions")
        # Continue processing without metrics
```

2. **CloudWatch Service Unavailable**
```python
def _put_metric(self, name, value, unit, dimensions=None):
    if not self.cloudwatch:
        self.logger.warning(f"CloudWatch metrics disabled - skipping metric: {name}")
        return
```

## Data Retrieval

### Available Endpoints

The client currently supports these Congress.gov API endpoints:

1. `/bill` - Retrieve bill information
2. `/congress/current` - Get current congress details

### Bill Data Retrieval

#### Get Data for Date Range
```python
api_client = CongressAPI(config['api'])
bills = api_client.get_data_for_date(date)
```

Response format:
```json
{
    "id": "117-hr-1",
    "congress": 117,
    "title": "Bill Title",
    "update_date": "2024-02-21",
    "bill_type": "hr",
    "bill_number": "1",
    "version": 1,
    "origin_chamber": "House",
    "latest_action": {
        "text": "Action description",
        "action_date": "2024-02-21"
    }
}
```

### Data Transformation

The API client transforms raw Congress.gov data into a standardized format:

1. ID Generation
```python
def _generate_bill_id(self, bill):
    congress = str(bill.get('congress', ''))
    bill_type = bill.get('type', '').lower()
    bill_number = str(bill.get('number', ''))
    return f"{congress}-{bill_type}-{bill_number}"
```

2. Data Normalization
```python
transformed_bill = {
    'id': bill_id,
    'congress': bill.get('congress', current_congress),
    'title': bill.get('title', ''),
    'update_date': bill.get('updateDate', date_str),
    # ... additional fields
}
```

## Data Validation

### Validation Process

The `DataValidator` class performs multiple validation steps:

1. Schema Validation
```python
def validate_bill(self, bill):
    required_fields = ['id', 'congress', 'title', 'update_date']
    for field in required_fields:
        if not bill.get(field):
            return False, f"Missing required field: {field}"
```

2. Data Cleanup
```python
def cleanup_bill(self, bill):
    # Remove null values
    return {k: v for k, v in bill.items() if v is not None}
```

### Error Handling

Validation errors are tracked and reported:
```python
if not is_valid:
    validation_errors[bill_id] = errors
    logger.warning(f"Bill {bill_id} failed validation: {errors}")
```

## Usage Examples

### Basic Usage

1. Initialize API Client
```python
config = load_config()
api_client = CongressAPI(config['api'])
```

2. Get Current Congress
```python
current_congress = api_client.get_current_congress()
```

3. Download Bills for Date Range
```python
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)
for date in date_range(start_date, end_date):
    bills = api_client.get_data_for_date(date)
```

### Advanced Usage

1. Parallel Processing
```python
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    future_to_chunk = {
        executor.submit(process_date_chunk, api_client, db_handler, chunk): chunk 
        for chunk in date_chunks
    }
```

2. Error Recovery
```python
retry_count = 0
while retry_count < max_retries:
    try:
        data = api_client.get_data_for_date(date)
        break
    except Exception:
        retry_count += 1
        wait_time = 2 ** retry_count
        time.sleep(wait_time)