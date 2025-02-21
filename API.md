# API Documentation

This document details the Congress.gov API integration and usage patterns implemented in the Congress Data Downloader.

## Congress.gov API Integration

### API Client Overview

The `CongressAPI` class in `congress_api.py` handles all interactions with the Congress.gov API. It implements:
- Automatic rate limiting
- Error handling and retries
- Response validation
- Data transformation

### Authentication

The API client requires a Congress.gov API key, which can be obtained from [Congress.gov](https://api.congress.gov/). The key can be provided through:
1. Environment variable: `CONGRESS_API_KEY`
2. Configuration file: `config.json` (not recommended)

### Rate Limiting

The client implements intelligent rate limiting:
- Default limit: 5 requests per second
- Automatic backoff on rate limit errors
- Jitter added to prevent thundering herd
- Exponential backoff on consecutive errors

### Error Handling

1. **Rate Limit Exceeded**
   ```python
   except RateLimitExceeded:
       time.sleep(self.retry_delay * (2 ** retry_count))
   ```

2. **Network Errors**
   ```python
   except requests.exceptions.RequestException:
       if retry_count < self.max_retries:
           time.sleep(self.retry_delay)
   ```

3. **API Errors**
   ```python
   if response.status_code >= 500:
       # Server error, retry
   elif response.status_code == 429:
       # Rate limit hit, backoff
   ```

### Data Models

1. **Bill Data**
   ```python
   {
       'id': 'bill-id',
       'congress': 117,
       'title': 'Bill Title',
       'bill_type': 'hr',
       'bill_number': 1234,
       'introduced_date': '2024-01-01',
       'update_date': '2024-01-02',
       'latest_action': {
           'text': 'Action description',
           'action_date': '2024-01-02'
       }
   }
   ```

2. **Sponsor Data**
   ```python
   {
       'bioguideId': 'sponsor-id',
       'firstName': 'First',
       'lastName': 'Last',
       'party': 'Party',
       'state': 'ST'
   }
   ```

### Example Usage

1. **Initialize Client**
   ```python
   from congress_api import CongressAPI

   api = CongressAPI(config['api'])
   ```

2. **Fetch Bills by Date**
   ```python
   bills = api.get_bills_for_date('2024-01-01')
   ```

3. **Get Bill Details**
   ```python
   bill = api.get_bill_details('117-hr-1234')
   ```

### Response Validation

The API client validates responses using the `DataValidator` class:

```python
validator = DataValidator()
is_valid, errors = validator.validate_bill(bill_data)
if is_valid:
    cleaned_data = validator.cleanup_bill(bill_data)
```

### Pagination Handling

The client automatically handles pagination for large result sets:

```python
def get_all_bills_for_date(self, date):
    offset = 0
    while True:
        bills = self.get_bills_for_date(date, offset)
        if not bills:
            break
        yield from bills
        offset += len(bills)
```

### Error Response Examples

1. **Rate Limit Error**
   ```json
   {
       "error": "TOO_MANY_REQUESTS",
       "message": "Rate limit exceeded",
       "retryAfter": 60
   }
   ```

2. **Invalid API Key**
   ```json
   {
       "error": "INVALID_API_KEY",
       "message": "The API key provided is invalid"
   }
   ```

### Best Practices

1. **Rate Limit Compliance**
   - Respect the rate limits
   - Implement backoff strategies
   - Use parallel processing wisely

2. **Error Handling**
   - Always check response status
   - Implement proper retries
   - Log errors comprehensively

3. **Data Management**
   - Validate all responses
   - Clean and normalize data
   - Handle missing fields gracefully