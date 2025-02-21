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

### CloudWatch Permissions (Optional)
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        }
    ]
}
```

Note: CloudWatch permissions are optional. The application will continue to function without CloudWatch access, with metrics being logged locally instead.

## How to Contribute

### 1. Setting Up Development Environment

1. Fork and clone the repository
```bash
git clone https://github.com/yourusername/congress-downloader.git
cd congress-downloader
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Set up environment variables
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2
export CONGRESS_API_KEY=your_congress_api_key
```

### 2. Development Workflow

1. Create a feature branch
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes
3. Run tests
4. Submit a pull request

### 3. Code Style

This project follows PEP 8 style guide for Python code. Additional style guidelines:

- Use meaningful variable names
- Write descriptive docstrings
- Keep functions focused and small
- Add type hints where appropriate
- Include inline comments for complex logic

Example:
```python
def process_date_chunk(
    api_client: CongressAPI,
    db_handler: DynamoHandler,
    dates: List[datetime],
    logger: logging.Logger
) -> Tuple[int, List[Dict]]:
    """Process a chunk of dates in parallel.

    Args:
        api_client: Instance of CongressAPI
        db_handler: Instance of DynamoHandler
        dates: List of dates to process
        logger: Logger instance

    Returns:
        Tuple containing:
        - Number of successfully processed items
        - List of failed dates with error details
    """
    # Implementation
```

### 4. Testing

#### Running Tests
```bash
python -m pytest tests/
```

#### Writing Tests
- Place tests in the `tests/` directory
- Follow test file naming convention: `test_*.py`
- Use descriptive test names
- Include both positive and negative test cases

Example test:
```python
def test_process_date_chunk():
    # Arrange
    api_client = MockCongressAPI()
    db_handler = MockDynamoHandler()
    dates = [datetime(2024, 1, 1)]

    # Act
    total_items, failed_dates = process_date_chunk(
        api_client, db_handler, dates, mock_logger
    )

    # Assert
    assert total_items == 10
    assert len(failed_dates) == 0
```

### 5. Documentation

- Update relevant documentation for your changes
- Include docstrings for new functions and classes
- Add inline comments for complex logic
- Update README.md if needed

### 6. Commit Messages

Follow conventional commits specification:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- feat: New feature
- fix: Bug fix
- docs: Documentation
- style: Formatting
- refactor: Code restructuring
- test: Adding tests
- chore: Maintenance

Example:
```
feat(api): add rate limiting to Congress.gov API client

- Implement intelligent rate limiting
- Add exponential backoff
- Include jitter for request spacing

Closes #123
```

### 7. Pull Request Process

1. Create descriptive pull request title
2. Fill out the pull request template
3. Reference any related issues
4. Ensure all tests pass
5. Request review from maintainers

### 8. Issue Reporting

When creating an issue, include:

1. Clear description of the problem
2. Steps to reproduce
3. Expected vs actual behavior
4. Environment details
5. Relevant logs or screenshots

### 9. Development Best Practices

#### Error Handling
```python
try:
    data = api_client.get_data_for_date(date)
except APIError as e:
    logger.error(f"API error: {str(e)}")
    metrics.track_api_error(str(e))
    raise
```

#### Logging
```python
logger.info(f"Processing date: {date_str}")
logger.debug(f"API response: {response}")
logger.error(f"Failed to process: {error}")
```

#### Configuration
```python
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("config.json not found")
        raise
```

### 10. Release Process

1. Version Bumping
```bash
bumpversion patch  # or minor, major