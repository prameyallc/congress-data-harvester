# Contributing Guidelines

This document outlines the process for contributing to the Congress Data Downloader project.

## Quick Start

1. **Fork and Clone**
```bash
git clone https://github.com/yourusername/congress-downloader.git
cd congress-downloader
```

2. **Set Up Environment**
```bash
# Install dependencies
pip install -r requirements.txt

# Set required environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2
export CONGRESS_API_KEY=your_congress_api_key
```

3. **Verify Setup**
```bash
python health_check.py
```

## Development Workflow

### 1. Creating Features

1. Create a feature branch:
```bash
git checkout -b feature/your-feature-name
```

2. Run tests before making changes:
```bash
python -m pytest tests/
```

3. Implement your changes, following our code style guidelines

4. Run tests again:
```bash
python -m pytest tests/
```

### 2. Code Style

We follow PEP 8 with these additional guidelines:

1. **Naming Conventions**
```python
# Classes use PascalCase
class DataValidator:

# Functions and variables use snake_case
def process_committee_data(committee_info):
    total_members = calculate_total_members()
```

2. **Type Hints**
```python
from typing import Dict, List, Optional

def get_committee_members(
    committee_id: str,
    congress: int
) -> List[Dict[str, str]]:
    """Get committee members."""
    pass
```

3. **Docstrings**
```python
def analyze_committee_data(
    committee: Dict[str, any]
) -> Dict[str, any]:
    """Analyze committee data and extract key information.

    Args:
        committee: Raw committee data dictionary

    Returns:
        Dictionary containing analyzed committee information

    Raises:
        ValueError: If required fields are missing
    """
    pass
```

### 3. Testing

#### Writing Tests

1. **Test File Structure**
```python
# test_committee_data.py

def test_process_committee_data():
    # Arrange
    test_data = {
        'name': 'Test Committee',
        'type': 'standing'
    }

    # Act
    result = process_committee_data(test_data)

    # Assert
    assert result['name'] == 'Test Committee'
    assert result['type'] == 'standing'
```

2. **Test Categories**
- Unit tests: `tests/unit/`
- Integration tests: `tests/integration/`
- API tests: `tests/api/`

3. **Mock External Services**
```python
from unittest.mock import patch

@patch('congress_api.CongressAPI._make_request')
def test_api_rate_limiting(mock_request):
    mock_request.return_value = {'data': 'test'}
    api = CongressAPI(config)
    result = api.get_committee_data()
```

### 4. Documentation

#### Documentation Files

1. **Code Documentation**
- Add docstrings to all public functions
- Include type hints
- Document exceptions

2. **API Documentation**
- Update API.md for endpoint changes
- Document request/response formats
- Include example usage

3. **Configuration Documentation**
- Update CONFIGURATION.md for new options
- Document environment variables
- Include example configurations

### 5. Pull Request Process

1. **Checklist**
- [ ] Tests pass
- [ ] Documentation updated
- [ ] Code follows style guide
- [ ] Commit messages are descriptive

2. **PR Template**
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Performance improvement

## Testing
Describe testing performed

## Documentation
List documentation updates made
```

### 6. Commit Messages

Follow conventional commits specification:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance

Examples:
```
feat(api): add rate limiting to Congress.gov API client

- Implement intelligent rate limiting
- Add exponential backoff
- Include jitter for request spacing

Closes #123
```

### 7. Issue Guidelines

When creating issues:

1. **Bug Reports**
```markdown
## Bug Description
Clear description of the problem

## Steps to Reproduce
1. Step one
2. Step two
3. Step three

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Environment
- Python version
- OS
- Relevant configuration
```

2. **Feature Requests**
```markdown
## Feature Description
Clear description of proposed feature

## Use Case
Why this feature is needed

## Proposed Solution
How you think it should work

## Alternatives Considered
Other approaches considered
```

### 8. Development Best Practices

1. **Error Handling**
```python
try:
    data = api_client.get_data_for_date(date)
except APIError as e:
    logger.error(f"API error: {str(e)}")
    metrics.track_api_error(str(e))
    raise
```

2. **Logging**
```python
logger.info(f"Processing date: {date_str}")
logger.debug(f"API response: {response}")
logger.error(f"Failed to process: {error}")
```

3. **Configuration**
```python
def load_config():
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("config.json not found")
        raise
```

### 9. Release Process

1. **Version Bumping**
```bash
bumpversion patch  # or minor, major
```

2. **Changelog Updates**
- Document all changes
- Note breaking changes
- Include upgrade steps

3. **Testing Checklist**
- [ ] All tests pass
- [ ] Integration tests pass
- [ ] Documentation accurate
- [ ] Release notes complete