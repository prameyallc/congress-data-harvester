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