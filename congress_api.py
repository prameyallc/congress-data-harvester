import os
import time
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

class CongressAPI:
    def __init__(self, config):
        self.base_url = config['base_url'].rstrip('/')  # Remove trailing slash if present
        self.rate_limit = config['rate_limit']
        # Get API key from environment variable first, fall back to config
        self.api_key = os.environ.get('CONGRESS_API_KEY', config.get('api_key'))
        if not self.api_key:
            raise ValueError("Congress.gov API key not found in environment or config")
        self.session = self._setup_session()
        self.last_request_time = 0
        self.logger = logging.getLogger('congress_downloader')

    def _setup_session(self):
        """Setup requests session with retry logic and authentication"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.rate_limit['max_retries'],
            backoff_factor=self.rate_limit['retry_delay'],
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _rate_limit_wait(self):
        """Implement rate limiting"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < 1.0 / self.rate_limit['requests_per_second']:
            time.sleep(1.0 / self.rate_limit['requests_per_second'] - time_since_last_request)

        self.last_request_time = time.time()

    def _make_request(self, endpoint, params=None):
        """Make API request with rate limiting and error handling"""
        self._rate_limit_wait()

        if params is None:
            params = {}

        # Add API key to parameters
        params['api_key'] = self.api_key

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self.logger.debug(f"Making request to: {url}")
        self.logger.debug(f"Request parameters: {params}")

        try:
            response = self.session.get(url, params=params)

            self.logger.debug(f"Response status code: {response.status_code}")
            if response.status_code != 200:
                self.logger.error(f"Error response content: {response.text}")
                self.logger.error(f"Full URL with params: {response.url}")

            if response.status_code == 403:
                raise ValueError("Invalid or expired API key")
            elif response.status_code == 404:
                raise ValueError(f"API endpoint not found: {endpoint}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                raise Exception("Rate limit exceeded")
            raise Exception(f"API request failed: {str(e)}")

        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {str(e)}")

    def get_earliest_date(self):
        """Get the earliest available date in the API"""
        try:
            response = self._make_request('bill/search', {
                'sort': 'updateDate',
                'fromDateTime': '2000-01-01T00:00:00Z',
                'offset': 0,
                'limit': 1,
                'format': 'json'
            })

            if response and 'bills' in response and response['bills']:
                return datetime(2000, 1, 1)  # Placeholder - we'll implement actual date parsing later
            return datetime(2000, 1, 1)  # Default fallback
        except Exception:
            # Fallback to a reasonable default if API call fails
            return datetime(2000, 1, 1)

    def get_data_for_date(self, date):
        """Get all data for a specific date"""
        try:
            # Format date for API
            date_str = date.strftime('%Y-%m-%d')

            # Use the bill/search endpoint with proper date filtering
            params = {
                'q': '',  # Empty query to get all bills
                'fromDateTime': f"{date_str}T00:00:00Z",
                'toDateTime': f"{date_str}T23:59:59Z",
                'sort': 'updateDate',
                'offset': 0,
                'limit': 250,  # Adjust based on API limits
                'format': 'json'
            }

            response_data = self._make_request('bill/search', params)

            # Process and return the data
            return response_data.get('bills', [])

        except Exception as e:
            raise Exception(f"Failed to get data for date {date_str}: {str(e)}")