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
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
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
            wait_time = 1.0 / self.rate_limit['requests_per_second'] - time_since_last_request
            self.logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)

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

            # Log response status and headers for debugging
            self.logger.debug(f"Response status code: {response.status_code}")
            self.logger.debug(f"Response headers: {dict(response.headers)}")

            if response.status_code != 200:
                self.logger.error(f"Error response content: {response.text}")
                self.logger.error(f"Full URL with params: {response.url}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                self.logger.error("Rate limit exceeded. Consider adjusting rate limiting parameters.")
                raise Exception("Rate limit exceeded")
            if e.response.status_code == 403:
                self.logger.error("API key authentication failed.")
                raise Exception("API authentication failed")
            if e.response.status_code == 404:
                self.logger.error(f"API endpoint not found: {url}")
                raise Exception(f"API endpoint not found: {url}")
            raise Exception(f"API request failed: {str(e)}")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {str(e)}")
            raise Exception(f"Request failed: {str(e)}")

    def get_earliest_date(self):
        """Get the earliest available date in the API"""
        try:
            # Get list of bills from the first available congress
            response = self._make_request('congress/current', {
                'format': 'json'
            })

            # Get the current congress number
            current_congress = response.get('congress', {}).get('number', 118)

            # Calculate approximate start date based on congress number
            # Each congress is 2 years, starting from 1789
            years_since_start = (current_congress - 1) * 2
            start_year = 1789 + years_since_start

            return datetime(start_year, 1, 1)

        except Exception as e:
            self.logger.error(f"Failed to get earliest date: {str(e)}")
            return datetime(2000, 1, 1)  # Fallback to a reasonable default

    def _generate_bill_id(self, bill):
        """Generate a unique bill ID from available fields"""
        try:
            congress = str(bill.get('congress', ''))
            bill_type = bill.get('billType', '').lower()
            bill_number = str(bill.get('billNumber', ''))
            if congress and bill_type and bill_number:
                return f"{congress}-{bill_type}-{bill_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate bill ID: {str(e)}")
        return None

    def get_data_for_date(self, date):
        """Get all data for a specific date"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            current_congress = self._get_current_congress()

            all_bills = []
            offset = 0
            limit = 20  # API default limit

            while True:
                # Query the bill endpoint with proper parameters
                params = {
                    'offset': offset,
                    'limit': limit,
                    'format': 'json',
                    'sort': 'updateDate+desc',
                    'fromDateTime': f"{date_str}T00:00:00Z",
                    'toDateTime': f"{date_str}T23:59:59Z"
                }

                self.logger.info(f"Fetching bills for date {date_str} (offset: {offset})")
                response_data = self._make_request('bill', params)
                bills = response_data.get('bills', [])

                if not bills:
                    break

                # Log sample of raw bill data for debugging
                if offset == 0:
                    self.logger.debug(f"Sample raw bill data: {bills[0] if bills else 'No bills found'}")

                # Transform bills for DynamoDB storage
                for bill in bills:
                    try:
                        # Generate ID if not present
                        bill_id = bill.get('billId') or self._generate_bill_id(bill)
                        if not bill_id:
                            self.logger.warning(f"Unable to generate ID for bill: {bill}")
                            continue

                        # Extract chamber info
                        origin_chamber_info = bill.get('originChamber', {})
                        origin_chamber = (origin_chamber_info.get('name', '') 
                                       if isinstance(origin_chamber_info, dict) 
                                       else str(origin_chamber_info))

                        transformed_bill = {
                            'id': bill_id,
                            'congress': bill.get('congress', current_congress),
                            'title': bill.get('title', ''),
                            'update_date': bill.get('updateDate', date_str),
                            'bill_type': bill.get('billType', ''),
                            'bill_number': bill.get('billNumber', ''),
                            'version': 1,
                            # Additional metadata
                            'origin_chamber': origin_chamber,
                            'latest_action': bill.get('latestAction', {}),
                            'update_date_including_text': bill.get('updateDateIncludingText', ''),
                            'introduced_date': bill.get('introducedDate', ''),
                            'sponsors': bill.get('sponsors', []),
                            'committees': bill.get('committees', [])
                        }

                        # Ensure critical fields are present
                        if not transformed_bill['congress'] or not transformed_bill['bill_type']:
                            self.logger.warning(f"Missing critical fields for bill ID {bill_id}")
                            continue

                        all_bills.append(transformed_bill)

                    except Exception as e:
                        self.logger.error(f"Failed to transform bill {bill.get('billId', 'unknown')}: {str(e)}")
                        continue

                self.logger.debug(f"Processed {len(bills)} bills from current batch")

                # Check if there are more results
                if len(bills) < limit:
                    break

                offset += limit

            self.logger.info(f"Retrieved {len(all_bills)} bills for date {date_str}")
            return all_bills

        except Exception as e:
            self.logger.error(f"Failed to get data for date {date}: {str(e)}")
            raise

    def _get_current_congress(self):
        """Get the current Congress number"""
        try:
            response = self._make_request('congress/current', {
                'format': 'json'
            })
            return response.get('congress', {}).get('number', 118)  # Default to 118th if not found
        except Exception as e:
            self.logger.error(f"Failed to get current congress: {str(e)}")
            return 118  # Default to 118th Congress if API call fails