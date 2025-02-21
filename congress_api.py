import os
import time
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from random import uniform
from monitoring import metrics
from data_validator import DataValidator
from typing import Dict, List, Any, Optional

class CongressBaseAPI:
    """Base class for Congress.gov API interactions"""

    def __init__(self, config):
        self.base_url = config['base_url'].rstrip('/')
        self.rate_limit = config['rate_limit']
        self.api_key = os.environ.get('CONGRESS_API_KEY', config.get('api_key'))
        if not self.api_key:
            raise ValueError("Congress.gov API key not found in environment or config")
        self.session = self._setup_session()
        self.last_request_time = 0
        self.consecutive_errors = 0
        self.logger = logging.getLogger('congress_downloader')

    def _setup_session(self):
        """Set up requests session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.rate_limit['max_retries'],
            backoff_factor=self.rate_limit['retry_delay'],
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True,
            raise_on_status=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _rate_limit_wait(self):
        """Implement rate limiting with jitter"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        base_wait = 1.0 / self.rate_limit['requests_per_second']
        jitter = uniform(-0.1 * base_wait, 0.1 * base_wait)
        wait_time = base_wait + jitter
        if self.consecutive_errors > 0:
            backoff_multiplier = min(2 ** self.consecutive_errors, 60)
            wait_time *= backoff_multiplier
            self.logger.warning(f"Rate limit backoff: waiting {wait_time:.2f} seconds after {self.consecutive_errors} consecutive errors")
        if time_since_last_request < wait_time:
            sleep_time = wait_time - time_since_last_request
            self.logger.debug(f"Rate limiting: waiting {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make API request with rate limiting and error handling"""
        self._rate_limit_wait()
        if params is None:
            params = {}
        params['api_key'] = self.api_key
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        start_time = time.time()

        try:
            response = self.session.get(url, params=params)
            duration = time.time() - start_time
            metrics.track_api_request(
                endpoint=endpoint,
                status_code=response.status_code,
                duration=duration
            )

            self.logger.debug(f"Request to: {url}")
            self.logger.debug(f"Response status: {response.status_code}")

            if response.status_code == 200:
                self.consecutive_errors = 0
                return response.json()

            if response.status_code == 429:
                self.consecutive_errors += 1
                retry_after = response.headers.get('Retry-After', 60)
                self.logger.error(f"Rate limit exceeded. Retry after {retry_after} seconds")
                time.sleep(float(retry_after))
                raise Exception("Rate limit exceeded")

            if response.status_code == 403:
                self.logger.error("API authentication failed")
                raise Exception("API authentication failed")

            if response.status_code == 404:
                self.logger.error(f"API endpoint not found: {url}")
                raise Exception(f"API endpoint not found: {url}")

            self.logger.error(f"Unexpected status code {response.status_code}: {response.text}")
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            duration = time.time() - start_time
            metrics.track_api_request(
                endpoint=endpoint,
                status_code=500,
                duration=duration
            )
            self.consecutive_errors += 1
            self.logger.error(f"Request failed: {str(e)}")
            raise Exception(f"Request failed: {str(e)}")

    def get_current_congress(self) -> int:
        """Get the current Congress number"""
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = self._make_request('congress/current', {'format': 'json'})
                return response.get('congress', {}).get('number', 118)
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    self.logger.error(f"Failed to get current congress after {max_retries} attempts")
                    return 118
                wait_time = 2 ** retry_count
                self.logger.warning(f"Retrying current congress lookup after {wait_time} seconds")
                time.sleep(wait_time)

    def get_available_endpoints(self) -> Dict[str, Any]:
        """Get list of available API endpoints and their details"""
        try:
            response = self._make_request('', {'format': 'json'})
            return response
        except Exception as e:
            self.logger.error(f"Failed to get available endpoints: {str(e)}")
            raise

    def get_earliest_date(self) -> datetime:
        """Get earliest available date for data"""
        try:
            response = self._make_request('congress/earliest', {'format': 'json'})
            if 'congress' in response:
                congress_num = response['congress'].get('number', 1)
                year = 1789 + (congress_num - 1) * 2
                return datetime(year, 1, 1)
        except Exception:
            self.logger.warning("Failed to get earliest date, using default")

        # Default to First Congress if API call fails
        return datetime(1789, 3, 4)

class CongressAPI(CongressBaseAPI):
    """Extended API client for specific data types"""

    def __init__(self, config):
        super().__init__(config)
        self.validator = DataValidator()

    def get_data_for_date(self, date: datetime) -> List[Dict]:
        """Get all data types for a specific date"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            current_congress = self.get_current_congress()

            # Get available endpoints
            endpoints = self.get_available_endpoints()
            all_data = []

            # Process each available endpoint
            for endpoint_name, endpoint_info in endpoints.items():
                if endpoint_name in ['bill', 'amendment', 'nomination', 'treaty']:
                    try:
                        data = self._get_endpoint_data(
                            endpoint_name, 
                            date_str, 
                            current_congress
                        )
                        if data:
                            all_data.extend(data)
                    except Exception as e:
                        self.logger.error(f"Failed to get {endpoint_name} data: {str(e)}")
                        continue

            return all_data

        except Exception as e:
            self.logger.error(f"Failed to get data for date {date}: {str(e)}")
            raise

    def _get_endpoint_data(
        self, 
        endpoint: str, 
        date_str: str, 
        current_congress: int,
        offset: int = 0,
        limit: int = 20
    ) -> List[Dict]:
        """Get data for a specific endpoint with pagination"""
        try:
            params = {
                'offset': offset,
                'limit': limit,
                'format': 'json',
                'sort': 'updateDate+desc',
                'fromDateTime': f"{date_str}T00:00:00Z",
                'toDateTime': f"{date_str}T23:59:59Z"
            }

            self.logger.info(f"Fetching {endpoint} for date {date_str} (offset: {offset})")
            response_data = self._make_request(endpoint, params)
            items = response_data.get(f'{endpoint}s', [])

            if not items:
                return []

            processed_items = []
            for item in items:
                try:
                    processed_item = self._process_item(endpoint, item, current_congress)
                    if processed_item:
                        processed_items.append(processed_item)
                except Exception as e:
                    self.logger.error(f"Failed to process {endpoint} item: {str(e)}")
                    continue

            return processed_items

        except Exception as e:
            self.logger.error(f"Failed to get {endpoint} data: {str(e)}")
            return []

    def _process_item(self, endpoint: str, item: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a single item based on its type"""
        try:
            if endpoint == 'bill':
                return self._process_bill(item, current_congress)
            elif endpoint == 'amendment':
                return self._process_amendment(item, current_congress)
            elif endpoint == 'nomination':
                return self._process_nomination(item, current_congress)
            elif endpoint == 'treaty':
                return self._process_treaty(item, current_congress)
            else:
                self.logger.warning(f"Unknown endpoint type: {endpoint}")
                return None
        except Exception as e:
            self.logger.error(f"Failed to process {endpoint} item: {str(e)}")
            return None

    def _process_bill(self, bill: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bill"""
        try:
            bill_id = bill.get('billId') or self._generate_bill_id(bill)
            if not bill_id:
                self.logger.warning("Unable to generate ID for bill")
                return None

            transformed_bill = {
                'id': bill_id,
                'type': 'bill',
                'congress': bill.get('congress', current_congress),
                'title': bill.get('title', ''),
                'update_date': bill.get('updateDate', ''),
                'bill_type': bill.get('type', ''),
                'bill_number': bill.get('number', ''),
                'version': 1,
                'origin_chamber': bill.get('originChamber', {}).get('name', ''),
                'origin_chamber_code': bill.get('originChamberCode', ''),
                'latest_action': {
                    'text': bill.get('latestAction', {}).get('text', ''),
                    'action_date': bill.get('latestAction', {}).get('actionDate', ''),
                },
                'update_date_including_text': bill.get('updateDateIncludingText', ''),
                'introduced_date': bill.get('introducedDate', ''),
                'sponsors': bill.get('sponsors', []),
                'committees': bill.get('committees', []),
                'url': bill.get('url', '')
            }

            is_valid, errors = self.validator.validate_bill(transformed_bill)
            if not is_valid:
                self.logger.warning(f"Bill {bill_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_bill(transformed_bill)

        except Exception as e:
            self.logger.error(f"Failed to transform bill: {str(e)}")
            return None

    def _generate_bill_id(self, bill: Dict) -> Optional[str]:
        """Generate a bill ID from bill data"""
        try:
            congress = str(bill.get('congress', ''))
            bill_type = bill.get('type', '').lower()
            bill_number = str(bill.get('number', ''))
            if congress and bill_type and bill_number:
                return f"{congress}-{bill_type}-{bill_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate bill ID: {str(e)}")
        return None

    def _process_amendment(self, amendment: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate an amendment"""
        try:
            amendment_id = self._generate_amendment_id(amendment)
            if not amendment_id:
                self.logger.warning("Unable to generate ID for amendment")
                return None

            transformed_amendment = {
                'id': amendment_id,
                'type': 'amendment',
                'congress': amendment.get('congress', current_congress),
                'number': amendment.get('number'),
                'update_date': amendment.get('updateDate', ''),
                'amendment_type': amendment.get('type', ''),
                'purpose': amendment.get('purpose', ''),
                'description': amendment.get('description', ''),
                'version': 1,
                'chamber': amendment.get('chamber', {}).get('name', ''),
                'chamber_code': amendment.get('chamberCode', ''),
                'associated_bill': {
                    'congress': amendment.get('amendedBill', {}).get('congress', ''),
                    'type': amendment.get('amendedBill', {}).get('type', ''),
                    'number': amendment.get('amendedBill', {}).get('number', '')
                },
                'latest_action': {
                    'text': amendment.get('latestAction', {}).get('text', ''),
                    'action_date': amendment.get('latestAction', {}).get('actionDate', ''),
                },
                'update_date_including_text': amendment.get('updateDateIncludingText', ''),
                'submitted_date': amendment.get('submittedDate', ''),
                'sponsors': amendment.get('sponsors', []),
                'url': amendment.get('url', '')
            }

            is_valid, errors = self.validator.validate_amendment(transformed_amendment)
            if not is_valid:
                self.logger.warning(f"Amendment {amendment_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_amendment(transformed_amendment)

        except Exception as e:
            self.logger.error(f"Failed to transform amendment: {str(e)}")
            return None

    def _generate_amendment_id(self, amendment: Dict) -> Optional[str]:
        """Generate an amendment ID from amendment data"""
        try:
            congress = str(amendment.get('congress', ''))
            amdt_type = amendment.get('type', '').lower()
            amdt_number = str(amendment.get('number', ''))
            if congress and amdt_type and amdt_number:
                return f"{congress}-{amdt_type}-{amdt_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate amendment ID: {str(e)}")
        return None

    def _process_nomination(self, nomination: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a nomination"""
        try:
            nomination_id = self._generate_nomination_id(nomination)
            if not nomination_id:
                self.logger.warning("Unable to generate ID for nomination")
                return None

            transformed_nomination = {
                'id': nomination_id,
                'type': 'nomination',
                'congress': nomination.get('congress', current_congress),
                'nomination_number': nomination.get('number'),
                'update_date': nomination.get('updateDate', ''),
                'position': nomination.get('position', ''),
                'nominee': nomination.get('nominee', ''),
                'organization': nomination.get('organization', ''),
                'version': 1,
                'latest_action': {
                    'text': nomination.get('latestAction', {}).get('text', ''),
                    'action_date': nomination.get('latestAction', {}).get('actionDate', ''),
                },
                'update_date_including_text': nomination.get('updateDateIncludingText', ''),
                'received_date': nomination.get('receivedDate', ''),
                'status': nomination.get('status', ''),
                'description': nomination.get('description', ''),
                'url': nomination.get('url', '')
            }

            is_valid, errors = self.validator.validate_nomination(transformed_nomination)
            if not is_valid:
                self.logger.warning(f"Nomination {nomination_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_nomination(transformed_nomination)

        except Exception as e:
            self.logger.error(f"Failed to transform nomination: {str(e)}")
            return None

    def _generate_nomination_id(self, nomination: Dict) -> Optional[str]:
        """Generate a nomination ID from nomination data"""
        try:
            congress = str(nomination.get('congress', ''))
            nom_number = str(nomination.get('number', ''))
            if congress and nom_number:
                return f"{congress}-nom-{nom_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate nomination ID: {str(e)}")
        return None

    def _process_treaty(self, treaty: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a treaty"""
        try:
            treaty_id = self._generate_treaty_id(treaty)
            if not treaty_id:
                self.logger.warning("Unable to generate ID for treaty")
                return None

            transformed_treaty = {
                'id': treaty_id,
                'type': 'treaty',
                'congress': treaty.get('congress', current_congress),
                'treaty_number': treaty.get('number'),
                'update_date': treaty.get('updateDate', ''),
                'title': treaty.get('title', ''),
                'description': treaty.get('description', ''),
                'version': 1,
                'latest_action': {
                    'text': treaty.get('latestAction', {}).get('text', ''),
                    'action_date': treaty.get('latestAction', {}).get('actionDate', ''),
                },
                'update_date_including_text': treaty.get('updateDateIncludingText', ''),
                'transmitted_date': treaty.get('transmittedDate', ''),
                'treaty_type': treaty.get('type', ''),
                'country': treaty.get('country', ''),
                'subject': treaty.get('subject', ''),
                'url': treaty.get('url', '')
            }

            is_valid, errors = self.validator.validate_treaty(transformed_treaty)
            if not is_valid:
                self.logger.warning(f"Treaty {treaty_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_treaty(transformed_treaty)

        except Exception as e:
            self.logger.error(f"Failed to transform treaty: {str(e)}")
            return None

    def _generate_treaty_id(self, treaty: Dict) -> Optional[str]:
        """Generate a treaty ID from treaty data"""
        try:
            congress = str(treaty.get('congress', ''))
            treaty_number = str(treaty.get('number', ''))
            if congress and treaty_number:
                return f"{congress}-treaty-{treaty_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate treaty ID: {str(e)}")
        return None