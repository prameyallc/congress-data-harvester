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

class CongressAPI:
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
        self.validator = DataValidator()

    def _setup_session(self):
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

    def _make_request(self, endpoint, params=None):
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

    def _generate_bill_id(self, bill):
        try:
            congress = str(bill.get('congress', ''))
            bill_type = bill.get('type', '').lower()
            bill_number = str(bill.get('number', ''))
            if congress and bill_type and bill_number:
                return f"{congress}-{bill_type}-{bill_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate bill ID: {str(e)}")
        return None

    @metrics.track_duration('get_data_for_date')
    def get_data_for_date(self, date):
        try:
            date_str = date.strftime('%Y-%m-%d')
            current_congress = self._get_current_congress()
            all_bills = []
            offset = 0
            limit = 20
            max_retries = 3
            retry_count = 0

            invalid_bills = 0
            validation_errors = {}

            while True:
                try:
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

                    processed_count = 0
                    error_count = 0

                    for bill in bills:
                        try:
                            bill_id = bill.get('billId') or self._generate_bill_id(bill)
                            if not bill_id:
                                self.logger.warning(f"Unable to generate ID for bill: {bill}")
                                error_count += 1
                                continue

                            origin_chamber_info = bill.get('originChamber', {})
                            origin_chamber = (origin_chamber_info.get('name', '') if isinstance(origin_chamber_info, dict) else str(origin_chamber_info))
                            latest_action = bill.get('latestAction', {})
                            if isinstance(latest_action, str):
                                latest_action = {'text': latest_action}

                            transformed_bill = {
                                'id': bill_id,
                                'congress': bill.get('congress', current_congress),
                                'title': bill.get('title', ''),
                                'update_date': bill.get('updateDate', date_str),
                                'bill_type': bill.get('type', ''),
                                'bill_number': bill.get('number', ''),
                                'version': 1,
                                'origin_chamber': origin_chamber,
                                'origin_chamber_code': bill.get('originChamberCode', ''),
                                'latest_action': {
                                    'text': latest_action.get('text', ''),
                                    'action_date': latest_action.get('actionDate', ''),
                                },
                                'update_date_including_text': bill.get('updateDateIncludingText', ''),
                                'introduced_date': bill.get('introducedDate', ''),
                                'sponsors': bill.get('sponsors', []),
                                'committees': bill.get('committees', []),
                                'url': bill.get('url', '')
                            }

                            # Validate and clean the bill data
                            is_valid, errors = self.validator.validate_bill(transformed_bill)
                            if not is_valid:
                                invalid_bills += 1
                                validation_errors[bill_id] = errors
                                self.logger.warning(f"Bill {bill_id} failed validation: {errors}")
                                error_count += 1
                                continue

                            # Clean and normalize the data
                            transformed_bill = self.validator.cleanup_bill(transformed_bill)

                            # Validate sponsors and committees
                            for sponsor in transformed_bill['sponsors']:
                                is_valid, errors = self.validator.validate_sponsor(sponsor)
                                if not is_valid:
                                    self.logger.warning(f"Invalid sponsor in bill {bill_id}: {errors}")

                            for committee in transformed_bill['committees']:
                                is_valid, errors = self.validator.validate_committee(committee)
                                if not is_valid:
                                    self.logger.warning(f"Invalid committee in bill {bill_id}: {errors}")

                            all_bills.append(transformed_bill)
                            processed_count += 1

                        except Exception as e:
                            self.logger.error(f"Failed to transform bill {bill.get('billId', 'unknown')}: {str(e)}")
                            error_count += 1
                            continue

                    metrics.track_bills_processed(processed_count, success=True)
                    if error_count > 0:
                        metrics.track_bills_processed(error_count, success=False)

                    if len(bills) < limit:
                        break

                    offset += limit
                    retry_count = 0

                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        self.logger.error(f"Max retries reached for offset {offset}. Moving to next batch.")
                        offset += limit
                        retry_count = 0
                        continue

                    wait_time = 2 ** retry_count
                    self.logger.warning(f"Retrying after {wait_time} seconds. Attempt {retry_count} of {max_retries}")
                    time.sleep(wait_time)

            if invalid_bills > 0:
                self.logger.warning(f"Total invalid bills for {date_str}: {invalid_bills}")
                self.logger.debug(f"Validation errors: {validation_errors}")

            self.logger.info(f"Retrieved {len(all_bills)} valid bills for date {date_str}")
            return all_bills

        except Exception as e:
            self.logger.error(f"Failed to get data for date {date}: {str(e)}")
            raise

    def _get_current_congress(self):
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

    def get_earliest_date(self):
        try:
            response = self._make_request('congress/current', {
                'format': 'json'
            })
            current_congress = response.get('congress', {}).get('number', 118)
            years_since_start = (current_congress - 1) * 2
            start_year = 1789 + years_since_start
            return datetime(start_year, 1, 1)
        except Exception as e:
            self.logger.error(f"Failed to get earliest date: {str(e)}")
            return datetime(2000, 1, 1)