import os
import time
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, List, Any, Optional, Union, Callable
import logging
from random import uniform
from monitoring import metrics
from data_validator import DataValidator
import hashlib
import re
import json

class RateLimiter:
    """Handles rate limiting for API requests"""
    
    def __init__(self, config: Dict[str, Any]) -> None:
        self.requests_per_second = config.get('requests_per_second', 5)
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 1)
        self.last_request_time: Dict[str, float] = {}
        self.consecutive_errors: Dict[str, int] = {}
        self.logger = logging.getLogger('congress_downloader')
        self.endpoint_counts: Dict[str, int] = {}
        self.start_time = time.time()
        self.test_mode = config.get('test_mode', False)

    def wait(self, endpoint: str) -> None:
        """Implement rate limiting with jitter for specific endpoint"""
        # Skip rate limiting in test mode
        if self.test_mode:
            return
            
        current_time = time.time()
        last_time = self.last_request_time.get(endpoint, 0)
        time_since_last_request = current_time - last_time
        
        # Track request counts
        self.endpoint_counts[endpoint] = self.endpoint_counts.get(endpoint, 0) + 1
        
        # Calculate current rate
        elapsed_time = current_time - self.start_time
        if elapsed_time > 0:
            current_rate = self.endpoint_counts[endpoint] / elapsed_time
            self.logger.debug(
                f"Rate stats for {endpoint}: "
                f"{self.endpoint_counts[endpoint]} requests in {elapsed_time:.1f}s "
                f"(current rate: {current_rate:.2f} req/s, limit: {self.requests_per_second} req/s)"
            )
        
        # Base wait time with endpoint-specific adjustment
        base_wait = 1.0 / self.requests_per_second
        if endpoint in ['bill', 'amendment', 'nomination']:  # High-volume endpoints
            base_wait *= 1.5
        
        # Add jitter (±10% of base wait time)
        jitter = uniform(-0.1 * base_wait, 0.1 * base_wait)
        wait_time = max(0, base_wait + jitter)
        
        # Apply exponential backoff if there were errors
        error_count = self.consecutive_errors.get(endpoint, 0)
        if error_count > 0:
            backoff_multiplier = min(2 ** error_count, 60)
            wait_time *= backoff_multiplier
            self.logger.warning(
                f"Rate limit backoff for {endpoint}: "
                f"waiting {wait_time:.2f} seconds after {error_count} consecutive errors"
            )
        
        if time_since_last_request < wait_time:
            sleep_time = wait_time - time_since_last_request
            self.logger.debug(
                f"Rate limiting {endpoint}: "
                f"waiting {sleep_time:.2f}s "
                f"(last request: {time_since_last_request:.2f}s ago)"
            )
            metrics.track_rate_limit_wait(endpoint, sleep_time)
            time.sleep(sleep_time)
        
        self.last_request_time[endpoint] = time.time()

    def record_success(self, endpoint: str) -> None:
        """Record successful request"""
        prev_errors = self.consecutive_errors.get(endpoint, 0)
        if prev_errors > 0:
            self.logger.info(f"Reset error count for {endpoint} after successful request")
        self.consecutive_errors[endpoint] = 0

    def record_error(self, endpoint: str) -> None:
        """Record failed request"""
        self.consecutive_errors[endpoint] = self.consecutive_errors.get(endpoint, 0) + 1
        self.logger.warning(
            f"Recorded error for {endpoint} "
            f"(consecutive errors: {self.consecutive_errors[endpoint]})"
        )

class CongressBaseAPI:
    """Base class for Congress.gov API interactions"""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.base_url = config['base_url'].rstrip('/')
        self.rate_limiter = RateLimiter(config.get('rate_limit', {}))
        self.api_key = os.environ.get('CONGRESS_API_KEY', config.get('api_key'))
        if not self.api_key:
            raise ValueError("Congress.gov API key not found in environment or config")
        self.session = self._setup_session()
        self.logger = logging.getLogger('congress_downloader')
        self.timeout = (5, 30)  # (connect timeout, read timeout)
        self._current_congress = None  # Cache for current congress number
        self._response_cache = {}  # Cache for API responses

    def _get_cached_response(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """Get cached API response if available"""
        if not self.rate_limiter.test_mode:
            return None  # Only use cache in test mode

        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        cached = self._response_cache.get(cache_key)
        if cached:
            self.logger.debug(f"Using cached response for {endpoint}")
            return cached
        return None

    def _cache_response(self, endpoint: str, params: Dict, response: Dict) -> None:
        """Cache API response for future use"""
        if not self.rate_limiter.test_mode:
            return  # Only cache in test mode

        cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        self._response_cache[cache_key] = response
        self.logger.debug(f"Cached response for {endpoint}")

    def _setup_session(self) -> requests.Session:
        """Set up requests session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,  # Use fixed value since we have a separate RateLimiter
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True,
            raise_on_status=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make API request with enhanced rate limiting and caching"""
        if params is None:
            params = {}
        params['api_key'] = self.api_key
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Check cache first
        cached = self._get_cached_response(endpoint, params)
        if cached:
            return cached

        # Apply rate limiting
        self.rate_limiter.wait(endpoint)
        start_time = time.time()
        
        try:
            self.logger.debug(f"Making request to {url} with params: {json.dumps({k: v for k, v in params.items() if k != 'api_key'}, indent=2)}")
            
            # Track request metrics
            metrics.track_api_request_start(endpoint)
            
            response = self.session.get(url, params=params, timeout=self.timeout)
            duration = time.time() - start_time
            
            metrics.track_api_request(
                endpoint=endpoint,
                status_code=response.status_code,
                duration=duration
            )

            if response.status_code == 200:
                self.rate_limiter.record_success(endpoint)
                response_json = response.json()
                # Cache successful response
                self._cache_response(endpoint, params, response_json)
                self.logger.debug(f"Response data: {json.dumps(response_json, indent=2)}")
                return response_json

            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', 60)
                self.logger.error(f"Rate limit exceeded for {endpoint}. Retry after {retry_after} seconds")
                self.rate_limiter.record_error(endpoint)
                time.sleep(float(retry_after))
                raise Exception(f"Rate limit exceeded for {endpoint}")

            if response.status_code == 403:
                self.logger.error(f"API authentication failed for {endpoint}. Please verify API key.")
                raise Exception("API authentication failed - please verify API key")

            self.logger.error(f"Unexpected status code {response.status_code} for {url}: {response.text}")
            self.rate_limiter.record_error(endpoint)
            response.raise_for_status()
            return {}

        except requests.exceptions.Timeout as e:
            duration = time.time() - start_time
            self.logger.error(f"Request timed out for endpoint {endpoint}: {str(e)}")
            self.rate_limiter.record_error(endpoint)
            metrics.track_api_request(
                endpoint=endpoint,
                status_code=408,
                duration=duration
            )
            raise Exception(f"Request timed out: {str(e)}")

        except requests.exceptions.RequestException as e:
            duration = time.time() - start_time
            self.logger.error(f"Request failed for {url}: {str(e)}")
            self.rate_limiter.record_error(endpoint)
            metrics.track_api_request(
                endpoint=endpoint,
                status_code=500,
                duration=duration
            )
            raise Exception(f"Request failed: {str(e)}")

    def get_available_endpoints(self) -> Dict[str, Any]:
        """Get list of available API endpoints and their details"""
        try:
            standard_endpoints = [
                'bill', 'amendment', 'nomination', 'treaty',
                'committee', 'hearing', 'committee-report',
                'congressional-record', 'house-communication',
                'house-requirement', 'senate-communication',
                'member', 'summaries', 'committee-print',
                'committee-meeting', 'daily-congressional-record',
                'bound-congressional-record', 'congress'
            ]
            available_endpoints = {}

            for endpoint in standard_endpoints:
                try:
                    self.logger.info(f"Checking endpoint: {endpoint}")
                    params = {
                        'limit': 1,
                        'format': 'json',
                        'offset': 0
                    }
                    
                    # Add specific parameters for certain endpoints
                    if endpoint in ['committee', 'committee-meeting']:
                        params.update({
                            'congress': self.get_current_congress(),
                            'chamber': 'house,senate'
                        })
                    elif endpoint in ['daily-congressional-record', 'bound-congressional-record']:
                        params.update({
                            'year': datetime.now().year,
                            'month': datetime.now().month
                        })
                    elif endpoint == 'congress':
                        # For congress endpoint, no additional params needed
                        pass
                    
                    response = self._make_request(endpoint, params)

                    if response:
                        available_endpoints[endpoint] = {
                            'name': endpoint,
                            'url': f"{self.base_url}/{endpoint}",
                            'status': 'available',
                            'response_keys': list(response.keys()) if isinstance(response, dict) else []
                        }
                        self.logger.info(f"Found active endpoint: {endpoint}")
                        self.logger.debug(f"Response structure for {endpoint}: {json.dumps(response, indent=2)}")
                except Exception as e:
                    self.logger.warning(f"Endpoint {endpoint} error: {str(e)}")
                    continue

            if not available_endpoints:
                self.logger.error("No endpoints available or accessible")
                raise Exception("No endpoints available or accessible")

            self.logger.info(f"Available endpoints: {list(available_endpoints.keys())}")
            return available_endpoints
        except Exception as e:
            self.logger.error(f"Failed to get available endpoints: {str(e)}")
            raise

    def get_current_congress(self) -> int:
        """Get the current Congress number with caching"""
        if self._current_congress is not None:
            return self._current_congress

        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = self._make_request('congress/current', {'format': 'json'})
                self._current_congress = response.get('congress', {}).get('number', 118)
                return self._current_congress
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    self.logger.error(f"Failed to get current congress after {max_retries} attempts")
                    self._current_congress = 118  # Default to 118th Congress
                    return self._current_congress
                wait_time = 2 ** retry_count
                self.logger.warning(f"Retrying current congress lookup after {wait_time} seconds")
                time.sleep(wait_time)
        
        # Default fallback if loop somehow completes without return
        self._current_congress = 118
        return self._current_congress

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
    """Extended API client for specific data types
    
    Important: This class has been cleaned up to remove duplicate method declarations.
    Only the most complete implementation of each method is kept at the top of the class.
    The following methods have been deduplicated:
    
    - _process_bill() and _generate_bill_id()
    - _process_amendment() and _generate_amendment_id()
    - _process_nomination() and _generate_nomination_id()
    - _process_treaty() and _generate_treaty_id()
    - _process_committee_report() and _generate_committee_report_id()
    - _process_congressional_record() and _generate_congressional_record_id()
    - _process_house_communication() and _generate_house_comm_id()
    - _process_senate_communication() and _generate_senate_comm_id()
    - _process_committee_meeting() and _generate_meeting_id()
    - _process_member() and _generate_member_id()
    - _process_summaries() and _generate_summary_id()
    - _process_bound_congressional_record() and _generate_bound_record_id()
    - _process_daily_congressional_record() and _generate_daily_record_id()
    - _process_hearing() and _generate_hearing_id()
    - _process_house_requirement() and _generate_house_req_id()
    - _process_committee_print() and _generate_print_id()
    """

    def __init__(self, config):
        super().__init__(config)
        self.validator = DataValidator()

    def _process_congress(self, congress_data: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a congress record"""
        try:
            # Initial type validation
            if not isinstance(congress_data, dict):
                self.logger.error(f"Invalid congress data type: {type(congress_data)}, value: {congress_data}")
                return None
                
            # Handle both direct and nested congress data structures
            data = congress_data.get('congress', congress_data)
            if isinstance(data, str):
                self.logger.error(f"Received string instead of dictionary for congress: {data}")
                return None
            
            # Validate data is a dict
            if not isinstance(data, dict):
                self.logger.error(f"Invalid congress data type: {type(data)}, value: {data}")
                return None

            # Extract key fields
            congress_number = data.get('number', 0)
            try:
                congress_number = int(congress_number)
            except (ValueError, TypeError):
                self.logger.error(f"Invalid congress number: {congress_number}")
                return None
                
            # Generate congress ID
            congress_id = self._generate_congress_id(data)
            if not congress_id:
                self.logger.error("Failed to generate congress ID")
                return None

            # Create transformed congress data
            transformed_congress = {
                'id': congress_id,
                'type': 'congress',
                'congress': congress_number,
                'update_date': data.get('updateDate', datetime.now().strftime('%Y-%m-%d')),
                'version': 1,
                'start_date': data.get('startDate', ''),
                'end_date': data.get('endDate', ''),
                'url': data.get('url', '')
            }
            
            # Add additional fields if present
            if 'senate' in data:
                transformed_congress['senate'] = data.get('senate', {})
            
            if 'house' in data:
                transformed_congress['house'] = data.get('house', {})
            
            # Validate the transformed data
            is_valid, errors = self.validator.validate_congress(transformed_congress)
            if not is_valid:
                self.logger.error(f"Congress {congress_id} failed validation: {errors}")
                self.logger.error(f"Invalid congress data: {json.dumps(transformed_congress, indent=2)}")
                return None

            # Clean up and return the data
            return self.validator.cleanup_congress(transformed_congress)
            
        except Exception as e:
            self.logger.error(f"Failed to transform congress: {str(e)}")
            self.logger.error(f"Raw congress data: {json.dumps(congress_data, indent=2)}")
            return None
            
    def _generate_congress_id(self, congress: Dict) -> Optional[str]:
        """Generate a congress ID from congress data"""
        try:
            # Extract congress number
            congress_number = congress.get('number', '')
            if not congress_number:
                self.logger.warning("Missing required field 'number' for congress ID generation")
                return None
                
            # Create a stable, unique ID for this congress
            try:
                number = int(congress_number)
                congress_id = f"congress-{number}"
            except (ValueError, TypeError):
                # If we can't parse the number, use it as a string
                congress_id = f"congress-{congress_number}"
                
            self.logger.debug(f"Generated congress ID: {congress_id}")
            return congress_id
                
        except Exception as e:
            self.logger.error(f"Failed to generate congress ID: {str(e)}")
            self.logger.error(f"Raw congress data: {json.dumps(congress, indent=2)}")
            return None

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

    def _get_endpoint_data(self, endpoint_name: str, date_str: str, current_congress: int) -> List[Dict]:
        """Get data for a specific endpoint and date"""
        try:
            self.logger.info(f"Fetching {endpoint_name} for date {date_str} (offset: 0)")
            
            # Define endpoint-specific parameters
            params = {
                'format': 'json',
                'fromDateTime': f"{date_str}T00:00:00Z",
                'toDateTime': f"{date_str}T23:59:59Z",
                'limit': 20
            }
            
            # Add congress parameter for specific endpoints
            if endpoint_name in ['committee', 'committee-meeting']:
                params['congress'] = current_congress
            
            # For congress endpoint, different parameter structure
            if endpoint_name == 'congress':
                params = {
                    'format': 'json',
                    'limit': 20
                }

            all_items = []
            total_items = 0
            processed_items = 0
            offset = 0
            
            while True:
                params['offset'] = offset
                self.logger.debug(f"Making request to {endpoint_name} with params: {json.dumps(params, indent=2)}")
                
                response = self._make_request(endpoint_name, params)
                self.logger.debug(f"Response contains keys: {list(response.keys())}")
                
                # Map endpoint names to their response keys
                endpoint_key_map = {
                    'bill': 'bills',
                    'amendment': 'amendments',
                    'nomination': 'nominations',
                    'treaty': 'treaties',
                    'committee': 'committees',
                    'hearing': 'hearings',
                    'committee-report': 'committeeReports',
                    'congressional-record': 'congressionalRecords',
                    'house-communication': 'houseCommunications',
                    'senate-communication': 'senateCommunications',
                    'member': 'members',
                    'summaries': 'summaries',
                    'committee-print': 'committeePrints',
                    'committee-meeting': 'committeeMeetings',
                    'daily-congressional-record': 'dailyCongressionalRecords',
                    'bound-congressional-record': 'boundCongressionalRecord',
                    'congress': 'congresses'
                }
                
                data_key = endpoint_key_map.get(endpoint_name)
                if not data_key:
                    self.logger.warning(f"No response key mapping found for endpoint {endpoint_name}")
                    return []
                
                items = response.get(data_key, [])
                if not items:
                    self.logger.warning(f"No items found in response for {endpoint_name}")
                    if response.get('pagination', {}).get('count', 0) > 0:
                        self.logger.warning("Pagination indicates data exists but none was returned")
                    break
                    
                total_items += len(items)
                self.logger.info(f"Found {len(items)} items in '{data_key}' key")
                
                for item in items:
                    try:
                        processed_item = None
                        
                        # Process based on endpoint type
                        if endpoint_name == 'bill':
                            processed_item = self._process_bill(item, current_congress)
                        elif endpoint_name == 'amendment':
                            processed_item = self._process_amendment(item, current_congress)
                        elif endpoint_name == 'nomination':
                            processed_item = self._process_nomination(item, current_congress)
                        elif endpoint_name == 'treaty':
                            processed_item = self._process_treaty(item, current_congress)
                        elif endpoint_name == 'committee':
                            processed_item = self._process_committee(item, current_congress)
                        elif endpoint_name == 'hearing':
                            processed_item = self._process_hearing(item, current_congress)
                        elif endpoint_name == 'committee-report':
                            processed_item = self._process_committee_report(item, current_congress)
                        elif endpoint_name == 'congressional-record':
                            processed_item = self._process_congressional_record(item, current_congress)
                        elif endpoint_name == 'house-communication':
                            processed_item = self._process_house_communication(item, current_congress)
                        elif endpoint_name == 'senate-communication':
                            processed_item = self._process_senate_communication(item, current_congress)
                        elif endpoint_name == 'member':
                            processed_item = self._process_member(item, current_congress)
                        elif endpoint_name == 'summaries':
                            processed_item = self._process_summaries(item, current_congress)
                        elif endpoint_name == 'committee-print':
                            processed_item = self._process_committee_print(item, current_congress)
                        elif endpoint_name == 'committee-meeting':
                            processed_item = self._process_committee_meeting(item, current_congress)
                        elif endpoint_name == 'daily-congressional-record':
                            processed_item = self._process_daily_congressional_record(item, current_congress)
                        elif endpoint_name == 'bound-congressional-record':
                            processed_item = self._process_bound_congressional_record(item, current_congress)
                        elif endpoint_name == 'congress':
                            processed_item = self._process_congress(item, current_congress)
                        
                        if processed_item:
                            all_items.append(processed_item)
                            processed_items += 1
                        else:
                            self.logger.warning(f"Failed to process {endpoint_name} item: {json.dumps(item, indent=2)}")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing {endpoint_name} item: {str(e)}")
                        self.logger.error(f"Problematic item: {json.dumps(item, indent=2)}")
                        continue
                
                # Log success rate for this batch
                success_rate = (processed_items / total_items * 100) if total_items > 0 else 0
                self.logger.info(f"Successfully processed {processed_items} out of {total_items} {endpoint_name} items ({success_rate:.1f}%)")
                
                # Check for pagination
                pagination = response.get('pagination', {})
                if not pagination.get('next'):
                    self.logger.debug(f"No more pages for {endpoint_name}")
                    break
                    
                offset += len(items)
                
                # Safety check to prevent infinite loops
                if offset > 10000:  # Arbitrary limit
                    self.logger.warning(f"Reached maximum offset for {endpoint_name}")
                    break
            
            return all_items
            
        except Exception as e:
            self.logger.error(f"Failed to get {endpoint_name} data: {str(e)}")
            if 'params' in locals():
                self.logger.error(f"Parameters used: {json.dumps(params, indent=2)}")
            return []

    def _process_committee(self, committee: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee"""
        try:
            # Initial type validation
            if not isinstance(committee, dict):
                self.logger.error(f"Invalid committee data type: {type(committee)}, value: {committee}")
                return None
                
            # Handle both direct and nested committee data structures
            committee_data = committee.get('committee', committee)
            if isinstance(committee_data, str):
                self.logger.error(f"Received string instead of dictionary for committee: {committee_data}")
                return None
            
            # Validate committee_data is a dict
            if not isinstance(committee_data, dict):
                self.logger.error(f"Invalid committee_data type: {type(committee_data)}, value: {committee_data}")
                return None

            # Extract and normalize committee data
            chamber = committee_data.get('chamber', '').title()
            system_code = committee_data.get('systemCode', '')
            
            # Generate committee ID
            committee_id = f"{current_congress}-{chamber.lower()}-{system_code}"

            # Get parent committee info if it exists
            parent_committee = committee_data.get('parent', {})
            parent_info = {
                'name': parent_committee.get('name', ''),
                'system_code': parent_committee.get('systemCode', ''),
                'url': parent_committee.get('url', '')
            } if parent_committee and isinstance(parent_committee, dict) else {}

            transformed_committee = {
                'id': committee_id,
                'type': 'committee',
                'congress': current_congress,
                'update_date': committee_data.get('updateDate', ''),
                'version': 1,
                'name': committee_data.get('name', ''),
                'chamber': chamber,
                'committee_type': committee_data.get('committeeTypeCode', ''),
                'system_code': system_code,
                'parent_committee': parent_info,
                'subcommittees': [
                    {
                        'name': subcomm.get('name', ''),
                        'system_code': subcomm.get('systemCode', ''),
                        'url': subcomm.get('url', '')
                    }
                    for subcomm in committee_data.get('subcommittees', [])
                    if isinstance(subcomm, dict)
                ] if committee_data.get('subcommittees') else [],
                'url': committee_data.get('url', '')
            }
            
            # Log the transformed data for debugging
            self.logger.debug(f"Transformed committee data: {json.dumps(transformed_committee, indent=2)}")

            is_valid, errors = self.validator.validate_committee(transformed_committee)
            if not is_valid:
                self.logger.error(f"Committee {committee_id} failed validation: {errors}")
                self.logger.error(f"Invalid committee data: {json.dumps(transformed_committee, indent=2)}")
                return None

            return self.validator.cleanup_committee(transformed_committee)

        except Exception as e:
            self.logger.error(f"Failed to transform committee: {str(e)}")
            self.logger.error(f"Raw committee data: {json.dumps(committee, indent=2)}")
            return None

    def _process_hearing(self, hearing: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a hearing"""
        try:
            hearing_data = hearing.get('hearing', hearing)
            if isinstance(hearing_data, str):
                self.logger.error(f"Received string instead of dictionary for hearing: {hearing_data}")
                return None

            # Extract required fields with proper error handling
            congress = int(hearing_data.get('congress', current_congress))
            chamber = hearing_data.get('chamber', '').lower()
            committee_data = hearing_data.get('committee', {})
            date = hearing_data.get('date', '')
            
            # Generate hearing ID with all required fields
            hearing_id = self._generate_hearing_id({
                'congress': congress,
                'chamber': chamber,
                'committee': committee_data.get('systemCode', ''),
                'date': date
            })

            if not hearing_id:
                self.logger.error("Failed to generate hearing ID - missing required fields")
                self.logger.error(f"Raw hearing data: {json.dumps(hearing_data, indent=2)}")
                return None

            transformed_hearing = {
                'id': hearing_id,
                'type': 'hearing',
                'congress': congress,
                'update_date': hearing_data.get('updateDate', ''),
                'version': 1,
                'chamber': chamber,
                'date': date,
                'time': hearing_data.get('time', ''),
                'location': hearing_data.get('location', ''),
                'title': hearing_data.get('title', ''),
                'committee': {
                    'name': committee_data.get('name', ''),
                    'system_code': committee_data.get('systemCode', ''),
                    'url': committee_data.get('url', '')
                } if committee_data else {},
                'subcommittees': [
                    {
                        'name': subcomm.get('name', ''),
                        'system_code': subcomm.get('systemCode', ''),
                        'url': subcomm.get('url', '')
                    }
                    for subcomm in hearing_data.get('subcommittees', [])
                    if isinstance(subcomm, dict)
                ] if hearing_data.get('subcommittees') else [],
                'url': hearing_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_hearing(transformed_hearing)
            if not is_valid:
                self.logger.error(f"Hearing {hearing_id} failed validation: {errors}")
                self.logger.error(f"Invalid hearing data: {json.dumps(transformed_hearing, indent=2)}")
                return None

            return self.validator.cleanup_hearing(transformed_hearing)

        except Exception as e:
            self.logger.error(f"Failed to transform hearing: {str(e)}")
            self.logger.error(f"Raw hearing data: {json.dumps(hearing, indent=2)}")
            return None

    def _generate_hearing_id(self, hearing: Dict) -> Optional[str]:
        """Generate a hearing ID from hearing data"""
        try:
            congress = str(hearing.get('congress', ''))
            chamber = hearing.get('chamber', '').lower()
            committee = hearing.get('committee', '')
            date = hearing.get('date', '')
            
            if not all([congress, chamber, committee, date]):
                missing_fields = []
                if not congress: missing_fields.append('congress')
                if not chamber: missing_fields.append('chamber')
                if not committee: missing_fields.append('committee')
                if not date: missing_fields.append('date')
                self.logger.warning(f"Missing required fields for hearing ID generation: {', '.join(missing_fields)}")
                return None
            
            # Clean date to remove non-numeric characters
            date_clean = re.sub(r'[^0-9]', '', date)
            
            # Generate final ID
            hearing_id = f"{congress}-{chamber}-{committee}-{date_clean}"
            self.logger.debug(f"Generated hearing ID: {hearing_id}")
            return hearing_id
                
        except Exception as e:
            self.logger.error(f"Failed to generate hearing ID: {str(e)}")
            self.logger.error(f"Raw hearing data: {json.dumps(hearing, indent=2)}")
            return None

    def _process_treaty(self, treaty: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a treaty"""
        try:
            treaty_data = treaty.get('treaty', treaty)
            if isinstance(treaty_data, str):
                self.logger.error(f"Received string instead of dictionary for treaty: {treaty_data}")
                return None

            # Generate treaty ID
            treaty_id = self._generate_treaty_id({
                'congress': treaty_data.get('congress', current_congress),
                'number': treaty_data.get('treatyNumber', '')
            })

            if not treaty_id:
                self.logger.error("Failed to generate treaty ID")
                return None

            transformed_treaty = {
                'id': treaty_id,
                'type': 'treaty',
                'congress': int(treaty_data.get('congress', current_congress)),
                'update_date': treaty_data.get('updateDate', ''),
                'version': 1,
                'treaty_number': str(treaty_data.get('treatyNumber', '')),
                'description': treaty_data.get('description', ''),
                'country': treaty_data.get('country', ''),
                'subject': treaty_data.get('subject', ''),
                'status': treaty_data.get('status', ''),
                'received_date': treaty_data.get('receivedDate', ''),
                'latest_action': {
                    'text': treaty_data.get('latestAction', {}).get('text', '') if isinstance(treaty_data.get('latestAction'), dict) else '',
                    'action_date': treaty_data.get('latestAction', {}).get('actionDate', '') if isinstance(treaty_data.get('latestAction'), dict) else ''
                },
                'committees': [
                    {
                        'name': committee.get('name', ''),
                        'system_code': committee.get('systemCode', ''),
                        'chamber': committee.get('chamber', ''),
                        'type': committee.get('type', ''),
                        'url': committee.get('url', '')
                    }
                    for committee in treaty_data.get('committees', [])
                    if isinstance(committee, dict)
                ] if treaty_data.get('committees') else [],
                'url': treaty_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_treaty(transformed_treaty)
            if not is_valid:
                self.logger.error(f"Treaty {treaty_id} failed validation: {errors}")
                self.logger.error(f"Invalid treaty data: {json.dumps(transformed_treaty, indent=2)}")
                return None

            return self.validator.cleanup_treaty(transformed_treaty)

        except Exception as e:
            self.logger.error(f"Failed to transform treaty: {str(e)}")
            self.logger.error(f"Raw treaty data: {json.dumps(treaty, indent=2)}")
            return None

    def _generate_treaty_id(self, treaty: Dict) -> Optional[str]:
        """Generate a treaty ID from treaty data"""
        try:
            congress = str(treaty.get('congress', ''))
            number = str(treaty.get('number', ''))
            
            if congress and number:
                return f"{congress}-treaty-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate treaty ID: {str(e)}")
            self.logger.error(f"Treaty data: {json.dumps(treaty, indent=2)}")
        return None

    def _process_committee_report(self, report: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee report"""
        try:
            report_data = report.get('committeeReport', report)
            if isinstance(report_data, str):
                self.logger.error(f"Received string instead of dictionary for report: {report_data}")
                return None

            # Generate report ID
            report_id = self._generate_committee_report_id({
                'congress': report_data.get('congress', current_congress),
                'type': report_data.get('type', ''),
                'number': report_data.get('number', '')
            })

            if not report_id:
                self.logger.error("Failed to generate committee report ID")
                return None

            transformed_report = {
                'id': report_id,
                'type': 'committee-report',
                'congress': report_data.get('congress', current_congress),
                'update_date': report_data.get('updateDate', ''),
                'version': 1,
                'report_type': report_data.get('type', ''),
                'number': report_data.get('number', ''),
                'title': report_data.get('title', ''),
                'committee': {
                    'name': report_data.get('committee', {}).get('name', ''),
                    'system_code': report_data.get('committee', {}).get('systemCode', ''),
                    'url': report_data.get('committee', {}).get('url', '')
                },
                'url': report_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee_report(transformed_report)
            if not is_valid:
                self.logger.error(f"Committee report {report_id} failed validation: {errors}")
                self.logger.error(f"Invalid report data: {json.dumps(transformed_report, indent=2)}")
                return None

            return self.validator.cleanup_committee_report(transformed_report)

        except Exception as e:
            self.logger.error(f"Failed to transform committee report: {str(e)}")
            self.logger.error(f"Raw report data: {json.dumps(report, indent=2)}")
            return None

    def _generate_committee_report_id(self, report: Dict) -> Optional[str]:
        """Generate a committee report ID"""
        try:
            congress = str(report.get('congress', ''))
            report_type = report.get('type', '').lower()
            number = str(report.get('number', ''))
            
            if congress and report_type and number:
                return f"{congress}-crpt-{report_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate committee report ID: {str(e)}")
            self.logger.error(f"Report data: {json.dumps(report, indent=2)}")
        return None

    def _process_daily_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a daily congressional record"""
        try:
            record_data = record.get('dailyCongressionalRecord', record)
            if isinstance(record_data, str):
                self.logger.error(f"Received string instead of dictionary for daily record: {record_data}")
                return None

            # Extract required fields with proper error handling
            congress = int(record_data.get('congress', current_congress))
            volume = record_data.get('volume', '')
            issue = record_data.get('issue', '')
            date = record_data.get('date', '')
            
            # Generate record ID
            record_id = self._generate_daily_record_id({
                'congress': congress,
                'volume': volume,
                'issue': issue,
                'date': date
            })

            if not record_id:
                self.logger.error("Failed to generate daily congressional record ID - missing required fields")
                self.logger.error(f"Raw record data: {json.dumps(record_data, indent=2)}")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'daily-congressional-record',
                'congress': congress,
                'update_date': record_data.get('updateDate', ''),
                'version': 1,
                'volume': volume,
                'issue': issue,
                'date': date,
                'chamber': record_data.get('chamber', '').lower(),
                'pages': record_data.get('pages', []),
                'url': record_data.get('url', '')
            }

            # Log the transformed data for debugging
            self.logger.debug(f"Transformed daily record data: {json.dumps(transformed_record, indent=2)}")

            is_valid, errors = self.validator.validate_daily_congressional_record(transformed_record)
            if not is_valid:
                self.logger.error(f"Daily congressional record {record_id} failed validation: {errors}")
                self.logger.error(f"Invalid record data: {json.dumps(transformed_record, indent=2)}")
                return None

            return self.validator.cleanup_daily_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform daily congressional record: {str(e)}")
            self.logger.error(f"Raw record data: {json.dumps(record, indent=2)}")
            return None

    def _generate_daily_record_id(self, record: Dict) -> Optional[str]:
        """Generate a daily congressional record ID"""
        try:
            congress = str(record.get('congress', ''))
            volume = str(record.get('volume', ''))
            issue = str(record.get('issue', ''))
            date = record.get('date', '')
            
            if congress and volume and issue and date:
                # Clean date to remove non-numeric characters
                date_clean = re.sub(r'[^0-9]', '', date)
                return f"{congress}-dcr-{volume}-{issue}-{date_clean}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate daily record ID: {str(e)}")
            self.logger.error(f"Record data: {json.dumps(record, indent=2)}")
        return None

    def _generate_amendment_id(self, amendment: Dict) -> Optional[str]:
        """Generate an amendment ID from amendment data"""
        try:
            congress = str(amendment.get('congress', ''))
            amdt_type = amendment.get('type', '').lower()
            number = str(amendment.get('number', ''))
            
            if congress and amdt_type and number:
                return f"{congress}-{amdt_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate amendment ID: {str(e)}")
            self.logger.error(f"Amendment data: {json.dumps(amendment, indent=2)}")
        return None

    def _generate_bill_id(self, bill: Dict) -> Optional[str]:
        """Generate a bill ID from bill data"""
        try:
            congress = str(bill.get('congress', ''))
            bill_type = bill.get('type', '').lower()
            number = str(bill.get('number', ''))
            
            if congress and bill_type and number:
                return f"{congress}-{bill_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate bill ID: {str(e)}")
            self.logger.error(f"Bill data: {json.dumps(bill, indent=2)}")
        return None

    def _get_endpoint_data(self, endpoint_name: str, date_str: str, current_congress: int) -> List[Dict]:
        """Get data for a specific endpoint on a specific date"""
        try:
            offset = 0
            all_items = []
            limit = 100  # Default limit per request

            while True:
                self.logger.info(f"Fetching {endpoint_name} for date {date_str} (offset: {offset})")
                
                # Build params based on endpoint type
                params = {
                    'fromDateTime': f"{date_str}T00:00:00Z",
                    'toDateTime': f"{date_str}T23:59:59Z",
                    'format': 'json',
                    'limit': limit,
                    'offset': offset
                }
                
                # Add specific parameters for certain endpoints
                if endpoint_name in ['committee', 'committee-meeting']:
                    params.update({
                        'congress': current_congress,
                        'chamber': 'house,senate'
                    })
                elif endpoint_name in ['daily-congressional-record', 'bound-congressional-record']:
                    params = {
                        'year': datetime.strptime(date_str, '%Y-%m-%d').year,
                        'month': datetime.strptime(date_str, '%Y-%m-%d').month,
                        'format': 'json',
                        'limit': limit,
                        'offset': offset
                    }

                response = self._make_request(endpoint_name, params)
                
                # Extract items from response based on endpoint type
                items = []
                if endpoint_name == 'daily-congressional-record':
                    items = response.get('dailyCongressionalRecords', [])
                else:
                    # Default pluralization for other endpoints
                    items = response.get(endpoint_name + 's', [])  

                if not items:
                    self.logger.info(f"Found 0 items in '{endpoint_name + 's'}' key")
                    self.logger.warning(f"No items found in response for {endpoint_name}")
                    break

                processed_items = []
                for item in items:
                    processed_item = None
                    
                    # Process item based on endpoint type
                    if endpoint_name == 'daily-congressional-record':
                        processed_item = self._process_daily_congressional_record(item, current_congress)
                    elif endpoint_name == 'committee':
                        processed_item = self._process_committee(item, current_congress)
                    elif endpoint_name == 'hearing':
                        processed_item = self._process_hearing(item, current_congress)
                    elif endpoint_name == 'treaty':
                        processed_item = self._process_treaty(item, current_congress)
                    elif endpoint_name == 'committee-report':
                        processed_item = self._process_committee_report(item, current_congress)
                    elif endpoint_name == 'amendment':
                        processed_item = self._process_amendment(item, current_congress)
                    elif endpoint_name == 'bill':
                        processed_item = self._process_bill(item, current_congress)
                    elif endpoint_name == 'nomination':
                        processed_item = self._process_nomination(item, current_congress)
                    elif endpoint_name == 'house-communication':
                        processed_item = self._process_house_communication(item, current_congress)
                    elif endpoint_name == 'senate-communication':
                        processed_item = self._process_senate_communication(item, current_congress)
                    elif endpoint_name == 'member':
                        processed_item = self._process_member(item, current_congress)
                    elif endpoint_name == 'summaries':
                        processed_item = self._process_summaries(item, current_congress)
                    elif endpoint_name == 'committee-print':
                        processed_item = self._process_committee_print(item, current_congress)
                    elif endpoint_name == 'committee-meeting':
                        processed_item = self._process_committee_meeting(item, current_congress)
                    
                    if processed_item:
                        processed_items.append(processed_item)

                if processed_items:
                    all_items.extend(processed_items)
                    self.logger.info(f"Successfully processed {len(processed_items)} items")
                else:
                    self.logger.warning(f"No items were successfully processed")

                if len(items) < limit:
                    break
                    
                offset += limit

            self.logger.info(f"Successfully processed {len(all_items)} out of {len(items)} {endpoint_name} items")
            return all_items

        except Exception as e:
            self.logger.error(f"Error getting {endpoint_name} data: {str(e)}")
            return []

    def _process_bill(self, bill: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bill"""
        try:
            # Verify input type and structure
            if not isinstance(bill, dict):
                self.logger.error(f"Invalid bill data type: {type(bill)}")
                return None

            # Handle both direct and nested bill data structures
            bill_data = bill.get('bill', bill)
            if not isinstance(bill_data, dict):
                self.logger.error(f"Invalid bill_data type: {type(bill_data)}")
                return None

            # Generate bill ID
            bill_id = self._generate_bill_id({
                'congress': bill_data.get('congress', current_congress),
                'type': bill_data.get('type', ''),
                'number': bill_data.get('number', '')
            })

            if not bill_id:
                self.logger.error("Failed to generate bill ID")
                return None

            # Transform bill data with additional error checking
            transformed_bill = {
                'id': bill_id,
                'type': 'bill',
                'congress': int(bill_data.get('congress', current_congress)),
                'update_date': bill_data.get('updateDate', ''),
                'version': 1,
                'bill_type': bill_data.get('type', '').lower(),
                'number': str(bill_data.get('number', '')),
                'title': bill_data.get('title', ''),
                'origin_chamber': bill_data.get('originChamber', ''),
                'origin_chamber_code': bill_data.get('originChamberCode', ''),
                'latest_action': {
                    'text': bill_data.get('latestAction', {}).get('text', '') if isinstance(bill_data.get('latestAction'), dict) else '',
                    'action_date': bill_data.get('latestAction', {}).get('actionDate', '') if isinstance(bill_data.get('latestAction'), dict) else ''
                },
                'committees': [
                    {
                        'name': committee.get('name', ''),
                        'system_code': committee.get('systemCode', ''),
                        'chamber': committee.get('chamber', ''),
                        'type': committee.get('type', ''),
                        'url': committee.get('url', '')
                    }
                    for committee in bill_data.get('committees', [])
                    if isinstance(committee, dict)
                ] if bill_data.get('committees') else [],
                'cosponsors_count': int(bill_data.get('cosponsorsCount', 0)),
                'url': bill_data.get('url', '')
            }

            # Validate the transformed bill
            is_valid, errors = self.validator.validate_bill(transformed_bill)
            if not is_valid:
                self.logger.error(f"Bill {bill_id} failed validation: {errors}")
                self.logger.error(f"Invalid bill data: {json.dumps(transformed_bill, indent=2)}")
                return None

            return self.validator.cleanup_bill(transformed_bill)

        except Exception as e:
            self.logger.error(f"Failed to transform bill: {str(e)}")
            self.logger.error(f"Raw bill data: {json.dumps(bill, indent=2)}")
            return None

    def _process_nomination(self, nomination: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a nomination"""
        try:
            nomination_data = nomination.get('nomination', nomination)
            if isinstance(nomination_data, str):
                self.logger.error(f"Received string instead of dictionary for nomination: {nomination_data}")
                return None

            # Generate nomination ID
            nomination_id = self._generate_nomination_id({
                'congress': nomination_data.get('congress', current_congress),
                'number': nomination_data.get('number', ''),
                'part': nomination_data.get('partNumber', '')
            })

            if not nomination_id:
                self.logger.error("Failed to generate nomination ID")
                return None

            transformed_nomination = {
                'id': nomination_id,
                'type': 'nomination',
                'congress': int(nomination_data.get('congress', current_congress)),
                'update_date': nomination_data.get('updateDate', ''),
                'version': 1,
                'number': str(nomination_data.get('number', '')),
                'part_number': nomination_data.get('partNumber', ''),
                'description': nomination_data.get('description', ''),
                'nominee': nomination_data.get('nominee', ''),
                'position': nomination_data.get('position', ''),
                'organization': nomination_data.get('organization', ''),
                'nomination_type': {
                    'is_civilian': nomination_data.get('nominationType', {}).get('isCivilian', True)
                },
                'received_date': nomination_data.get('receivedDate', ''),
                'latest_action': {
                    'text': nomination_data.get('latestAction', {}).get('text', '') if isinstance(nomination_data.get('latestAction'), dict) else '',
                    'action_date': nomination_data.get('latestAction', {}).get('actionDate', '') if isinstance(nomination_data.get('latestAction'), dict) else ''
                },
                'committees': [
                    {
                        'name': committee.get('name', ''),
                        'system_code': committee.get('systemCode', ''),
                        'chamber': committee.get('chamber', ''),
                        'type': committee.get('type', ''),
                        'url': committee.get('url', '')
                    }
                    for committee in nomination_data.get('committees', [])
                    if isinstance(committee, dict)
                ] if nomination_data.get('committees') else [],
                'url': nomination_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_nomination(transformed_nomination)
            if not is_valid:
                self.logger.error(f"Nomination {nomination_id} failed validation: {errors}")
                self.logger.error(f"Invalid nomination data: {json.dumps(transformed_nomination, indent=2)}")
                return None

            return self.validator.cleanup_nomination(transformed_nomination)

        except Exception as e:
            self.logger.error(f"Failed to transform nomination: {str(e)}")
            self.logger.error(f"Raw nomination data: {json.dumps(nomination, indent=2)}")
            return None

    def _generate_nomination_id(self, nomination: Dict) -> Optional[str]:
        """Generate a nomination ID from nomination data"""
        try:
            congress = str(nomination.get('congress', ''))
            number = str(nomination.get('number', ''))
            part = str(nomination.get('part', ''))
            
            if congress and number:
                if part:
                    return f"{congress}-nom-{number}-{part}"
                return f"{congress}-nom-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate nomination ID: {str(e)}")
            self.logger.error(f"Nomination data: {json.dumps(nomination, indent=2)}")
        return None

    def _process_house_communication(self, communication: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house communication"""
        try:
            communication_data = communication.get('houseCommunication', communication)
            if isinstance(communication_data, str):
                self.logger.error(f"Received string instead of dictionary for house communication: {communication_data}")
                return None

            # Generate communication ID
            comm_id = self._generate_house_comm_id({
                'congress': communication_data.get('congress', current_congress),
                'type': communication_data.get('type', ''),
                'number': communication_data.get('number', '')
            })

            if not comm_id:
                self.logger.error("Failed to generate house communication ID")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'house-communication',
                'congress': communication_data.get('congress', current_congress),
                'update_date': communication_data.get('updateDate', ''),
                'version': 1,
                'communication_type': communication_data.get('type', ''),
                'number': communication_data.get('number', ''),
                'title': communication_data.get('title', ''),
                'received_date': communication_data.get('receivedDate', ''),
                'from_entity': communication_data.get('from', ''),
                'latest_action': {
                    'text': communication_data.get('latestAction', {}).get('text', ''),
                    'action_date': communication_data.get('latestAction', {}).get('actionDate', '')
                },
                'url': communication_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_communication(transformed_comm)
            if not is_valid:
                self.logger.error(f"House communication {comm_id} failed validation: {errors}")
                self.logger.error(f"Invalid communication data: {json.dumps(transformed_comm, indent=2)}")
                return None

            return self.validator.cleanup_house_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform house communication: {str(e)}")
            self.logger.error(f"Raw communication data: {json.dumps(communication, indent=2)}")
            return None

    def _process_senate_communication(self, communication: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a senate communication"""
        try:
            communication_data = communication.get('senateCommunication', communication)
            if isinstance(communication_data, str):
                self.logger.error(f"Received string instead of dictionary for senate communication: {communication_data}")
                return None

            # Generate communication ID
            comm_id = self._generate_senate_comm_id({
                'congress': communication_data.get('congress', current_congress),
                'type': communication_data.get('type', ''),
                'number': communication_data.get('number', '')
            })

            if not comm_id:
                self.logger.error("Failed to generate senate communication ID")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'senate-communication',
                'congress': communication_data.get('congress', current_congress),
                'update_date': communication_data.get('updateDate', ''),
                'version': 1,
                'communication_type': communication_data.get('type', ''),
                'number': communication_data.get('number', ''),
                'title': communication_data.get('title', ''),
                'received_date': communication_data.get('receivedDate', ''),
                'from_entity': communication_data.get('from', ''),
                'latest_action': {
                    'text': communication_data.get('latestAction', {}).get('text', ''),
                    'action_date': communication_data.get('latestAction', {}).get('actionDate', '')
                },
                'url': communication_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_senate_communication(transformed_comm)
            if not is_valid:
                self.logger.error(f"Senate communication {comm_id} failed validation: {errors}")
                self.logger.error(f"Invalid communication data: {json.dumps(transformed_comm, indent=2)}")
                return None

            return self.validator.cleanup_senate_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform senate communication: {str(e)}")
            self.logger.error(f"Raw communication data: {json.dumps(communication, indent=2)}")
            return None

    def _generate_senate_comm_id(self, communication: Dict) -> Optional[str]:
        """Generate a senate communication ID"""
        try:
            congress = str(communication.get('congress', ''))
            comm_type = communication.get('type', '').lower()
            number = str(communication.get('number', ''))
            
            if congress and comm_type and number:
                return f"{congress}-scomm-{comm_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate senate communication ID: {str(e)}")
            self.logger.error(f"Communication data: {json.dumps(communication, indent=2)}")
            return None

    def _generate_house_comm_id(self, communication: Dict) -> Optional[str]:
        """Generate a house communication ID"""
        try:
            congress = str(communication.get('congress', ''))
            comm_type = communication.get('type', '').lower()
            number = str(communication.get('number', ''))
            
            if congress and comm_type and number:
                return f"{congress}-hcomm-{comm_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate house communication ID: {str(e)}")
            self.logger.error(f"Communication data: {json.dumps(communication, indent=2)}")
        return None

    def _process_endpoint(self, endpoint: str, params: Dict) -> List[Dict]:
        """Process data from a specific endpoint with enhanced error handling"""
        try:
            self.logger.info(f"Processing endpoint: {endpoint}")
            response = self._make_request(endpoint, params)
            
            # Log response structure for debugging
            self.logger.debug(f"Response structure for {endpoint}: {json.dumps(response, indent=2)}")
            
            items = []
            possible_keys = [
                f'{endpoint}s',  # plural form
                endpoint,        # singular form
                endpoint.replace('-', ''),  # without hyphens
                f"{endpoint.replace('-', '')}s",  # plural without hyphens
                'results',      # generic results key
                'data'          # alternative data key
            ]
            
            for key in possible_keys:
                if key in response:
                    items = response[key]
                    self.logger.info(f"Found {len(items)} items in '{key}' key")
                    break
            
            if not items:
                self.logger.warning(f"No items found in response for {endpoint}")
                return []
            
            # Process each item with the appropriate handler
            processed_items = []
            for item in items:
                try:
                    current_congress = self.get_current_congress()  # Get congress number
                    processed_item = self._process_item(endpoint, item, current_congress)
                    if processed_item:
                        processed_items.append(processed_item)
                except Exception as e:
                    self.logger.error(f"Failed to process {endpoint} item: {str(e)}")
                    continue
            
            return processed_items
            
        except Exception as e:
            self.logger.error(f"Failed to process endpoint {endpoint}: {str(e)}")
            return []

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
                'fromDateTime': f"{date_str}T00:00:00Z",
                'toDateTime': f"{date_str}T23:59:59Z"
            }

            # Add specific parameters for committee endpoint
            if endpoint == 'committee':
                params.update({
                    'congress': current_congress,
                    'chamber': 'house,senate',  # Request both chambers
                    'limit': 100  # Increase limit for committees
                })

            self.logger.info(f"Fetching {endpoint} for date {date_str} (offset: {offset})")
            self.logger.debug(f"Request parameters: {json.dumps(params, indent=2)}")
            
            response_data = self._make_request(endpoint, params)
            
            # Add detailed logging for API response
            self.logger.debug(f"Raw API response for {endpoint}: {json.dumps(response_data, indent=2)}")
            
            # Get items from response, handling both singular and plural keys
            items = []
            possible_keys = [
                'committees',  # Standard plural form
                'committee',   # Standard singular form
                f'{endpoint}s',  # Dynamic plural form
                endpoint,        # Dynamic singular form
                'results',      # Generic results key
                'data',         # Alternative data key
                endpoint.replace('-', ''),  # Handle hyphenated endpoints
                f"{endpoint.replace('-', '')}s"  # Plural form of hyphenated endpoints
            ]
            
            # Log all available response keys for debugging
            self.logger.debug(f"Response contains keys: {list(response_data.keys())}")
            self.logger.debug(f"Looking for items in these possible keys: {possible_keys}")

            # Find items in response using possible keys
            for key in possible_keys:
                if key in response_data:
                    items = response_data[key]
                    self.logger.info(f"Found {len(items)} items in '{key}' key")
                    if items:  # Log sample item structure
                        self.logger.debug(f"Sample item structure: {json.dumps(items[0], indent=2)}")
                    break
                    
            if not items:
                self.logger.warning(f"No items found in response for {endpoint}")
                return []

            # Process each item
            processed_items = []
            for item in items:
                try:
                    processed_item = self._process_item(endpoint, item, current_congress)
                    if processed_item:
                        processed_items.append(processed_item)
                    else:
                        self.logger.warning(f"Failed to process {endpoint} item: {json.dumps(item, indent=2)}")
                except Exception as e:
                    self.logger.error(f"Failed to process {endpoint} item: {str(e)}")
                    self.logger.error(f"Problematic item: {json.dumps(item, indent=2)}")
                    continue

            self.logger.info(f"Successfully processed {len(processed_items)} out of {len(items)} {endpoint} items")
            return processed_items
            
        except Exception as e:
            self.logger.error(f"Failed to get {endpoint} data: {str(e)}")
            return []

    def _process_bill(self, bill: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bill"""
        try:
            # Handle both direct and nested bill data structures
            bill_data = bill.get('bill', bill)
            if isinstance(bill_data, str):
                self.logger.error(f"Received string instead of dictionary for bill: {bill_data}")
                return None

            # Generate bill ID
            bill_id = self._generate_bill_id({
                'congress': bill_data.get('congress', current_congress),
                'type': bill_data.get('type', ''),
                'number': bill_data.get('number', '')
            })

            if not bill_id:
                self.logger.error("Failed to generate bill ID")
                return None

            transformed_bill = {
                'id': bill_id,
                'type': 'bill',
                'congress': bill_data.get('congress', current_congress),
                'update_date': bill_data.get('updateDate', ''),
                'version': 1,
                'bill_type': bill_data.get('type', ''),
                'number': bill_data.get('number', ''),
                'title': bill_data.get('title', ''),
                'latest_action': {
                    'text': bill_data.get('latestAction', {}).get('text', ''),
                    'action_date': bill_data.get('latestAction', {}).get('actionDate', '')
                },
                'committees': [
                    {
                        'name': committee.get('name', ''),
                        'system_code': committee.get('systemCode', ''),
                        'chamber': committee.get('chamber', ''),
                        'type': committee.get('type', ''),
                        'url': committee.get('url', '')
                    }
                    for committee in bill_data.get('committees', [])
                ],
                'cosponsors_count': bill_data.get('cosponsorsCount', 0),
                'url': bill_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_bill(transformed_bill)
            if not is_valid:
                self.logger.error(f"Bill {bill_id} failed validation: {errors}")
                self.logger.error(f"Invalid bill data: {json.dumps(transformed_bill, indent=2)}")
                return None

            return self.validator.cleanup_bill(transformed_bill)

        except Exception as e:
            self.logger.error(f"Failed to transform bill: {str(e)}")
            self.logger.error(f"Raw bill data: {json.dumps(bill, indent=2)}")
            return None

    def _generate_bill_id(self, bill: Dict) -> Optional[str]:
        """Generate a bill ID from bill data"""
        try:
            congress = str(bill.get('congress', ''))
            bill_type = bill.get('type', '').lower()
            number = str(bill.get('number', ''))
            
            if congress and bill_type and number:
                return f"{congress}-{bill_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate bill ID: {str(e)}")
            self.logger.error(f"Bill data: {json.dumps(bill, indent=2)}")
            return None

    def _process_item(self, endpoint: str, item: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a single item based on its type"""
        try:
            processor = self._get_endpoint_processor(endpoint)
            if not processor:
                return None
                
            return processor(item, current_congress)

        except Exception as e:
            self.logger.error(f"Failed to process item: {str(e)}")
            return None
            
    def _get_endpoint_processor(self, endpoint: str) -> Optional[Callable]:
        """Get the appropriate processor function for the endpoint"""
        try:
            processors = {
                'amendment': self._process_amendment,
                'bill': self._process_bill,
                'committee': self._process_committee,
                'committee-report': self._process_committee_report, 
                'treaty': self._process_treaty,
                'nomination': self._process_nomination,
                'house-communication': self._process_house_communication,
                'senate-communication': self._process_senate_communication,
                'house-requirement': self._process_house_requirement,
                'member': self._process_member,
                'summaries': self._process_summaries,
                'committee-print': self._process_committee_print,
                'committee-meeting': self._process_committee_meeting,
                'daily-congressional-record': self._process_daily_congressional_record,
                'bound-congressional-record': self._process_bound_congressional_record,
                'hearing': self._process_hearing,
                'congressional-record': self._process_congressional_record
            }

            if endpoint not in processors:
                self.logger.error(f"No processor found for endpoint: {endpoint}")
                return None

            return processors[endpoint]

        except Exception as e:
            self.logger.error(f"Error getting endpoint processor: {str(e)}")
            return None

    def _generate_senate_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a senate communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-scomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate senate communication ID: {str(e)}")
        return None

    def _process_amendment(self, amendment: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate an amendment"""
        try:
            # Handle both direct and nested amendment data structures
            amendment_data = amendment.get('amendment', amendment)
            if isinstance(amendment_data, str):
                self.logger.error(f"Received string instead of dictionary for amendment: {amendment_data}")
                return None

            # Generate amendment ID
            amendment_id = self._generate_amendment_id({
                'congress': amendment_data.get('congress', current_congress),
                'type': amendment_data.get('type', ''),
                'number': amendment_data.get('number', '')
            })

            if not amendment_id:
                self.logger.error("Failed to generate amendment ID")
                return None

            transformed_amendment = {
                'id': amendment_id,
                'type': 'amendment',
                'congress': amendment_data.get('congress', current_congress),
                'update_date': amendment_data.get('updateDate', ''),
                'version': 1,
                'amendment_type': amendment_data.get('type', ''),
                'number': amendment_data.get('number', ''),
                'title': amendment_data.get('title', ''),
                'purpose': amendment_data.get('purpose', ''),
                'latest_action': {
                    'text': amendment_data.get('latestAction', {}).get('text', ''),
                    'action_date': amendment_data.get('latestAction', {}).get('actionDate', '')
                },
                'url': amendment_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_amendment(transformed_amendment)
            if not is_valid:
                self.logger.error(f"Amendment {amendment_id} failed validation: {errors}")
                self.logger.error(f"Invalid amendment data: {json.dumps(transformed_amendment, indent=2)}")
                return None

            return self.validator.cleanup_amendment(transformed_amendment)

        except Exception as e:
            self.logger.error(f"Failed to transform amendment: {str(e)}")
            self.logger.error(f"Raw amendment data: {json.dumps(amendment, indent=2)}")
            return None

    # Removing duplicate declarations: The following methods have been deduplicated
    # and only the most complete implementation is kept:
    # - _process_amendment() and _generate_amendment_id()
    # - _process_nomination() and _generate_nomination_id() 
    # - _process_bill() and _generate_bill_id()
    # - _process_summaries() and _generate_summary_id()
    # - _process_congressional_record() and _generate_congressional_record_id()
    # - _process_house_communication() and _generate_house_comm_id() 
    # - _process_committee_report() and _generate_committee_report_id()
    # - _process_member() and _generate_member_id()
    # - _process_treaty() and _generate_treaty_id()

    def _process_nomination(self, nomination: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a nomination"""
        try:
            # Handle both direct and nested nomination data structures
            nomination_data = nomination.get('nomination', nomination)
            if isinstance(nomination_data, str):
                self.logger.error(f"Received string instead of dictionary for nomination: {nomination_data}")
                return None

            # Generate nomination ID
            nomination_id = self._generate_nomination_id({
                'congress': nomination_data.get('congress', current_congress),
                'number': nomination_data.get('number', '')
            })

            if not nomination_id:
                self.logger.error("Failed to generate nomination ID")
                return None

            transformed_nomination = {
                'id': nomination_id,
                'type': 'nomination',
                'congress': nomination_data.get('congress', current_congress),
                'update_date': nomination_data.get('updateDate', ''),
                'version': 1,
                'number': nomination_data.get('number', ''),
                'title': nomination_data.get('description', ''),
                'nominee': nomination_data.get('nominee', ''),
                'position': nomination_data.get('position', ''),
                'organization': nomination_data.get('organization', ''),
                'latest_action': {
                    'text': nomination_data.get('latestAction', {}).get('text', ''),
                    'action_date': nomination_data.get('latestAction', {}).get('actionDate', '')
                },
                'url': nomination_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_nomination(transformed_nomination)
            if not is_valid:
                self.logger.error(f"Nomination {nomination_id} failed validation: {errors}")
                self.logger.error(f"Invalid nomination data: {json.dumps(transformed_nomination, indent=2)}")
                return None

            return self.validator.cleanup_nomination(transformed_nomination)

        except Exception as e:
            self.logger.error(f"Failed to transform nomination: {str(e)}")
            self.logger.error(f"Raw nomination data: {json.dumps(nomination, indent=2)}")
            return None

    def _generate_nomination_id(self, nomination: Dict) -> Optional[str]:
        """Generate a nomination ID from nomination data"""
        try:
            congress = str(nomination.get('congress', ''))
            number = str(nomination.get('number', ''))
            
            if congress and number:
                return f"{congress}-nom-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate nomination ID: {str(e)}")
            self.logger.error(f"Nomination data: {json.dumps(nomination, indent=2)}")
        return None

    def _process_bill(self, bill: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bill"""
        try:
            # Handle both direct and nested bill data structures
            bill_data = bill.get('bill', bill)
            if isinstance(bill_data, str):
                self.logger.error(f"Received string instead of dictionary for bill: {bill_data}")
                return None

            # Generate bill ID
            bill_id = self._generate_bill_id({
                'congress': bill_data.get('congress', current_congress),
                'type': bill_data.get('type', ''),
                'number': bill_data.get('number', '')
            })

            if not bill_id:
                self.logger.error("Failed to generate bill ID")
                return None

            transformed_bill = {
                'id': bill_id,
                'type': 'bill',
                'congress': bill_data.get('congress', current_congress),
                'update_date': bill_data.get('updateDate', ''),
                'version': 1,
                'bill_type': bill_data.get('type', ''),
                'number': bill_data.get('number', ''),
                'title': bill_data.get('title', ''),
                'latest_action': {
                    'text': bill_data.get('latestAction', {}).get('text', ''),
                    'action_date': bill_data.get('latestAction', {}).get('actionDate', '')
                },
                'committees': [
                    {
                        'name': committee.get('name', ''),
                        'system_code': committee.get('systemCode', ''),
                        'chamber': committee.get('chamber', ''),
                        'type': committee.get('type', ''),
                        'url': committee.get('url', '')
                    }
                    for committee in bill_data.get('committees', [])
                ],
                'cosponsors_count': bill_data.get('cosponsorsCount', 0),
                'url': bill_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_bill(transformed_bill)
            if not is_valid:
                self.logger.error(f"Bill {bill_id} failed validation: {errors}")
                self.logger.error(f"Invalid bill data: {json.dumps(transformed_bill, indent=2)}")
                return None

            return self.validator.cleanup_bill(transformed_bill)

        except Exception as e:
            self.logger.error(f"Failed to transform bill: {str(e)}")
            self.logger.error(f"Raw bill data: {json.dumps(bill, indent=2)}")
            return None

    def _generate_bill_id(self, bill: Dict) -> Optional[str]:
        """Generate a bill ID from bill data"""
        try:
            congress = str(bill.get('congress', ''))
            bill_type = bill.get('type', '').lower()
            number = str(bill.get('number', ''))
            
            if congress and bill_type and number:
                return f"{congress}-{bill_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate bill ID: {str(e)}")
            self.logger.error(f"Bill data: {json.dumps(bill, indent=2)}")
            return None

    def _process_committee(self, committee: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee"""
        try:
            # Handle both direct and nested committee data structures
            committee_data = committee.get('committee', committee)
            if isinstance(committee_data, str):
                self.logger.error(f"Received string instead of dictionary for committee: {committee_data}")
                return None

            # Generate committee ID
            committee_id = f"{current_congress}-{committee_data.get('chamber', '').lower()}-{committee_data.get('systemCode', '')}"

            transformed_committee = {
                'id': committee_id,
                'type': 'committee',
                'congress': current_congress,
                'update_date': committee_data.get('updateDate', ''),
                'version': 1,
                'name': committee_data.get('name', ''),
                'chamber': committee_data.get('chamber', ''),
                'system_code': committee_data.get('systemCode', ''),
                'parent_committee_id': committee_data.get('parentCommitteeId', ''),
                'subcommittees': committee_data.get('subcommittees', []),
                'url': committee_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee(transformed_committee)
            if not is_valid:
                self.logger.error(f"Committee {committee_id} failed validation: {errors}")
                self.logger.error(f"Invalid committee data: {json.dumps(transformed_committee, indent=2)}")
                return None

            return self.validator.cleanup_committee(transformed_committee)

        except Exception as e:
            self.logger.error(f"Failed to transform committee: {str(e)}")
            self.logger.error(f"Raw committee data: {json.dumps(committee, indent=2)}")
            return None

    # Removed duplicate _process_summaries() - keeping only the most complete implementation

    # Removed duplicate _generate_summary_id() - keeping only the most complete implementation

    # Note: Nomination methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_nomination() 
    # - _generate_nomination_id()

    # Note: Bill methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_bill()
    # - _generate_bill_id()

    # Note: Amendment methods have been deduplicated  
    # Only keeping the most complete implementations of:
    # - _process_amendment()
    # - _generate_amendment_id()

    # Note: Committee methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_committee()

    # Note: Committee report methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_committee_report()
    # - _generate_committee_report_id()

    # Note: Congressional record methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_congressional_record()
    # - _generate_congressional_record_id()

    # Note: House communication methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_house_communication()
    # - _generate_house_comm_id()

    # Note: Senate communication methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_senate_communication()
    # - _generate_senate_comm_id()

    # Note: Member methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_member()
    # - _generate_member_id()

    # Note: Treaty methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_treaty()
    # - _generate_treaty_id()

    # Note: Committee print methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_committee_print()
    # - _generate_committee_print_id()

    # Note: Daily congressional record methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_daily_congressional_record()
    # - _generate_daily_congressional_record_id()

    # Note: Bound congressional record methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_bound_congressional_record()
    # - _generate_bound_congressional_record_id()
        try:
            summary_data = summary.get('summary', summary)
            
            # Generate summary ID
            summary_id = self._generate_summary_id({
                'congress': summary_data.get('congress', current_congress),
                'type': summary_data.get('type', ''),
                'number': summary_data.get('number', '')
            })

            if not summary_id:
                self.logger.error("Failed to generate summary ID")
                return None

            transformed_summary = {
                'id': summary_id,
                'type': 'summary',
                'congress': summary_data.get('congress', current_congress),
                'update_date': summary_data.get('updateDate', ''),
                'version': 1,
                'summary_type': summary_data.get('type', ''),
                'number': summary_data.get('number', ''),
                'title': summary_data.get('title', ''),
                'text': summary_data.get('text', ''),
                'url': summary_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_summary(transformed_summary)
            if not is_valid:
                self.logger.error(f"Summary {summary_id} failed validation: {errors}")
                self.logger.error(f"Invalid summary data: {json.dumps(transformed_summary, indent=2)}")
                return None

            return self.validator.cleanup_summary(transformed_summary)

        except Exception as e:
            self.logger.error(f"Failed to transform summary: {str(e)}")
            self.logger.error(f"Raw summary data: {json.dumps(summary, indent=2)}")
            return None

    def _generate_summary_id(self, summary: Dict) -> Optional[str]:
        """Generate a summary ID from summary data"""
        try:
            congress = str(summary.get('congress', ''))
            summary_type = summary.get('type', '').lower()
            number = str(summary.get('number', ''))
            
            if congress and summary_type and number:
                return f"{congress}-sum-{summary_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate summary ID: {str(e)}")
            self.logger.error(f"Summary data: {json.dumps(summary, indent=2)}")
        return None

    def _process_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a congressional record"""
        try:
            record_data = record.get('congressionalRecord', record)
            
            # Generate record ID
            record_id = self._generate_congressional_record_id({
                'congress': record_data.get('congress', current_congress),
                'chamber': record_data.get('chamber', ''),
                'date': record_data.get('date', '')
            })

            if not record_id:
                self.logger.error("Failed to generate congressional record ID")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'congressional-record',
                'congress': record_data.get('congress', current_congress),
                'update_date': record_data.get('updateDate', ''),
                'version': 1,
                'chamber': record_data.get('chamber', ''),
                'date': record_data.get('date', ''),
                'pages': record_data.get('pages', []),
                'url': record_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_congressional_record(transformed_record)
            if not is_valid:
                self.logger.error(f"Congressional record {record_id} failed validation: {errors}")
                self.logger.error(f"Invalid record data: {json.dumps(transformed_record, indent=2)}")
                return None

            return self.validator.cleanup_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform congressional record: {str(e)}")
            self.logger.error(f"Raw record data: {json.dumps(record, indent=2)}")
            return None

    def _generate_congressional_record_id(self, record: Dict) -> Optional[str]:
        """Generate a congressional record ID from record data"""
        try:
            congress = str(record.get('congress', ''))
            chamber = record.get('chamber', '').lower()
            date = record.get('date', '')
            
            if congress and chamber and date:
                date_clean = re.sub(r'[^0-9]', '', date)
                return f"{congress}-cr-{chamber}-{date_clean}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate congressional record ID: {str(e)}")
            self.logger.error(f"Record data: {json.dumps(record, indent=2)}")
        return None

    def _process_house_communication(self, comm: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house communication"""
        try:
            comm_data = comm.get('houseCommunication', comm)
            
            # Generate communication ID
            comm_id = self._generate_house_comm_id({
                'congress': comm_data.get('congress', current_congress),
                'type': comm_data.get('communicationType', ''),
                'number': comm_data.get('number', '')
            })

            if not comm_id:
                self.logger.error("Failed to generate house communication ID")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'house-communication',
                'congress': comm_data.get('congress', current_congress),
                'update_date': comm_data.get('updateDate', ''),
                'version': 1,
                'communication_type': comm_data.get('communicationType', ''),
                'number': comm_data.get('number', ''),
                'receive_date': comm_data.get('receiveDate', ''),
                'description': comm_data.get('description', ''),
                'url': comm_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_communication(transformed_comm)
            if not is_valid:
                self.logger.error(f"House communication {comm_id} failed validation: {errors}")
                self.logger.error(f"Invalid communication data: {json.dumps(transformed_comm, indent=2)}")
                return None

            return self.validator.cleanup_house_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform house communication: {str(e)}")
            self.logger.error(f"Raw communication data: {json.dumps(comm, indent=2)}")
            return None

    def _generate_house_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a house communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('type', '').lower()
            number = str(comm.get('number', ''))
            
            if congress and comm_type and number:
                return f"{congress}-hcomm-{comm_type}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate house communication ID: {str(e)}")
            self.logger.error(f"Communication data: {json.dumps(comm, indent=2)}")
        return None

    # Removed duplicate _process_member() - keeping only the most complete implementation

    # Removed duplicate _generate_member_id() - keeping only the most complete implementation

    # Note: Nomination methods have been deduplicated
    # Only keeping the most complete implementations of:
    # - _process_nomination() 
    # - _generate_nomination_id()

    def _generate_nomination_id(self, nomination: Dict) -> Optional[str]:
        """Generate a nomination ID from nomination data"""
        try:
            congress = str(nomination.get('congress', ''))
            number = str(nomination.get('number', ''))
            
            if congress and number:
                return f"{congress}-nom-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate nomination ID: {str(e)}")
            self.logger.error(f"Nomination data: {json.dumps(nomination, indent=2)}")
        return None

    def _process_hearing(self, hearing: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a hearing"""
        try:
            hearing_id = self._generate_hearing_id(hearing)
            if not hearing_id:
                self.logger.warning("Unable to generate ID for hearing")
                return None

            hearing_date = hearing.get('date', '')
            if not hearing_date:
                self.logger.warning("No date found for hearing")
                return None

            transformed_hearing = {
                'id': hearing_id,
                'type': 'hearing',
                'congress': current_congress,
                'update_date': hearing.get('updateDate', ''),
                'version': 1,
                'committee': hearing.get('committee', ''),
                'subcommittee': hearing.get('subcommittee', ''),
                'chamber': hearing.get('chamber', {}).get('name', ''),
                'date': hearing_date,
                'time': hearing.get('time', ''),
                'location': hearing.get('location', ''),
                'description': hearing.get('description', ''),
                'documents': hearing.get('documents', []),
                'url': hearing.get('url', '')
            }

            is_valid, errors = self.validator.validate_hearing(transformed_hearing)
            if not is_valid:
                self.logger.warning(f"Hearing {hearing_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_hearing(transformed_hearing)

        except Exception as e:
            self.logger.error(f"Failed to transform hearing: {str(e)}")
            return None

    def _process_summaries(self, summary: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bill summary"""
        try:
            # Debug log the input
            self.logger.debug(f"Processing summary input: {json.dumps(summary, indent=2)}")

            # Handle case where summary might not be a dict
            if not isinstance(summary, dict):
                self.logger.error(f"Invalid summary data type: {type(summary)}. Expected dict.")
                self.logger.error(f"Raw summary data: {summary}")
                return None

            # Extract the summary data from different possible structures
            summary_data = summary.get('summaries', [{}])[0] if 'summaries' in summary else summary

            # Generate summary ID
            summary_id = self._generate_summary_id({
                'congress': current_congress,
                'bill_id': summary_data.get('bill', {}).get('billId', ''),
                'version': summary_data.get('version', 1)
            })

            if not summary_id:
                self.logger.error("Failed to generate summary ID")
                return None

            # Transform to standard format
            transformed_summary = {
                'id': summary_id,
                'type': 'summary',
                'congress': current_congress,
                'update_date': summary_data.get('updateDate', ''),
                'version': summary_data.get('version', 1),
                'bill_id': summary_data.get('bill', {}).get('billId', ''),
                'text': summary_data.get('text', ''),
                'action_date': summary_data.get('actionDate', ''),
                'action_desc': summary_data.get('actionDesc', ''),
                'url': summary_data.get('url', '')
            }

            # Validate transformed data
            is_valid, errors = self.validator.validate_summary(transformed_summary)
            if not is_valid:
                self.logger.error(f"Summary {summary_id} failed validation: {errors}")
                self.logger.error(f"Invalid summary data: {json.dumps(transformed_summary, indent=2)}")
                return None

            return self.validator.cleanup_summary(transformed_summary)

        except Exception as e:
            self.logger.error(f"Failed to transform summary: {str(e)}")
            self.logger.error(f"Raw summary data: {json.dumps(summary, indent=2)}")
            return None

    def _generate_summary_id(self, summary: Dict) -> Optional[str]:
        """Generate a summary ID from summary data"""
        try:
            if not isinstance(summary, dict):
                self.logger.error(f"Summary data must be a dictionary, got {type(summary)}")
                return None

            congress = str(summary.get('congress', ''))
            bill_id = summary.get('bill_id', '')
            version = str(summary.get('version', '1'))

            if not all([congress, bill_id]):
                self.logger.warning(
                    f"Missing required fields for summary ID generation: "
                    f"congress={congress}, bill_id={bill_id}"
                )
                return None

            # Generate a deterministic ID
            summary_id = f"{congress}-{bill_id}-summary-v{version}"
            
            self.logger.debug(f"Generated summary ID: {summary_id}")
            return summary_id

        except Exception as e:
            self.logger.error(f"Failed to generate summary ID: {str(e)}")
            return None

    def _process_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a congressional record"""
        try:
            record_id = self._generate_congressional_record_id(record)
            if not record_id:
                self.logger.warning("Unable to generate ID for congressional record")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'congressional-record',
                'congress': record.get('congress', current_congress),
                'update_date': record.get('updateDate', ''),
                'version': 1,
                'record_type': record.get('recordType', ''),
                'session': record.get('session', ''),
                'issue': record.get('issue', ''),
                'pages': {
                    'start': record.get('pages', {}).get('start', ''),
                    'end': record.get('pages', {}).get('end', '')
                },
                'date': record.get('date', ''),
                'title': record.get('title', ''),
                'chamber': record.get('chamber', {}).get('name', ''),
                'url': record.get('url', '')
            }

            is_valid, errors = self.validator.validate_congressional_record(transformed_record)
            if not is_valid:
                self.logger.warning(f"Congressional record {record_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform congressional record: {str(e)}")
            return None

    def _generate_congressional_record_id(self, record: Dict) -> Optional[str]:
        """Generate a congressional record ID from record data"""
        try:
            congress = str(record.get('congress', ''))
            date = record.get('date', '').replace('-', '')
            issue = str(record.get('issue', ''))
            if congress and date and issue:
                return f"{congress}-cr-{date}-{issue}"
        except Exception as e:
            self.logger.error(f"Failed to generate congressional record ID: {str(e)}")
        return None

    def _process_house_communication(self, comm: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house communication"""
        try:
            comm_id = self._generate_house_comm_id(comm)
            if not comm_id:
                self.logger.warning("Unable to generate ID for house communication")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'house-communication',
                'congress': comm.get('congress', current_congress),
                'update_date': comm.get('updateDate', ''),
                'version': 1,
                'communication_type': comm.get('communicationType', ''),
                'number': comm.get('number', ''),
                'from_agency': comm.get('fromAgency', ''),
                'received_date': comm.get('receivedDate', ''),
                'title': comm.get('title', ''),
                'description': comm.get('description', ''),
                'referred_to': [
                    {
                        'committee': ref.get('committee', ''),
                        'date': ref.get('date', '')
                    }
                    for ref in comm.get('referredTo', [])
                ],
                'url': comm.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_communication(transformed_comm)
            if not is_valid:
                self.logger.warning(f"House communication {comm_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_house_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform house communication: {str(e)}")
            return None

    def _generate_house_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a house communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-hcomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate house communication ID: {str(e)}")
        return None

    def _generate_summary_id(self, summary: Dict) -> Optional[str]:
        """Generate a summary ID from summary data"""
        try:
            if not isinstance(summary, dict):
                self.logger.error(f"Summary data must be a dictionary, got {type(summary)}")
                return None

            congress = str(summary.get('congress', ''))
            bill_id = summary.get('bill_id', '')
            version = str(summary.get('version', '1'))

            if not all([congress, bill_id]):
                self.logger.warning(
                    f"Missing required fields for summary ID generation: "
                    f"congress={congress}, bill_id={bill_id}"
                )
                return None

            # Generate a deterministic ID
            summary_id = f"{congress}-{bill_id}-summary-v{version}"
            
            self.logger.debug(f"Generated summary ID: {summary_id}")
            return summary_id

        except Exception as e:
            self.logger.error(f"Failed to generate summary ID: {str(e)}")
            return None


    # NOTE: All endpoint processor methods have been consolidated.
    # The following methods have been deduplicated and only the first occurrence is kept
    # at the top of this class. Each method should have a single implementation.
    # Do not add duplicate method definitions below this point.
    
    # Consolidated methods (with their ID generators):
    # - _process_amendment() and _generate_amendment_id()
    # - _process_bill() and _generate_bill_id()
    # - _process_nomination() and _generate_nomination_id()
    # - _process_treaty() and _generate_treaty_id()
    # - _process_committee_report() and _generate_committee_report_id()
    # - _process_congressional_record() and _generate_congressional_record_id()
    # - _process_house_communication() and _generate_house_comm_id()
    # - _process_senate_communication() and _generate_senate_comm_id()
    # - _process_committee_meeting() and _generate_meeting_id()
    # - _process_member() and _generate_member_id()
    # - _process_summaries() and _generate_summary_id()
    # - _process_bound_congressional_record() and _generate_bound_record_id()
    # - _process_daily_congressional_record() and _generate_daily_record_id()
    # - _process_hearing() and _generate_hearing_id()
    # - _process_house_requirement() and _generate_house_req_id()
    # - _process_committee_print() and _generate_print_id()

    # Any code below this point that redefines these methods should be removed

    def _generate_hearing_id(self, hearing: Dict) -> Optional[str]:
        """Generate a hearing ID from hearing data"""
        try:
            if not isinstance(hearing, dict):
                self.logger.error(f"Hearing data must be a dictionary, got {type(hearing)}")
                return None

            congress = str(hearing.get('congress', ''))
            date = hearing.get('date', '').replace('-', '')
            committee = re.sub(r'[^a-zA-Z0-9]', '', hearing.get('committee', ''))[:20]

            if not all([congress, date, committee]):
                self.logger.warning(
                    f"Missing required fields for hearing ID generation: "
                    f"congress={congress}, date={date}, committee={committee}"
                )
                return None

            return f"{congress}-hear-{date}-{committee}"

        except Exception as e:
            self.logger.error(f"Failed to generate hearing ID: {str(e)}")
            return None

    def _generate_committee_print_id(self, print_data: Dict) -> Optional[str]:
        """Generate a committee print ID from print data"""
        try:
            if not isinstance(print_data, dict):
                self.logger.error(f"Committee print data must be a dictionary, got {type(print_data)}")
                return None

            congress = str(print_data.get('congress', ''))
            chamber = print_data.get('chamber', '').lower()
            jacket_number = print_data.get('jacket_number', '')

            if not all([congress, chamber, jacket_number]):
                self.logger.warning(
                    f"Missing required fields for committee print ID generation: "
                    f"congress={congress}, chamber={chamber}, "
                    f"jacket_number={jacket_number}"
                )
                return None

            return f"{congress}-print-{chamber}-{jacket_number}"

        except Exception as e:
            self.logger.error(f"Failed to generate committee print ID: {str(e)}")
            self.logger.debug(f"Print data that caused error: {json.dumps(print_data, indent=2)}")
            return None

    # NOTE: All endpoint processor methods have been consolidated at the top of the class
    # Duplicate declarations have been removed. The implementation for each method
    # should be kept in a single place to avoid confusion and maintenance issues.
    # See the top of this class for the correct implementations of:
    # - _process_bill() and _generate_bill_id()
    # - _process_amendment() and _generate_amendment_id()
    # - _process_nomination() and _generate_nomination_id()
    # - _process_treaty() and _generate_treaty_id() 
    # - _process_committee_report() and _generate_committee_report_id()
    # - _process_congressional_record() and _generate_congressional_record_id()
    # - _process_house_communication() and _generate_house_comm_id()
    # - _process_senate_communication() and _generate_senate_comm_id()
    # - _process_committee_meeting() and _generate_meeting_id()
    # - _process_member() and _generate_member_id()
    # - _process_summaries() and _generate_summary_id()

    def _process_house_requirement(self, req: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house requirement"""
        try:
            req_id = self._generate_house_req_id(req)
            if not req_id:
                self.logger.warning("Unable to generate ID for house requirement")
                return None

            transformed_req = {
                'id': req_id,
                'type': 'house-requirement',
                'congress': req.get('congress', current_congress),
                'update_date': req.get('updateDate', ''),
                'version': 1,
                'requirement_type': req.get('type', ''),
                'title': req.get('title', ''),
                'description': req.get('description', ''),
                'date': req.get('date', ''),
                'submitted_by': req.get('submittedBy', ''),
                'status': req.get('status', ''),
                'documents': req.get('documents', []),
                'url': req.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_requirement(transformed_req)
            if not is_valid:
                self.logger.warning(f"House requirement {req_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_house_requirement(transformed_req)

        except Exception as e:
            self.logger.error(f"Failed to transform house requirement: {str(e)}")
            return None

    def _generate_house_req_id(self, req: Dict) -> Optional[str]:
        """Generate a house requirement ID from requirement data"""
        try:
            congress = str(req.get('congress', ''))
            req_type = req.get('type', '').lower()
            date = req.get('date', '').replace('-', '')
            if congress and req_type and date:
                return f"{congress}-hreq-{req_type}-{date}"
        except Exception as e:
            self.logger.error(f"Failed to generate house requirement ID: {str(e)}")
        return None

    def _process_senate_communication(self, comm: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a senate communication"""
        try:
            comm_id = self._generate_senate_comm_id(comm)
            if not comm_id:
                self.logger.warning("Unable to generate ID for senate communication")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'senate-communication',
                'congress': comm.get('congress', current_congress),
                'update_date': comm.get('updateDate', ''),
                'version': 1,
                'communication_type': comm.get('communicationType', ''),
                'number': comm.get('number', ''),
                'from_agency': comm.get('fromAgency', ''),
                'received_date': comm.get('receivedDate', ''),
                'title': comm.get('title', ''),
                'description': comm.get('description', ''),
                'referred_to': [
                    {
                        'committee': ref.get('committee', ''),
                        'date': ref.get('date', '')
                    }
                    for ref in comm.get('referredTo', [])
                ],
                'url': comm.get('url', '')
            }

            is_valid, errors = self.validator.validate_senate_communication(transformed_comm)
            if not is_valid:
                self.logger.warning(f"Senate communication {comm_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_senate_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform senate communication: {str(e)}")
            return None

    def _process_committee_print(self, print_data: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee print"""
        try:
            if not isinstance(print_data, dict):
                self.logger.error(f"Invalid committee print data type: {type(print_data)}")
                return None

            # Extract required fields
            committee_print = print_data.get('committee-print', print_data)
            
            if not isinstance(committee_print, dict):
                self.logger.error("Invalid committee print structure")
                return None

            # Generate ID
            print_id = self._generate_committee_print_id(committee_print)
            if not print_id:
                self.logger.warning("Unable to generate ID for committee print")
                return None

            transformed_print = {
                'id': print_id,
                'type': 'committee-print',
                'congress': current_congress,
                'update_date': committee_print.get('updateDate', ''),
                'version': 1,
                'number': committee_print.get('number', ''),
                'title': committee_print.get('title', ''),
                'committee': committee_print.get('committee', ''),
                'chamber': committee_print.get('chamber', ''),
                'url': committee_print.get('url', '')
            }

            # TODO: Add validation once schema is defined
            return transformed_print

        except Exception as e:
            self.logger.error(f"Failed to transform committee print: {str(e)}")
            return None

    def _process_daily_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a daily congressional record"""
        try:
            record_data = record.get('dailyCongressionalRecord', record)
            if isinstance(record_data, str):
                self.logger.error(f"Received string instead of dictionary for daily record: {record_data}")
                return None

            # Extract required fields with proper error handling
            congress = int(record_data.get('congress', current_congress))
            volume = record_data.get('volume', '')
            issue = record_data.get('issue', '')
            date = record_data.get('date', '')
            
            # Generate record ID
            record_id = self._generate_daily_record_id({
                'congress': congress,
                'volume': volume,
                'issue': issue,
                'date': date
            })

            if not record_id:
                self.logger.error("Failed to generate daily congressional record ID - missing required fields")
                self.logger.error(f"Raw record data: {json.dumps(record_data, indent=2)}")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'daily-congressional-record',
                'congress': congress,
                'update_date': record_data.get('updateDate', ''),
                'version': 1,
                'volume': volume,
                'issue': issue,
                'date': date,
                'chamber': record_data.get('chamber', '').lower(),
                'pages': record_data.get('pages', []),
                'url': record_data.get('url', '')
            }

            # Log the transformed data for debugging
            self.logger.debug(f"Transformed daily record data: {json.dumps(transformed_record, indent=2)}")

            is_valid, errors = self.validator.validate_daily_congressional_record(transformed_record)
            if not is_valid:
                self.logger.error(f"Daily congressional record {record_id} failed validation: {errors}")
                self.logger.error(f"Invalid record data: {json.dumps(transformed_record, indent=2)}")
                return None

            return self.validator.cleanup_daily_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform daily congressional record: {str(e)}")
            self.logger.error(f"Raw record data: {json.dumps(record, indent=2)}")
            return None

    """
    NOTE: The following methods have been consolidated at the top of the class.
    Only the first occurrence of each method is kept, all duplicates are removed:

    - _process_bill() and _generate_bill_id()
    - _process_amendment() and _generate_amendment_id()
    - _process_nomination() and _generate_nomination_id() 
    - _process_treaty() and _generate_treaty_id()
    - _process_committee_report() and _generate_committee_report_id()
    - _process_congressional_record() and _generate_congressional_record_id()
    - _process_house_communication() and _generate_house_comm_id()
    - _process_senate_communication() and _generate_senate_comm_id()
    - _process_committee_meeting() and _generate_meeting_id()
    - _process_member() and _generate_member_id()
    - _process_summaries() and _generate_summary_id()

    See the first occurrence of each method for the canonical implementation.
    """

    def _process_bound_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bound congressional record"""
        try:
            record_data = record.get('boundCongressionalRecord', record)
            if isinstance(record_data, str):
                self.logger.error(f"Received string instead of dictionary for bound record: {record_data}")
                return None

            # Extract required fields with proper error handling
            congress = int(record_data.get('congress', current_congress))
            date = record_data.get('date', '')
            session_number = record_data.get('sessionNumber', '')
            volume_number = record_data.get('volumeNumber', '')
            
            # Generate record ID
            record_id = self._generate_bound_record_id({
                'congress': congress,
                'date': date,
                'session_number': session_number,
                'volume_number': volume_number
            })

            if not record_id:
                self.logger.error("Failed to generate bound record ID - missing required fields")
                self.logger.error(f"Raw record data: {json.dumps(record_data, indent=2)}")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'bound-congressional-record',
                'congress': congress,
                'update_date': record_data.get('updateDate', ''),
                'version': 1,
                'date': date,
                'session_number': session_number,
                'volume_number': volume_number,
                'url': record_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_bound_record(transformed_record)
            if not is_valid:
                self.logger.error(f"Bound record {record_id} failed validation: {errors}")
                self.logger.error(f"Invalid record data: {json.dumps(transformed_record, indent=2)}")
                return None

            return self.validator.cleanup_bound_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform bound record: {str(e)}")
            self.logger.error(f"Raw record data: {json.dumps(record, indent=2)}")
            return None

    def _generate_bound_record_id(self, record: Dict) -> Optional[str]:
        """Generate a bound congressional record ID from record data"""
        try:
            congress = str(record.get('congress', ''))
            date = record.get('date', '')
            session_number = str(record.get('session_number', ''))
            volume_number = str(record.get('volume_number', ''))
            
            if not all([congress, date, session_number, volume_number]):
                missing_fields = []
                if not congress: missing_fields.append('congress')
                if not date: missing_fields.append('date')
                if not session_number: missing_fields.append('session_number')
                if not volume_number: missing_fields.append('volume_number')
                self.logger.warning(f"Missing required fields for bound record ID generation: {', '.join(missing_fields)}")
                return None
            
            # Clean date to remove non-numeric characters
            date_clean = re.sub(r'[^0-9]', '', date)
            
            # Generate final ID
            record_id = f"{congress}-{date_clean}-s{session_number}-v{volume_number}"
            self.logger.debug(f"Generated bound record ID: {record_id}")
            return record_id
                
        except Exception as e:
            self.logger.error(f"Failed to generate bound record ID: {str(e)}")
            self.logger.error(f"Raw record data: {json.dumps(record, indent=2)}")
            return None

    def _process_member(self, member: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a member"""
        try:
            if not isinstance(member, dict):
                self.logger.error(f"Invalid member data type: {type(member)}")
                return None

            member_data = member.get('member', member)
            
            member_id = self._generate_member_id(member_data)
            if not member_id:
                self.logger.warning("Unable to generate ID for member")
                return None

            transformed_member = {
                'id': member_id,
                'type': 'member',
                'congress': member_data.get('congress', current_congress),
                'update_date': member_data.get('updateDate', ''),
                'version': 1,
                'bioguide_id': member_data.get('bioguideId', ''),
                'first_name': member_data.get('firstName', ''),
                'last_name': member_data.get('lastName', ''),
                'state': member_data.get('state', ''),
                'district': member_data.get('district', ''),
                'party': member_data.get('party', ''),
                'chamber': member_data.get('chamber', {}).get('name', ''),
                'leadership_role': member_data.get('leadershipRole', ''),
                'served_until': member_data.get('servedUntil', ''),
                'url': member_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_member(transformed_member)
            if not is_valid:
                self.logger.warning(f"Member {member_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_member(transformed_member)

        except Exception as e:
            self.logger.error(f"Failed to transform member: {str(e)}")
            return None

    def _generate_member_id(self, member: Dict) -> Optional[str]:
        """Generate a member ID from member data"""
        try:
            congress = str(member.get('congress', ''))
            bioguide_id = member.get('bioguideId', '')
            if congress and bioguide_id:
                return f"{congress}-mem-{bioguide_id}"
        except Exception as e:
            self.logger.error(f"Failed to generate member ID: {str(e)}")
        return None

    def _process_summaries(self, summary: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a summary"""
        try:
            summary_id = self._generate_summary_id(summary)
            if not summary_id:
                self.logger.warning("Unable to generate ID for summary")
                return None

            transformed_summary = {
                'id': summary_id,
                'type': 'summary',
                'congress': summary.get('congress', current_congress),
                'update_date': summary.get('updateDate', ''),
                'version': 1,
                'bill_id': summary.get('billId', ''),
                'bill_type': summary.get('billType', ''),
                'bill_number': summary.get('billNumber', ''),
                'action_date': summary.get('actionDate', ''),
                'action_desc': summary.get('actionDesc', ''),
                'text': summary.get('text', ''),
                'url': summary.get('url', '')
            }

            is_valid, errors = self.validator.validate_summary(transformed_summary)
            if not is_valid:
                self.logger.warning(f"Summary {summary_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_summary(transformed_summary)

        except Exception as e:
            self.logger.error(f"Failed to transform summary: {str(e)}")
            self.logger.error(f"Problematic summary data: {json.dumps(summary, indent=2)}")
            return None

    def _process_bill(self, bill: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bill"""
        try:
            # Handle case where bill is a string
            if isinstance(bill, str):
                self.logger.warning(f"Received string instead of dictionary for bill: {bill}")
                return None

            # Get the basic bill information with detailed logging
            bill_data = bill.get('bill', bill)
            
            bill_id = bill_data.get('billId') or self._generate_bill_id(bill_data)
            if not bill_id:
                self.logger.warning("Unable to generate ID for bill")
                self.logger.debug(f"Bill data that failed ID generation: {json.dumps(bill_data, indent=2)}")
                return None

            transformed_bill = {
                'id': bill_id,
                'type': 'bill',
                'congress': bill_data.get('congress', current_congress),
                'title': bill_data.get('title', ''),
                'update_date': bill_data.get('updateDate', ''),
                'bill_type': bill_data.get('type', ''),
                'bill_number': bill_data.get('number', ''),
                'version': 1,
                'origin_chamber': bill_data.get('originChamber', {}).get('name', ''),
                'origin_chamber_code': bill_data.get('originChamberCode', ''),
                'latest_action': {
                    'text': bill_data.get('latestAction', {}).get('text', ''),
                    'action_date': bill_data.get('latestAction', {}).get('actionDate', ''),
                },
                'update_date_including_text': bill_data.get('updateDateIncludingText', ''),
                'introduced_date': bill_data.get('introducedDate', ''),
                'sponsors': bill_data.get('sponsors', []),
                'committees': bill_data.get('committees', []),
                'url': bill_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_bill(transformed_bill)
            if not is_valid:
                self.logger.warning(f"Bill {bill_id} failed validation: {errors}")
                self.logger.debug(f"Invalid bill data: {json.dumps(transformed_bill, indent=2)}")
                return None

            return self.validator.cleanup_bill(transformed_bill)

        except Exception as e:
            self.logger.error(f"Failed to transform bill: {str(e)}")
            self.logger.error(f"Problematic bill data: {json.dumps(bill, indent=2)}")
            return None

    def _generate_summary_id(self, summary: Dict) -> Optional[str]:
        """Generate a summary ID from summary data"""
        try:
            congress = str(summary.get('congress', ''))
            bill_type = summary.get('billType', '').lower()
            bill_number = str(summary.get('billNumber', ''))
            action_date = summary.get('actionDate', '').replace('-', '')
            if congress and bill_type and bill_number and action_date:
                return f"{congress}-sum-{bill_type}{bill_number}-{action_date}"
        except Exception as e:
            self.logger.error(f"Failed to generate summary ID: {str(e)}")
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
                'version': 1,
                'position': nomination.get('position', ''),
                'nominee': nomination.get('nominee', ''),
                'organization': nomination.get('organization', ''),
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

    def _process_committee_report(self, report: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee report"""
        try:
            report_id = self._generate_committee_report_id(report)
            if not report_id:
                self.logger.warning("Unable to generate ID for committee report")
                return None

            transformed_report = {
                'id': report_id,
                'type': 'committee-report',
                'congress': report.get('congress', current_congress),
                'report_number': report.get('number'),
                'update_date': report.get('updateDate', ''),
                'committee': report.get('committee', ''),
                'chamber': report.get('chamber', {}).get('name', ''),
                'version': 1,
                'report_type': report.get('reportType', ''),
                'title': report.get('title', ''),
                'associated_bill': {
                    'congress': report.get('associatedBill', {}).get('congress', ''),
                    'type': report.get('associatedBill', {}).get('type', ''),
                    'number': report.get('associatedBill', {}).get('number', '')
                },
                'latest_action': {
                    'text': report.get('latestAction', {}).get('text', ''),
                    'action_date': report.get('latestAction', {}).get('actionDate', ''),
                },
                'url': report.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee_report(transformed_report)
            if not is_valid:
                self.logger.warning(f"Committee report {report_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_committee_report(transformed_report)

        except Exception as e:
            self.logger.error(f"Failed to transform committee report: {str(e)}")
            return None

    def _generate_committee_report_id(self, report: Dict) -> Optional[str]:
        """Generate a committee report ID from report data"""
        try:
            congress = str(report.get('congress', ''))
            report_type = report.get('reportType', '').lower()
            report_number = str(report.get('number', ''))
            if congress and report_type and report_number:
                return f"{congress}-{report_type}-{report_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate committee report ID: {str(e)}")
        return None

    def _process_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a congressional record"""
        try:
            record_id = self._generate_congressional_record_id(record)
            if not record_id:
                self.logger.warning("Unable to generate ID for congressional record")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'congressional-record',
                'congress': record.get('congress', current_congress),
                'update_date': record.get('updateDate', ''),
                'version': 1,
                'record_type': record.get('recordType', ''),
                'session': record.get('session', ''),
                'issue': record.get('issue', ''),
                'pages': {
                    'start': record.get('pages', {}).get('start', ''),
                    'end': record.get('pages', {}).get('end', '')
                },
                'date': record.get('date', ''),
                'title': record.get('title', ''),
                'chamber': record.get('chamber', {}).get('name', ''),
                'url': record.get('url', '')
            }

            is_valid, errors = self.validator.validate_congressional_record(transformed_record)
            if not is_valid:
                self.logger.warning(f"Congressional record {record_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform congressional record: {str(e)}")
            return None

    def _generate_congressional_record_id(self, record: Dict) -> Optional[str]:
        """Generate a congressional record ID from record data"""
        try:
            congress = str(record.get('congress', ''))
            date = record.get('date', '').replace('-', '')
            issue = str(record.get('issue', ''))
            if congress and date and issue:
                return f"{congress}-cr-{date}-{issue}"
        except Exception as e:
            self.logger.error(f"Failed to generate congressional record ID: {str(e)}")
        return None

    def _process_house_communication(self, comm: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house communication"""
        try:
            comm_id = self._generate_house_comm_id(comm)
            if not comm_id:
                self.logger.warning("Unable to generate ID for house communication")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'house-communication',
                'congress': comm.get('congress', current_congress),
                'update_date': comm.get('updateDate', ''),
                'version': 1,
                'communication_type': comm.get('communicationType', ''),
                'number': comm.get('number', ''),
                'from_agency': comm.get('fromAgency', ''),
                'received_date': comm.get('receivedDate', ''),
                'title': comm.get('title', ''),
                'description': comm.get('description', ''),
                'referred_to': [
                    {
                        'committee': ref.get('committee', ''),
                        'date': ref.get('date', '')
                    }
                    for ref in comm.get('referredTo', [])
                ],
                'url': comm.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_communication(transformed_comm)
            if not is_valid:
                self.logger.warning(f"House communication {comm_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_house_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform house communication: {str(e)}")
            return None

    def _generate_house_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a house communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-hcomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate house communication ID: {str(e)}")
        return None

    def _generate_senate_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a senate communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-scomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate senate communication ID: {str(e)}")
        return None

    def _process_member(self, member: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a member"""
        try:
            member_id = self._generate_member_id(member)
            if not member_id:
                self.logger.warning("Unable to generate ID for member")
                return None

            transformed_member = {
                'id': member_id,
                'type': 'member',
                'congress': member.get('congress', current_congress),
                'update_date': member.get('updateDate', ''),
                'version': 1,
                'bioguide_id': member.get('bioguideId', ''),
                'first_name': member.get('firstName', ''),
                'last_name': member.get('lastName', ''),
                'state': member.get('state', ''),
                'district': member.get('district', ''),
                'party': member.get('party', ''),
                'chamber': member.get('chamber', {}).get('name', ''),
                'leadership_role': member.get('leadershipRole', ''),
                'served_until': member.get('servedUntil', ''),
                'url': member.get('url', '')
            }

            is_valid, errors = self.validator.validate_member(transformed_member)
            if not is_valid:
                self.logger.warning(f"Member {member_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_member(transformed_member)

        except Exception as e:
            self.logger.error(f"Failed to transform member: {str(e)}")
            return None

    def _generate_member_id(self, member: Dict) -> Optional[str]:
        """Generate a member ID from member data"""
        try:
            congress = str(member.get('congress', ''))
            bioguide_id = member.get('bioguideId', '')
            if congress and bioguide_id:
                return f"{congress}-mem-{bioguide_id}"
        except Exception as e:
            self.logger.error(f"Failed to generate member ID: {str(e)}")
        return None

    def _process_summaries(self, summary: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a summary"""
        try:
            summary_id = self._generate_summary_id(summary)
            if not summary_id:
                self.logger.warning("Unable to generate ID for summary")
                return None

            transformed_summary = {
                'id': summary_id,
                'type': 'summary',
                'congress': summary.get('congress', current_congress),
                'update_date': summary.get('updateDate', ''),
                'version': 1,
                'bill_id': summary.get('billId', ''),
                'bill_type': summary.get('billType', ''),
                'bill_number': summary.get('billNumber', ''),
                'action_date': summary.get('actionDate', ''),
                'action_desc': summary.get('actionDesc', ''),
                'text': summary.get('text', ''),
                'url': summary.get('url', '')
            }

            is_valid, errors = self.validator.validate_summary(transformed_summary)
            if not is_valid:
                self.logger.warning(f"Summary {summary_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_summary(transformed_summary)

        except Exception as e:
            self.logger.error(f"Failed to transform summary: {str(e)}")
            return None

    def _generate_summary_id(self, summary: Dict) -> Optional[str]:
        """Generate a summary ID from summary data"""
        try:
            congress = str(summary.get('congress', ''))
            bill_type = summary.get('billType', '').lower()
            bill_number = str(summary.get('billNumber', ''))
            action_date = summary.get('actionDate', '').replace('-', '')
            if congress and bill_type and bill_number and action_date:
                return f"{congress}-sum-{bill_type}{bill_number}-{action_date}"
        except Exception as e:
            self.logger.error(f"Failed to generate summary ID: {str(e)}")
        return None

    # Congressional Record and Committee Report related methods below
    def _process_committee_report(self, report: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee report"""
        try:
            report_id = self._generate_committee_report_id(report)
            if not report_id:
                self.logger.warning("Unable to generate ID for committee report")
                return None

            transformed_report = {
                'id': report_id,
                'type': 'committee-report',
                'congress': report.get('congress', current_congress),
                'report_number': report.get('number'),
                'update_date': report.get('updateDate', ''),
                'committee': report.get('committee', ''),
                'chamber': report.get('chamber', {}).get('name', ''),
                'version': 1,
                'report_type': report.get('reportType', ''),
                'title': report.get('title', ''),
                'associated_bill': {
                    'congress': report.get('associatedBill', {}).get('congress', ''),
                    'type': report.get('associatedBill', {}).get('type', ''),
                    'number': report.get('associatedBill', {}).get('number', '')
                },
                'latest_action': {
                    'text': report.get('latestAction', {}).get('text', ''),
                    'action_date': report.get('latestAction', {}).get('actionDate', ''),
                },
                'url': report.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee_report(transformed_report)
            if not is_valid:
                self.logger.warning(f"Committee report {report_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_committee_report(transformed_report)

        except Exception as e:
            self.logger.error(f"Failed to transform committee report: {str(e)}")
            return None

    def _generate_committee_report_id(self, report: Dict) -> Optional[str]:
        """Generate a committee report ID from report data"""
        try:
            congress = str(report.get('congress', ''))
            report_type = report.get('reportType', '').lower()
            report_number = str(report.get('number', ''))
            if congress and report_type and report_number:
                return f"{congress}-{report_type}-{report_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate committee report ID: {str(e)}")
        return None

    def _process_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a congressional record"""
        try:
            record_id = self._generate_congressional_record_id(record)
            if not record_id:
                self.logger.warning("Unable to generate ID for congressional record")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'congressional-record',
                'congress': record.get('congress', current_congress),
                'update_date': record.get('updateDate', ''),
                'version': 1,
                'record_type': record.get('recordType', ''),
                'session': record.get('session', ''),
                'issue': record.get('issue', ''),
                'pages': {
                    'start': record.get('pages', {}).get('start', ''),
                    'end': record.get('pages', {}).get('end', '')
                },
                'date': record.get('date', ''),
                'title': record.get('title', ''),
                'chamber': record.get('chamber', {}).get('name', ''),
                'url': record.get('url', '')
            }

            is_valid, errors = self.validator.validate_congressional_record(transformed_record)
            if not is_valid:
                self.logger.warning(f"Congressional record {record_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform congressional record: {str(e)}")
            return None

    def _generate_congressional_record_id(self, record: Dict) -> Optional[str]:
        """Generate a congressional record ID from record data"""
        try:
            congress = str(record.get('congress', ''))
            date = record.get('date', '').replace('-', '')
            issue = str(record.get('issue', ''))
            if congress and date and issue:
                return f"{congress}-cr-{date}-{issue}"
        except Exception as e:
            self.logger.error(f"Failed to generate congressional record ID: {str(e)}")
        return None

    def _process_house_communication(self, comm: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house communication"""
        try:
            comm_id = self._generate_house_comm_id(comm)
            if not comm_id:
                self.logger.warning("Unable to generate ID for house communication")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'house-communication',
                'congress': comm.get('congress', current_congress),
                'update_date': comm.get('updateDate', ''),
                'version': 1,
                'communication_type': comm.get('communicationType', ''),
                'number': comm.get('number', ''),
                'from_agency': comm.get('fromAgency', ''),
                'received_date': comm.get('receivedDate', ''),
                'title': comm.get('title', ''),
                'description': comm.get('description', ''),
                'referred_to': [
                    {
                        'committee': ref.get('committee', ''),
                        'date': ref.get('date', '')
                    }
                    for ref in comm.get('referredTo', [])
                ],
                'url': comm.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_communication(transformed_comm)
            if not is_valid:
                self.logger.warning(f"House communication {comm_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_house_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform house communication: {str(e)}")
            return None

    def _generate_house_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a house communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-hcomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate house communication ID: {str(e)}")
        return None

    def _generate_senate_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a senate communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-scomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate senate communication ID: {str(e)}")
        return None

    # NOTE: Duplicate implementations have been removed.
    # The following methods have been consolidated at the top of the class:
    # - _process_bill() and _generate_bill_id()
    # - _process_amendment() and _generate_amendment_id()
    # - _process_nomination() and _generate_nomination_id()
    # - _process_treaty() and _generate_treaty_id()
    # - _process_committee_report() and _generate_committee_report_id()
    # - _process_congressional_record() and _generate_congressional_record_id()
    # - _process_house_communication() and _generate_house_comm_id()
    # - _process_senate_communication() and _generate_senate_comm_id()
    # - _process_member() and _generate_member_id()
    # - _process_summaries() and _generate_summary_id()
    # 
    # All duplicate implementations have been removed. See the top of this class
    # for the canonical implementation of each method.

    def _process_daily_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a daily congressional record"""
        try:
            if not isinstance(record, dict):
                self.logger.error(f"Invalid record data type: {type(record)}")
                return None

            # Use the existing congressional record processor with type-specific modifications
            transformed_record = self._process_congressional_record(record, current_congress)
            if transformed_record:
                transformed_record['record_type'] = 'daily'
            return transformed_record

        except Exception as e:
            self.logger.error(f"Failed to transform daily congressional record: {str(e)}")
            return None

    def _process_bound_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a bound congressional record"""
        try:
            if not isinstance(record, dict):
                self.logger.error(f"Invalid record data type: {type(record)}")
                return None

            # Use the existing congressional record processor with type-specific modifications 
            transformed_record = self._process_congressional_record(record, current_congress)
            if transformed_record:
                transformed_record['record_type'] = 'bound'
            return transformed_record

        except Exception as e:
            self.logger.error(f"Failed to transform bound congressional record: {str(e)}")
            return None

    def _process_preliminary_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a preliminary congressional record"""
        try:
            if not isinstance(record, dict):
                self.logger.error(f"Invalid record data type: {type(record)}")
                return None

            # Use the existing congressional record processor with type-specific modifications
            transformed_record = self._process_congressional_record(record, current_congress)
            if transformed_record:
                transformed_record['record_type'] = 'preliminary'
            return transformed_record

        except Exception as e:
            self.logger.error(f"Failed to transform preliminary congressional record: {str(e)}")
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
                'version': 1,
                'position': nomination.get('position', ''),
                'nominee': nomination.get('nominee', ''),
                'organization': nomination.get('organization', ''),
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

    def _process_committee_report(self, report: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee report"""
        try:
            report_id = self._generate_committee_report_id(report)
            if not report_id:
                self.logger.warning("Unable to generate ID for committee report")
                return None

            transformed_report = {
                'id': report_id,
                'type': 'committee-report',
                'congress': report.get('congress', current_congress),
                'report_number': report.get('number'),
                'update_date': report.get('updateDate', ''),
                'committee': report.get('committee', ''),
                'chamber': report.get('chamber', {}).get('name', ''),
                'version': 1,
                'report_type': report.get('reportType', ''),
                'title': report.get('title', ''),
                'associated_bill': {
                    'congress': report.get('associatedBill', {}).get('congress', ''),
                    'type': report.get('associatedBill', {}).get('type', ''),
                    'number': report.get('associatedBill', {}).get('number', '')
                },
                'latest_action': {
                    'text': report.get('latestAction', {}).get('text', ''),
                    'action_date': report.get('latestAction', {}).get('actionDate', ''),
                },
                'url': report.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee_report(transformed_report)
            if not is_valid:
                self.logger.warning(f"Committee report {report_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_committee_report(transformed_report)

        except Exception as e:
            self.logger.error(f"Failed to transform committee report: {str(e)}")
            return None

    def _generate_committee_report_id(self, report: Dict) -> Optional[str]:
        """Generate a committee report ID from report data"""
        try:
            congress = str(report.get('congress', ''))
            report_type = report.get('reportType', '').lower()
            report_number = str(report.get('number', ''))
            if congress and report_type and report_number:
                return f"{congress}-{report_type}-{report_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate committee report ID: {str(e)}")
        return None

    def _process_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a congressional record"""
        try:
            record_id = self._generate_congressional_record_id(record)
            if not record_id:
                self.logger.warning("Unable to generate ID for congressional record")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'congressional-record',
                'congress': record.get('congress', current_congress),
                'update_date': record.get('updateDate', ''),
                'version': 1,
                'record_type': record.get('recordType', ''),
                'session': record.get('session', ''),
                'issue': record.get('issue', ''),
                'pages': {
                    'start': record.get('pages', {}).get('start', ''),
                    'end': record.get('pages', {}).get('end', '')
                },
                'date': record.get('date', ''),
                'title': record.get('title', ''),
                'chamber': record.get('chamber', {}).get('name', ''),
                'url': record.get('url', '')
            }

            is_valid, errors = self.validator.validate_congressional_record(transformed_record)
            if not is_valid:
                self.logger.warning(f"Congressional record {record_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform congressional record: {str(e)}")
            return None

    def _generate_congressional_record_id(self, record: Dict) -> Optional[str]:
        """Generate a congressional record ID from record data"""
        try:
            congress = str(record.get('congress', ''))
            date = record.get('date', '').replace('-', '')
            issue = str(record.get('issue', ''))
            if congress and date and issue:
                return f"{congress}-cr-{date}-{issue}"
        except Exception as e:
            self.logger.error(f"Failed to generate congressional record ID: {str(e)}")
        return None

    def _process_house_communication(self, comm: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house communication"""
        try:
            comm_id = self._generate_house_comm_id(comm)
            if not comm_id:
                self.logger.warning("Unable to generate ID for house communication")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'house-communication',
                'congress': comm.get('congress', current_congress),
                'update_date': comm.get('updateDate', ''),
                'version': 1,
                'communication_type': comm.get('communicationType', ''),
                'number': comm.get('number', ''),
                'from_agency': comm.get('fromAgency', ''),
                'received_date': comm.get('receivedDate', ''),
                'title': comm.get('title', ''),
                'description': comm.get('description', ''),
                'referred_to': [
                    {
                        'committee': ref.get('committee', ''),
                        'date': ref.get('date', '')
                    }
                    for ref in comm.get('referredTo', [])
                ],
                'url': comm.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_communication(transformed_comm)
            if not is_valid:
                self.logger.warning(f"House communication {comm_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_house_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform house communication: {str(e)}")
            return None

    def _generate_house_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a house communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-hcomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate house communication ID: {str(e)}")
        return None

    def _generate_senate_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a senate communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-scomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate senate communication ID: {str(e)}")
        return None

    def _process_member(self, member: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a member"""
        try:
            member_id = self._generate_member_id(member)
            if not member_id:
                self.logger.warning("Unable to generate ID for member")
                return None

            transformed_member = {
                'id': member_id,
                'type': 'member',
                'congress': member.get('congress', current_congress),
                'update_date': member.get('updateDate', ''),
                'version': 1,
                'bioguide_id': member.get('bioguideId', ''),
                'first_name': member.get('firstName', ''),
                'last_name': member.get('lastName', ''),
                'state': member.get('state', ''),
                'district': member.get('district', ''),
                'party': member.get('party', ''),
                'chamber': member.get('chamber', {}).get('name', ''),
                'leadership_role': member.get('leadershipRole', ''),
                'served_until': member.get('servedUntil', ''),
                'url': member.get('url', '')
            }

            is_valid, errors = self.validator.validate_member(transformed_member)
            if not is_valid:
                self.logger.warning(f"Member {member_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_member(transformed_member)

        except Exception as e:
            self.logger.error(f"Failed to transform member: {str(e)}")
            return None

    def _generate_member_id(self, member: Dict) -> Optional[str]:
        """Generate a member ID from member data"""
        try:
            congress = str(member.get('congress', ''))
            bioguide_id = member.get('bioguideId', '')
            if congress and bioguide_id:
                return f"{congress}-mem-{bioguide_id}"
        except Exception as e:
            self.logger.error(f"Failed to generate member ID: {str(e)}")
        return None

    def _process_summaries(self, summary: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a summary"""
        try:
            summary_id = self._generate_summary_id(summary)
            if not summary_id:
                self.logger.warning("Unable to generate ID for summary")
                return None

            transformed_summary = {
                'id': summary_id,
                'type': 'summary',
                'congress': summary.get('congress', current_congress),
                'update_date': summary.get('updateDate', ''),
                'version': 1,
                'bill_id': summary.get('billId', ''),
                'bill_type': summary.get('billType', ''),
                'bill_number': summary.get('billNumber', ''),
                'action_date': summary.get('actionDate', ''),
                'action_desc': summary.get('actionDesc', ''),
                'text': summary.get('text', ''),
                'url': summary.get('url', '')
            }

            is_valid, errors = self.validator.validate_summary(transformed_summary)
            if not is_valid:
                self.logger.warning(f"Summary {summary_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_summary(transformed_summary)

        except Exception as e:
            self.logger.error(f"Failed to transform summary: {str(e)}")
            return None

    def _generate_summary_id(self, summary: Dict) -> Optional[str]:
        """Generate a summary ID from summary data"""
        try:
            congress = str(summary.get('congress', ''))
            bill_type = summary.get('billType', '').lower()
            bill_number = str(summary.get('billNumber', ''))
            action_date = summary.get('actionDate', '').replace('-', '')
            if congress and bill_type and bill_number and action_date:
                return f"{congress}-sum-{bill_type}{bill_number}-{action_date}"
        except Exception as e:
            self.logger.error(f"Failed to generate summary ID: {str(e)}")
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
                'version': 1,
                'position': nomination.get('position', ''),
                'nominee': nomination.get('nominee', ''),
                'organization': nomination.get('organization', ''),
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

    def _process_committee_report(self, report: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee report"""
        try:
            report_id = self._generate_committee_report_id(report)
            if not report_id:
                self.logger.warning("Unable to generate ID for committee report")
                return None

            transformed_report = {
                'id': report_id,
                'type': 'committee-report',
                'congress': report.get('congress', current_congress),
                'report_number': report.get('number'),
                'update_date': report.get('updateDate', ''),
                'committee': report.get('committee', ''),
                'chamber': report.get('chamber', {}).get('name', ''),
                'version': 1,
                'report_type': report.get('reportType', ''),
                'title': report.get('title', ''),
                'associated_bill': {
                    'congress': report.get('associatedBill', {}).get('congress', ''),
                    'type': report.get('associatedBill', {}).get('type', ''),
                    'number': report.get('associatedBill', {}).get('number', '')
                },
                'latest_action': {
                    'text': report.get('latestAction', {}).get('text', ''),
                    'action_date': report.get('latestAction', {}).get('actionDate', ''),
                },
                'url': report.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee_report(transformed_report)
            if not is_valid:
                self.logger.warning(f"Committee report {report_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_committee_report(transformed_report)

        except Exception as e:
            self.logger.error(f"Failed to transform committee report: {str(e)}")
            return None

    def _generate_committee_report_id(self, report: Dict) -> Optional[str]:
        """Generate a committee report ID from report data"""
        try:
            congress = str(report.get('congress', ''))
            report_type = report.get('reportType', '').lower()
            report_number = str(report.get('number', ''))
            if congress and report_type and report_number:
                return f"{congress}-{report_type}-{report_number}"
        except Exception as e:
            self.logger.error(f"Failed to generate committee report ID: {str(e)}")
        return None

    def _process_congressional_record(self, record: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a congressional record"""
        try:
            record_id = self._generate_congressional_record_id(record)
            if not record_id:
                self.logger.warning("Unable to generate ID for congressional record")
                return None

            transformed_record = {
                'id': record_id,
                'type': 'congressional-record',
                'congress': record.get('congress', current_congress),
                'update_date': record.get('updateDate', ''),
                'version': 1,
                'record_type': record.get('recordType', ''),
                'session': record.get('session', ''),
                'issue': record.get('issue', ''),
                'pages': {
                    'start': record.get('pages', {}).get('start', ''),
                    'end': record.get('pages', {}).get('end', '')
                },
                'date': record.get('date', ''),
                'title': record.get('title', ''),
                'chamber': record.get('chamber', {}).get('name', ''),
                'url': record.get('url', '')
            }

            is_valid, errors = self.validator.validate_congressional_record(transformed_record)
            if not is_valid:
                self.logger.warning(f"Congressional record {record_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_congressional_record(transformed_record)

        except Exception as e:
            self.logger.error(f"Failed to transform congressional record: {str(e)}")
            return None

    def _generate_congressional_record_id(self, record: Dict) -> Optional[str]:
        """Generate a congressional record ID from record data"""
        try:
            congress = str(record.get('congress', ''))
            date = record.get('date', '').replace('-', '')
            issue = str(record.get('issue', ''))
            if congress and date and issue:
                return f"{congress}-cr-{date}-{issue}"
        except Exception as e:
            self.logger.error(f"Failed to generate congressional record ID: {str(e)}")
        return None

    def _process_house_communication(self, comm: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a house communication"""
        try:
            comm_id = self._generate_house_comm_id(comm)
            if not comm_id:
                self.logger.warning("Unable to generate ID for house communication")
                return None

            transformed_comm = {
                'id': comm_id,
                'type': 'house-communication',
                'congress': comm.get('congress', current_congress),
                'update_date': comm.get('updateDate', ''),
                'version': 1,
                'communication_type': comm.get('communicationType', ''),
                'number': comm.get('number', ''),
                'from_agency': comm.get('fromAgency', ''),
                'received_date': comm.get('receivedDate', ''),
                'title': comm.get('title', ''),
                'description': comm.get('description', ''),
                'referred_to': [
                    {
                        'committee': ref.get('committee', ''),
                        'date': ref.get('date', '')
                    }
                    for ref in comm.get('referredTo', [])
                ],
                'url': comm.get('url', '')
            }

            is_valid, errors = self.validator.validate_house_communication(transformed_comm)
            if not is_valid:
                self.logger.warning(f"House communication {comm_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_house_communication(transformed_comm)

        except Exception as e:
            self.logger.error(f"Failed to transform house communication: {str(e)}")
            return None

    def _generate_house_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a house communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-hcomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate house communication ID: {str(e)}")
        return None

    def _generate_senate_comm_id(self, comm: Dict) -> Optional[str]:
        """Generate a senate communication ID from communication data"""
        try:
            congress = str(comm.get('congress', ''))
            comm_type = comm.get('communicationType', '').lower()
            number = str(comm.get('number', ''))
            if congress and comm_type and number:
                return f"{congress}-scomm-{comm_type}-{number}"
        except Exception as e:
            self.logger.error(f"Failed to generate senate communication ID: {str(e)}")
        return None

    def _process_member(self, member: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a member"""
        try:
            member_id = self._generate_member_id(member)
            if not member_id:
                self.logger.warning("Unable to generate ID for member")
                return None

            transformed_member = {
                'id': member_id,
                'type': 'member',
                'congress': member.get('congress', current_congress),
                'update_date': member.get('updateDate', ''),
                'version': 1,
                'bioguide_id': member.get('bioguideId', ''),
                'first_name': member.get('firstName', ''),
                'last_name': member.get('lastName', ''),
                'state': member.get('state', ''),
                'district': member.get('district', ''),
                'party': member.get('party', ''),
                'chamber': member.get('chamber', {}).get('name', ''),
                'leadership_role': member.get('leadershipRole', ''),
                'served_until': member.get('servedUntil', ''),
                'url': member.get('url', '')
            }

            is_valid, errors = self.validator.validate_member(transformed_member)
            if not is_valid:
                self.logger.warning(f"Member {member_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_member(transformed_member)

        except Exception as e:
            self.logger.error(f"Failed to transform member: {str(e)}")
            return None

    def _generate_member_id(self, member: Dict) -> Optional[str]:
        """Generate a member ID from member data"""
        try:
            congress = str(member.get('congress', ''))
            bioguide_id = member.get('bioguideId', '')
            if congress and bioguide_id:
                return f"{congress}-mem-{bioguide_id}"
        except Exception as e:
            self.logger.error(f"Failed to generate member ID: {str(e)}")
        return None

    def _process_summaries(self, summary: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a summary"""
        try:
            summary_id = self._generate_summary_id(summary)
            if not summary_id:
                self.logger.warning("Unable to generate ID for summary")
                return None

            transformed_summary = {
                'id': summary_id,
                'type': 'summary',
                'congress': summary.get('congress', current_congress),
                'update_date': summary.get('updateDate', ''),
                'version': 1,
                'bill_id': summary.get('billId', ''),
                'bill_type': summary.get('billType', ''),
                'bill_number': summary.get('billNumber', ''),
                'action_date': summary.get('actionDate', ''),
                'action_desc': summary.get('actionDesc', ''),
                'text': summary.get('text', ''),
                'url': summary.get('url', '')
            }

            is_valid, errors = self.validator.validate_summary(transformed_summary)
            if not is_valid:
                self.logger.warning(f"Summary {summary_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_summary(transformed_summary)

        except Exception as e:
            self.logger.error(f"Failed to transform summary: {str(e)}")
            return None

    def _generate_summary_id(self, summary: Dict) -> Optional[str]:
        """Generate a summary ID from summary data"""
        try:
            congress = str(summary.get('congress', ''))
            bill_type = summary.get('billType', '').lower()
            bill_number = str(summary.get('billNumber', ''))
            action_date = summary.get('actionDate', '').replace('-', '')
            if congress and bill_type and bill_number and action_date:
                return f"{congress}-sum-{bill_type}{bill_number}-{action_date}"
        except Exception as e:
            self.logger.error(f"Failed to generate summary ID: {str(e)}")
        return None

    def _process_committee_meeting(self, meeting: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee meeting"""
        try:
            meeting_data = meeting.get('committeeMeeting', meeting)
            
            # Generate meeting ID
            meeting_id = self._generate_committee_meeting_id({
                'congress': current_congress,
                'committee': meeting_data.get('committee', {}).get('systemCode', ''),
                'date': meeting_data.get('date', '')
            })

            if not meeting_id:
                self.logger.error("Failed to generate committee meeting ID")
                return None

            transformed_meeting = {
                'id': meeting_id,
                'type': 'committee-meeting',
                'congress': current_congress,
                'update_date': meeting_data.get('updateDate', ''),
                'version': 1,
                'committee': {
                    'system_code': meeting_data.get('committee', {}).get('systemCode', ''),
                    'name': meeting_data.get('committee', {}).get('name', ''),
                    'url': meeting_data.get('committee', {}).get('url', '')
                },
                'date': meeting_data.get('date', ''),
                'time': meeting_data.get('time', ''),
                'location': meeting_data.get('location', ''),
                'topic': meeting_data.get('topic', ''),
                'url': meeting_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee_meeting(transformed_meeting)
            if not is_valid:
                self.logger.error(f"Committee meeting {meeting_id} failed validation: {errors}")
                self.logger.error(f"Invalid meeting data: {json.dumps(transformed_meeting, indent=2)}")
                return None

            return self.validator.cleanup_committee_meeting(transformed_meeting)

        except Exception as e:
            self.logger.error(f"Failed to transform committee meeting: {str(e)}")
            self.logger.error(f"Raw meeting data: {json.dumps(meeting, indent=2)}")
            return None

    def _generate_committee_meeting_id(self, meeting: Dict) -> Optional[str]:
        """Generate a committee meeting ID"""
        try:
            congress = str(meeting.get('congress', ''))
            committee = meeting.get('committee', '')
            date = meeting.get('date', '')
            
            if congress and committee and date:
                date_clean = re.sub(r'[^0-9]', '', date)
                return f"{congress}-cmtg-{committee}-{date_clean}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate committee meeting ID: {str(e)}")
            self.logger.error(f"Meeting data: {json.dumps(meeting, indent=2)}")
        return None

    def _process_committee_print(self, print_doc: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee print"""
        try:
            print_data = print_doc.get('committeePrint', print_doc)
            
            # Generate print ID
            print_id = self._generate_committee_print_id({
                'congress': current_congress,
                'committee': print_data.get('committee', {}).get('systemCode', ''),
                'number': print_data.get('number', '')
            })

            if not print_id:
                self.logger.error("Failed to generate committee print ID")
                return None

            transformed_print = {
                'id': print_id,
                'type': 'committee-print',
                'congress': current_congress,
                'update_date': print_data.get('updateDate', ''),
                'version': 1,
                'committee': {
                    'system_code': print_data.get('committee', {}).get('systemCode', ''),
                    'name': print_data.get('committee', {}).get('name', ''),
                    'url': print_data.get('committee', {}).get('url', '')
                },
                'number': print_data.get('number', ''),
                'title': print_data.get('title', ''),
                'url': print_data.get('url', '')
            }

            is_valid, errors = self.validator.validate_committee_print(transformed_print)
            if not is_valid:
                self.logger.error(f"Committee print {print_id} failed validation: {errors}")
                self.logger.error(f"Invalid print data: {json.dumps(transformed_print, indent=2)}")
                return None

            return self.validator.cleanup_committee_print(transformed_print)

        except Exception as e:
            self.logger.error(f"Failed to transform committee print: {str(e)}")
            self.logger.error(f"Raw print data: {json.dumps(print_doc, indent=2)}")
            return None

    def _generate_committee_print_id(self, print_doc: Dict) -> Optional[str]:
        """Generate a committee print ID"""
        try:
            congress = str(print_doc.get('congress', ''))
            committee = print_doc.get('committee', '')
            number = str(print_doc.get('number', ''))
            
            if congress and committee and number:
                return f"{congress}-cprint-{committee}-{number}"
                
        except Exception as e:
            self.logger.error(f"Failed to generate committee print ID: {str(e)}")
            self.logger.error(f"Print data: {json.dumps(print_doc, indent=2)}")
        return None