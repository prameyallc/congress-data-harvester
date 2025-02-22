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
import hashlib
import re
import json

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
        self.timeout = (5, 30)  # (connect timeout, read timeout)

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
            self.logger.debug(f"Making request to {url} with params: {json.dumps({k: v for k, v in params.items() if k != 'api_key'}, indent=2)}")
            response = self.session.get(url, params=params, timeout=self.timeout)
            duration = time.time() - start_time

            self.logger.debug(f"Response status code: {response.status_code}")
            self.logger.debug(f"Response headers: {dict(response.headers)}")

            metrics.track_api_request(
                endpoint=endpoint,
                status_code=response.status_code,
                duration=duration
            )

            if response.status_code == 200:
                self.consecutive_errors = 0
                response_json = response.json()
                self.logger.info(f"Successful response from {endpoint}")
                self.logger.debug(f"Response data: {json.dumps(response_json, indent=2)}")
                self.logger.debug(f"Response data keys: {list(response_json.keys())}")
                return response_json

            if response.status_code == 429:
                self.consecutive_errors += 1
                retry_after = response.headers.get('Retry-After', 60)
                self.logger.error(f"Rate limit exceeded. Retry after {retry_after} seconds")
                time.sleep(float(retry_after))
                raise Exception("Rate limit exceeded")

            if response.status_code == 403:
                self.logger.error(f"API authentication failed. Please verify API key. URL: {url}")
                raise Exception("API authentication failed - please verify API key")

            if response.status_code == 404:
                self.logger.error(f"API endpoint not found: {url}")
                raise Exception(f"API endpoint not found: {url}")

            self.logger.error(f"Unexpected status code {response.status_code} for {url}: {response.text}")
            response.raise_for_status()

        except requests.exceptions.Timeout as e:
            duration = time.time() - start_time
            self.logger.error(f"Request timed out for endpoint {endpoint}: {str(e)}")
            metrics.track_api_request(
                endpoint=endpoint,
                status_code=408,
                duration=duration
            )
            self.consecutive_errors += 1
            raise Exception(f"Request timed out: {str(e)}")

        except requests.exceptions.RequestException as e:
            duration = time.time() - start_time
            metrics.track_api_request(
                endpoint=endpoint,
                status_code=500,
                duration=duration
            )
            self.consecutive_errors += 1
            self.logger.error(f"Request failed for {url}: {str(e)}")
            raise Exception(f"Request failed: {str(e)}")

    def get_available_endpoints(self) -> Dict[str, Any]:
        """Get list of available API endpoints and their details"""
        try:
            standard_endpoints = [
                'bill', 'amendment', 'nomination', 'treaty',
                'committee', 'hearing', 'committee-report',
                'congressional-record', 'house-communication',
                'house-requirement', 'senate-communication',
                'member', 'summaries'
            ]
            available_endpoints = {}

            for endpoint in standard_endpoints:
                try:
                    self.logger.info(f"Checking endpoint: {endpoint}")
                    # Test the committee endpoint with more specific parameters
                    params = {
                        'limit': 1,
                        'format': 'json',
                        'offset': 0
                    }
                    if endpoint == 'committee':
                        params.update({
                            'congress': self.get_current_congress(),
                            'chamber': 'house'  # Try with a specific chamber
                        })
                    
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
            
            for key in possible_keys:
                if key in response_data:
                    items = response_data[key]
                    self.logger.info(f"Found {len(items)} items in '{key}' key")
                    # Log sample item structure
                    if items:
                        self.logger.debug(f"Sample item structure: {json.dumps(items[0], indent=2)}")
                    break
            
            if not items:
                self.logger.warning(f"No items found in response. Available keys: {list(response_data.keys())}")
                return []

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

    def _process_item(self, endpoint: str, item: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a single item based on its type"""
        try:
            processors = {
                'bill': self._process_bill,
                'amendment': self._process_amendment,
                'nomination': self._process_nomination,
                'treaty': self._process_treaty,
                'committee-report': self._process_committee_report,
                'congressional-record': self._process_congressional_record,
                'house-communication': self._process_house_communication,
                'committee': self._process_committee,
                'hearing': self._process_hearing,
                'house-requirement': self._process_house_requirement,
                'senate-communication': self._process_senate_communication,
                'member': self._process_member,
                'summaries': self._process_summaries
            }

            if endpoint not in processors:
                self.logger.warning(f"No processor available for endpoint: {endpoint}")
                return None

            return processors[endpoint](item, current_congress)

        except Exception as e:
            self.logger.error(f"Failed to process {endpoint} item: {str(e)}")
            return None

    def _process_committee(self, committee: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a committee"""
        try:
            # Log raw committee data for debugging
            self.logger.debug(f"Raw committee data: {json.dumps(committee, indent=2)}")
            
            # Handle case where committee is a string
            if isinstance(committee, str):
                self.logger.warning(f"Received string instead of dictionary for committee: {committee}")
                return None

            # Extract committee data, handling both direct and nested structures
            committee_data = committee.get('committee', committee)
            
            # Get the basic committee information with detailed logging
            committee_type = committee_data.get('committeeTypeCode', committee_data.get('type', '')).lower()
            chamber = committee_data.get('chamber', committee_data.get('originChamber', ''))
            chamber_name = chamber.get('name', chamber) if isinstance(chamber, dict) else str(chamber).title()
            name = committee_data.get('name', committee_data.get('committeeName', ''))
            
            self.logger.debug(f"Extracted committee fields: type={committee_type}, chamber={chamber_name}, name={name}")
            
            # Generate committee ID
            committee_id = self._generate_committee_id({
                'congress': current_congress,
                'chamber': chamber_name.lower() if chamber_name else '',
                'committeeTypeCode': committee_type,
                'name': name
            })
            
            if not committee_id:
                self.logger.warning("Unable to generate ID for committee")
                self.logger.debug("ID generation failed with data: " + 
                                f"congress={current_congress}, " +
                                f"chamber={chamber_name}, " +
                                f"type={committee_type}, " +
                                f"name={name}")
                return None

            transformed_committee = {
                'id': committee_id,
                'type': 'committee',
                'congress': current_congress,
                'update_date': committee_data.get('updateDate', ''),
                'version': 1,
                'name': name,
                'chamber': chamber_name,
                'committee_type': committee_type,
                'system_code': committee_data.get('systemCode', ''),
                'parent_committee': {
                    'name': committee_data.get('parent', {}).get('name', ''),
                    'system_code': committee_data.get('parent', {}).get('systemCode', ''),
                    'url': committee_data.get('parent', {}).get('url', '')
                } if committee_data.get('parent') else {},
                'url': committee_data.get('url', '')
            }

            # Log the transformation details
            self.logger.debug(f"Transformed committee: {json.dumps(transformed_committee, indent=2)}")

            is_valid, errors = self.validator.validate_committee(transformed_committee)
            if not is_valid:
                self.logger.warning(f"Committee {committee_id} failed validation: {errors}")
                return None

            return self.validator.cleanup_committee(transformed_committee)

        except Exception as e:
            self.logger.error(f"Failed to transform committee: {str(e)}")
            self.logger.error(f"Problematic committee data: {json.dumps(committee, indent=2)}")
            return None

    def _generate_committee_id(self, committee: Dict) -> Optional[str]:
        """Generate a committee ID from committee data"""
        try:
            # Type checking with detailed logging
            if not isinstance(committee, dict):
                self.logger.error(f"Committee data must be a dictionary, got {type(committee)}")
                self.logger.debug(f"Invalid committee data: {committee}")
                return None

            # Get direct values with fallbacks
            congress = str(committee.get('congress', ''))
            chamber = committee.get('chamber', '').lower()
            comm_type = committee.get('committeeTypeCode', committee.get('type', '')).lower()
            name = committee.get('name', '')

            # Log extracted values
            self.logger.debug(
                "ID generation values: " +
                f"congress={congress}, " +
                f"chamber={chamber}, " +
                f"type={comm_type}, " +
                f"name={name}"
            )

            # Validate required components
            if not all([congress, chamber, comm_type, name]):
                self.logger.warning(
                    f"Missing required fields for committee ID generation: " +
                    f"congress={congress}, chamber={chamber}, " +
                    f"comm_type={comm_type}, name={name}"
                )
                return None

            # Generate hash for name
            name_hash = hashlib.md5(name.encode()).hexdigest()[:8]
            committee_id = f"{congress}-{chamber}-{comm_type}-{name_hash}"
            
            self.logger.debug(f"Generated committee ID: {committee_id}")
            return committee_id

        except Exception as e:
            self.logger.error(f"Failed to generate committee ID: {str(e)}")
            self.logger.debug(f"Committee data that caused error: {json.dumps(committee, indent=2)}")
            return None

    def _process_hearing(self, hearing: Dict, current_congress: int) -> Optional[Dict]:
        """Process and validate a hearing"""
        try:
            # Extract and validate required fields from the hearing response structure
            hearing_details = hearing.get('hearing', hearing)
            
            if isinstance(hearing_details, dict):
                committee = hearing_details.get('committee', '')
                hearing_date = hearing_details.get('date', '')
                chamber = hearing_details.get('chamber', {})
                hearing_type = hearing_details.get('type', '')
                title = hearing_details.get('title', '')
            else:
                committee = hearing.get('committee', '')
                hearing_date = hearing.get('date', '')
                chamber = hearing.get('chamber', {})
                hearing_type = hearing.get('type', '')
                title = hearing.get('title', '')

            if not committee or not hearing_date:
                self.logger.warning(f"Missing required fields for hearing ID generation: committee={committee}, date={hearing_date}")
                return None

            hearing_id = self._generate_hearing_id({
                'congress': current_congress,
                'date': hearing_date,
                'committee': committee
            })

            if not hearing_id:
                self.logger.warning("Unable to generate ID for hearing")
                return None

            transformed_hearing = {
                'id': hearing_id,
                'type': 'hearing',
                'congress': current_congress,
                'update_date': hearing.get('updateDate', ''),
                'version': 1,
                'chamber': chamber.get('name', '') if isinstance(chamber, dict) else '',
                'committee': committee,
                'subcommittee': hearing.get('subcommittee', ''),
                'hearing_type': hearing_type,
                'title': title,
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