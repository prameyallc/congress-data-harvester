import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
import json

class DataValidator:
    """Validates and cleanups Congress.gov data before storage"""

    def __init__(self):
        self.logger = logging.getLogger('congress_downloader')
        self.validation_stats = {}  # Changed to a dictionary to store stats for each record type

    def validate_data(self, data: Dict[str, Any], data_type: str) -> Tuple[bool, List[str]]:
        """
        Validate data based on its type
        Returns: (is_valid, list of validation errors)
        """
        self.validation_stats['total_processed'] = self.validation_stats.get('total_processed', 0) + 1

        # Initialize stats for this type if not exists
        if 'by_type' not in self.validation_stats:
            self.validation_stats['by_type'] = {}
        if data_type not in self.validation_stats['by_type']:
            self.validation_stats['by_type'][data_type] = {
                'processed': 0,
                'valid': 0,
                'invalid': 0
            }
        self.validation_stats['by_type'][data_type]['processed'] += 1

        # Route to appropriate validator
        validators = {
            'bill': self.validate_bill,
            'amendment': self.validate_amendment,
            'nomination': self.validate_nomination,
            'treaty': self.validate_treaty,
            'committee-report': self.validate_committee_report,
            'congressional-record': self.validate_congressional_record,
            'house-communication': self.validate_house_communication,
            'committee': self.validate_committee,
            'hearing': self.validate_hearing,
            'house-requirement': self.validate_house_requirement,
            'senate-communication': self.validate_senate_communication,
            'member': self.validate_member,
            'summaries': self.validate_summary,
            'committee-print': self.validate_committee_print,
            'daily-congressional-record': self.validate_daily_congressional_record,
            'bound-congressional-record': self.validate_bound_record,
            'committee-meeting': self.validate_committee_meeting,
            'congress': self.validate_congress
        }

        if data_type in validators:
            return validators[data_type](data)
        else:
            return False, [f"Unknown data type: {data_type}"]

    def validate_bill(self, bill: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate bill data structure and content"""
        errors = []

        # Required fields
        required_fields = ['id', 'congress', 'title', 'update_date', 'bill_type', 'bill_number']
        for field in required_fields:
            if field not in bill:
                errors.append(f"Missing required field: {field}")

        if not errors:  # Only proceed with other validations if required fields exist
            # Congress number validation
            try:
                congress_num = int(bill['congress'])
                if congress_num < 1 or congress_num > 150:  # Reasonable range check
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Bill number validation
            try:
                bill_num = int(bill['bill_number'])
                if bill_num < 1:
                    errors.append("Bill number must be positive")
            except (ValueError, TypeError):
                errors.append("Bill number must be a valid number")

            # Bill type validation
            valid_bill_types = ['hr', 's', 'hjres', 'sjres', 'hconres', 'sconres', 'hres', 'sres']
            if bill['bill_type'].lower() not in valid_bill_types:
                errors.append(f"Invalid bill type: {bill['bill_type']}")

            # Date format validations
            for date_field in ['update_date', 'introduced_date']:
                if date_field in bill and bill[date_field]:
                    if not self._is_valid_date(bill[date_field]):
                        errors.append(f"Invalid {date_field} format: {bill[date_field]}")

            # Latest action validation
            if 'latest_action' in bill:
                latest_action = bill['latest_action']
                if not isinstance(latest_action, dict):
                    errors.append("latest_action must be a dictionary")
                else:
                    if 'action_date' in latest_action and latest_action['action_date']:
                        if not self._is_valid_date(latest_action['action_date']):
                            errors.append(f"Invalid action_date format: {latest_action['action_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('bill', is_valid)  # Use new stats method
        if is_valid:
            self.logger.debug(f"Bill {bill['id']} passed validation")
        else:
            self.logger.warning(f"Bill {bill['id']} failed validation: {', '.join(errors)}")

        return is_valid, errors

    def validate_amendment(self, amendment: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate amendment data structure"""
        errors = []
        required_fields = ['id', 'congress', 'type', 'number', 'update_date']

        for field in required_fields:
            if field not in amendment:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(amendment))

            # Amendment number validation
            try:
                amdt_num = int(amendment['number'])
                if amdt_num < 1:
                    errors.append("Amendment number must be positive")
            except (ValueError, TypeError):
                errors.append("Amendment number must be a valid number")

            # Type validation with detailed error message
            valid_types = {'samdt': 'Senate Amendment', 'hamdt': 'House Amendment'}
            amdt_type = amendment.get('type', '').lower()
            if amdt_type not in valid_types:
                errors.append(f"Invalid amendment type: {amendment.get('type')}. Must be one of: {', '.join(valid_types.keys())}")

            # Chamber validation
            if 'chamber' in amendment:
                valid_chambers = ['house', 'senate']
                chamber = amendment['chamber'].lower()
                if chamber not in valid_chambers:
                    errors.append(f"Invalid chamber: {amendment['chamber']}. Must be one of: House, Senate")

            # Associated bill validation
            if 'associated_bill' in amendment:
                if not isinstance(amendment['associated_bill'], dict):
                    errors.append("associated_bill must be a dictionary")
                else:
                    for field in ['congress', 'type', 'number']:
                        if field not in amendment['associated_bill']:
                            errors.append(f"Missing {field} in associated_bill")

        is_valid = len(errors) == 0
        self._update_validation_stats('amendment', is_valid)
        if not is_valid:
            self.logger.warning(f"Amendment validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid amendment data: {json.dumps(amendment, indent=2)}")

        return is_valid, errors

    def validate_nomination(self, nomination: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate nomination data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'description', 'organization']

        for field in required_fields:
            if field not in nomination:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(nomination))

            # Citation validation
            if 'citation' in nomination:
                if not nomination['citation'].startswith('PN'):
                    errors.append("Citation must start with 'PN' (e.g., 'PN123')")
                elif not re.match(r'^PN\d+(-\d+)?$', nomination['citation']):
                    errors.append("Invalid citation format. Must be 'PN' followed by numbers and optional dash (e.g., 'PN123' or 'PN123-1')")
            else:
                # Generate citation from other fields if missing
                if all(key in nomination for key in ['number', 'partNumber']):
                    nomination['citation'] = f"PN{nomination['number']}-{nomination['partNumber']}"

            # Organization validation
            if 'organization' in nomination:
                if not nomination['organization'].strip():
                    errors.append("Organization cannot be empty")
                elif len(nomination['organization'].strip()) < 3:
                    errors.append("Organization name must be at least 3 characters")

            # Latest action validation
            if 'latestAction' in nomination:
                if not isinstance(nomination['latestAction'], dict):
                    errors.append("latestAction must be a dictionary")
                else:
                    if 'actionDate' not in nomination['latestAction']:
                        errors.append("latestAction must contain actionDate")
                    elif not self._is_valid_date(nomination['latestAction']['actionDate']):
                        errors.append(f"Invalid action date format: {nomination['latestAction']['actionDate']}")
                    if 'text' not in nomination['latestAction'] or not nomination['latestAction']['text'].strip():
                        errors.append("latestAction must contain non-empty text")

            # Nomination type validation
            if 'nominationType' in nomination:
                if not isinstance(nomination['nominationType'], dict):
                    errors.append("nominationType must be a dictionary")
                elif 'isCivilian' in nomination['nominationType'] and not isinstance(nomination['nominationType']['isCivilian'], bool):
                    errors.append("nominationType.isCivilian must be a boolean")

            # Date validations with helpful error messages
            for date_field in ['received_date', 'update_date']:
                if date_field in nomination and nomination[date_field]:
                    if not self._is_valid_date(nomination[date_field]):
                        errors.append(f"Invalid {date_field} format: {nomination[date_field]}. Must be YYYY-MM-DD or ISO format")

        is_valid = len(errors) == 0
        self._update_validation_stats('nomination', is_valid)
        if not is_valid:
            self.logger.warning(f"Nomination validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid nomination data: {json.dumps(nomination, indent=2)}")

        return is_valid, errors

    def validate_treaty(self, treaty: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate treaty data structure"""
        errors = []
        required_fields = ['id', 'congress', 'treaty_number', 'update_date', 'description']

        for field in required_fields:
            if field not in treaty:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(treaty))

            # Treaty number validation
            if 'treaty_number' in treaty:
                if not treaty['treaty_number'].strip():
                    errors.append("Treaty number cannot be empty")
                elif not re.match(r'^[0-9-]+$', treaty['treaty_number']):
                    errors.append("Treaty number must contain only digits and hyphens")

            # Date validations
            for date_field in ['received_date', 'submitted_date', 'update_date']:
                if date_field in treaty and treaty[date_field]:
                    if not self._is_valid_date(treaty[date_field]):
                        errors.append(f"Invalid {date_field} format: {treaty[date_field]}")

            # Description validation
            if 'description' in treaty and not treaty['description'].strip():
                errors.append("Description cannot be empty")

            # Subject validation
            if 'subject' in treaty and not treaty['subject'].strip():
                errors.append("Subject cannot be empty")

        is_valid = len(errors) == 0
        self._update_validation_stats('treaty', is_valid)
        if not is_valid:
            self.logger.warning(f"Treaty validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid treaty data: {json.dumps(treaty, indent=2)}")

        return is_valid, errors

    def validate_committee_report(self, report: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee report data structure"""
        errors = []
        required_fields = ['id', 'congress', 'report_number', 'update_date', 'committee']

        for field in required_fields:
            if field not in report:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(report))

            # Report number validation
            try:
                report_num = int(report['report_number'])
                if report_num < 1:
                    errors.append("Report number must be positive")
            except (ValueError, TypeError):
                errors.append("Report number must be a valid number")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-report', is_valid)
        return is_valid, errors

    def validate_congress(self, congress: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate congress data structure and content"""
        errors = []

        # Required fields
        required_fields = ['id', 'congress', 'type', 'update_date']
        for field in required_fields:
            if field not in congress:
                errors.append(f"Missing required field: {field}")

        if not errors:  # Only proceed with other validations if required fields exist
            # Congress number validation
            try:
                congress_num = int(congress['congress'])
                if congress_num < 1 or congress_num > 150:  # Reasonable range check
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date format validations
            for date_field in ['update_date', 'start_date', 'end_date']:
                if date_field in congress and congress[date_field]:
                    if not self._is_valid_date(congress[date_field]):
                        errors.append(f"Invalid {date_field} format: {congress[date_field]}")

            # Type validation
            if congress.get('type') != 'congress':
                errors.append(f"Invalid type: {congress.get('type')}. Expected: congress")

            # Validate house and senate data if present
            if 'house' in congress and not isinstance(congress['house'], dict):
                errors.append("House data must be a dictionary")

            if 'senate' in congress and not isinstance(congress['senate'], dict):
                errors.append("Senate data must be a dictionary")

        is_valid = len(errors) == 0
        self._update_validation_stats('congress', is_valid)
        if is_valid:
            self.logger.debug(f"Congress {congress['id']} passed validation")
        else:
            self.logger.warning(f"Congress {congress['id']} failed validation: {', '.join(errors)}")

        return is_valid, errors

    def cleanup_committee_report(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee report data"""
        cleaned = report.copy()

        # Convert numeric fields
        for field in ['congress', 'report_number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['title', 'committee']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure associated bill is properly structured
        if 'associated_bill' not in cleaned:
            cleaned['associated_bill'] = {'congress': '', 'type': '', 'number': ''}

        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'date', 'volume_number']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(record))

            # Validate volume number
            try:
                vol_num = int(record['volume_number'])
                if vol_num < 1:
                    errors.append("Volume number must be positive")
            except (ValueError, TypeError):
                errors.append("Volume number must be a valid number")

            # Session number validation
            if 'session_number' in record:
                try:
                    session_num = int(record['session_number'])
                    if session_num < 1 or session_num > 2:
                        errors.append("Session number must be 1 or 2")
                except (ValueError, TypeError):
                    errors.append("Session number must be a valid number")

            # Date validations
            for date_field in ['date', 'update_date']:
                if date_field in record and record[date_field]:
                    if not self._is_valid_date(record[date_field]):
                        errors.append(f"Invalid {date_field} format: {record[date_field]}")

            # Chamber validation
            if 'chamber' in record:
                valid_chambers = ['house', 'senate', 'joint']
                if record['chamber'].lower() not in valid_chambers:
                    errors.append(f"Invalid chamber: {record['chamber']}")

            # Sections validation
            if 'sections' in record:
                if not isinstance(record['sections'], dict):
                    errors.append("Sections must be a dictionary")
                else:
                    valid_sections = ['digest', 'house', 'senate', 'extensions']
                    for section in record['sections']:
                        if section not in valid_sections:
                            errors.append(f"Invalid section: {section}")
                        elif not isinstance(record['sections'][section], dict):
                            errors.append(f"Section {section} must be a dictionary")

        is_valid = len(errors) == 0
        self._update_validation_stats('congressional-record', is_valid)
        if not is_valid:
            self.logger.warning(f"Congressional record validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid record data: {json.dumps(record, indent=2)}")

        return is_valid, errors

    def cleanup_congressional_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize congressional record data"""
        cleaned = record.copy()

        # Map API response fields to schema fields
        fieldmapping = {
            'Congress': 'congress',
            'Volume': 'volume_number',
            'Session': 'session_number',
            'Chamber': 'chamber',
            'PublishDate': 'date',
            'Issue': 'issue_number'
        }
        for api_field, schema_field in fieldmapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Convert numeric fields
        for field in ['congress', 'volume_number', 'session_number', 'issue_number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize chamber
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower().strip()

        # Normalize dates
        for date_field in ['date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Normalize sections
        if 'sections' in cleaned:
            normalized_sections = {}
            for section_name, content in cleaned['sections'].items():
                if isinstance(content, dict):
                    normalized_sections[section_name.lower()] = {
                        'title': content.get('title', '').strip(),
                        'content': content.get('content', '').strip(),
                        'page_range': content.get('page_range', '')
                    }
            cleaned['sections'] = normalized_sections

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def validate_house_communication(self, comm: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate house communication data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'communication_type', 'number']

        for field in required_fields:
            if field not in comm:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(comm))

            # Communication number validation
            try:
                comm_num = int(comm['number'])
                if comm_num < 1:
                    errors.append("Communication number must be positive")
            except (ValueError, TypeError):
                errors.append("Communication number must be a valid number")

            # Date validations
            for date_field in ['update_date', 'received_date']:
                if date_field in comm and comm[date_field]:
                    if not self._is_valid_date(comm[date_field]):
                        errors.append(f"Invalid {date_field} format: {comm[date_field]}")

            # Referred to validation
            if 'referred_to' in comm:
                if not isinstance(comm['referred_to'], list):
                    errors.append("referred_to must be a list")
                else:
                    for ref in comm['referred_to']:
                        if not isinstance(ref, dict):
                            errors.append("Each referral must be a dictionary")
                        elif 'committee' not in ref or 'date' not in ref:
                            errors.append("Each referral must contain 'committee' and 'date'")

        is_valid = len(errors) == 0
        self._update_validation_stats('house-communication', is_valid)
        return is_valid, errors

    def cleanup_house_communication(self, comm: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize house communication data"""
        cleaned = comm.copy()

        # Convert numeric fields
        for field in ['congress', 'number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['title', 'description', 'from_agency', 'communication_type']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure referred_to is a list
        if 'referred_to' not in cleaned or not isinstance(cleaned['referred_to'], list):
            cleaned['referred_to'] = []
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def _update_validation_stats(self, record_type: str, is_valid: bool) -> None:
        """Update validation statistics for monitoring"""
        if record_type not in self.validation_stats:
            self.validation_stats[record_type] = {'total': 0, 'valid': 0, 'invalid': 0}

        self.validation_stats[record_type]['total'] += 1
        if is_valid:
            self.validation_stats[record_type]['valid'] += 1
        else:
            self.validation_stats[record_type]['invalid'] += 1

    def _is_valid_date(self, date_str: str) -> bool:
        """Validate and normalize date string format"""
        if not date_str:
            return False
        try:
            # Handle both ISO format with time and simple YYYY-MM-DD
            from datetime import datetime
            if 'T' in date_str:
                dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
                return True
            else:
                datetime.strptime(date_str, '%Y-%m-%d')
                return True
        except ValueError:
            return False

    def _normalize_date(self, date_str: str) -> str:
        """Convert any valid date format to YYYY-MM-DD"""
        try:
            from datetime import datetime
            if 'T' in date_str:
                dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
            else:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return date_str

    def _validate_common_fields(self, item: Dict[str, Any]) -> List[str]:
        """Validate fields common across all record types"""
        errors = []

        # Required fields check
        required_fields = ['id', 'type', 'update_date', 'congress', 'version']
        for field in required_fields:
            if field not in item:
                errors.append(f"Missing required field: {field}")
                continue

            # Type-specific validation
            if field == 'congress':
                try:
                    congress_num = int(item[field])
                    if congress_num < 1 or congress_num > 150:
                        errors.append(f"Invalid congress number: {congress_num}")
                except (ValueError, TypeError):
                    errors.append("Congress must be a valid number")

            elif field == 'version':
                try:
                    version_num = int(item[field])
                    if version_num < 1:
                        errors.append("Version must be a positive integer")
                except (ValueError, TypeError):
                    errors.append("Version must be a valid number")

            elif field == 'update_date':
                if not self._is_valid_date(item[field]):
                    errors.append(f"Invalid update_date format: {item[field]}")

        # Chamber validation if present
        if 'chamber' in item:
            valid_chambers = ['house', 'senate', 'joint']
            if item['chamber'].lower() not in valid_chambers:
                errors.append(f"Invalid chamber: {item['chamber']}")

        # Date field validation if present
        if 'date' in item and item['date']:
            if not self._is_valid_date(item['date']):
                errors.append(f"Invalid date format: {item['date']}")

        return errors

    def _cleanup_common_fields(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize fields common across all record types"""
        cleaned = item.copy()

        # Convert numeric fields
        for field in ['congress', 'version']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")
                    cleaned[field] = 1  # Default to 1 for required numeric fields

        # Normalize dates to YYYY-MM-DD format
        for field in ['update_date', 'date', 'updateDate', 'publishDate', 'actionDate']:
            if field in cleaned and cleaned[field]:
                cleaned[field] = self._normalize_date(cleaned[field])

        # Map common field names from API response to schema names
        field_mapping = {
            'updateDate': 'update_date',
            'publishDate': 'publish_date',
            'Congress': 'congress',
            'Type': 'type',
            'Id': 'id'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize chamber to lowercase if present
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower()

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        return cleaned

    def cleanup_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """Clean and normalize data based on its type"""
        if data_type == 'bill':
            return self.cleanup_bill(data)
        elif data_type == 'amendment':
            return self.cleanup_amendment(data)
        elif data_type == 'nomination':
            return self.cleanup_nomination(data)
        elif data_type == 'treaty':
            return self.cleanup_treaty(data)
        elif data_type == 'committee-report':
            return self.cleanup_committee_report(data)
        elif data_type == 'congressional-record':
            return self.cleanup_congressional_record(data)
        elif data_type == 'house-communication':
            return self.cleanup_house_communication(data)
        elif data_type == 'committee':
            return self.cleanup_committee(data)
        elif data_type == 'hearing':
            return self.cleanup_hearing(data)
        elif data_type == 'house-requirement':
            return self.cleanup_house_requirement(data)
        elif data_type == 'senate-communication':
            return self.cleanup_senate_communication(data)
        elif data_type == 'member':
            return self.cleanup_member(data)
        elif data_type == 'summary':
            return self.cleanup_summary(data)
        elif data_type == 'committee-print':
            return self.cleanup_committee_print(data)
        elif data_type == 'committee-meeting':
            return self.cleanup_committee_meeting(data)
        elif data_type == 'daily-congressional-record':
            return self.cleanup_daily_congressional_record(data)
        elif data_type == 'bound-congressional-record':
            return self.cleanup_bound_record(data)
        elif data_type == 'congress':
            return self.cleanup_congress(data)
        return data

    def cleanup_bill(self, bill: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize bill data"""
        cleaned = bill.copy()

        # Normalize bill type to lowercase
        if 'bill_type' in cleaned:
            cleaned['bill_type'] = cleaned['bill_type'].lower()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Convert bill number to integer
        if 'bill_number' in cleaned:
            try:
                cleaned['bill_number'] = int(cleaned['bill_number'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert bill number to integer: {cleaned['bill_number']}")

        # Clean title (remove extra whitespace)
        if 'title' in cleaned:
            cleaned['title'] = ' '.join(cleaned['title'].split())

        # Normalize chamber names
        if 'origin_chamber' in cleaned and cleaned['origin_chamber']:
            chamber_mapping = {
                'house': 'House',
                'senate': 'Senate',
                'HOUSE': 'House',
                'SENATE': 'Senate'
            }
            cleaned['origin_chamber'] = chamber_mapping.get(cleaned['origin_chamber'], cleaned['origin_chamber'])

        # Ensure lists are present
        list_fields = ['sponsors', 'committees']
        for field in list_fields:
            if field not in cleaned or cleaned[field] is None:
                cleaned[field] = []

        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def cleanup_amendment(self, amendment: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize amendment data"""
        cleaned = amendment.copy()

        # Convert numeric fields
        for field in ['congress', 'number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize type field
        if 'type' in cleaned:
            cleaned['type'] = cleaned['type'].lower().strip()

        # Normalize chamber
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower().strip()

        # Map API response fields to schema fields
        field_mapping = {
            'amendmentNumber': 'number',
            'amendmentType': 'type',
            'purpose': 'purpose',
            'submitDate': 'submit_date',
            'congressionalRecord': 'congressional_record',
            'associatedBill': 'associated_bill'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize associated bill if present
        if 'associated_bill' in cleaned and isinstance(cleaned['associated_bill'], dict):
            cleaned['associated_bill'] = {
                'congress': int(cleaned['associated_bill'].get('congress', 0)),
                'type': cleaned['associated_bill'].get('type', '').lower(),
                'number': int(cleaned['associated_bill'].get('number', 0))
            }

        # Ensure submit_date is in correct format
        if 'submit_date' in cleaned and cleaned['submit_date']:
            cleaned['submit_date'] = self._normalize_date(cleaned['submit_date'])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def cleanup_nomination(self, nomination: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize nomination data"""
        cleaned = nomination.copy()

        # Extract number from citation if present
        if 'citation' in cleaned:
            try:
                number_str = cleaned['citation'].replace('PN', '').split('-')[0].strip()
                cleaned['number'] = int(number_str)
            except (ValueError, IndexError):                self.logger.warning(f"Could not extract number from citation: {cleaned['citation']}")
                cleaned['number'] = 0  # Default value for invalid number

        # Map API response fields to schema fields
        field_mapping = {
            'latestAction': 'latest_action',
            'nominationType': 'nomination_type',
            'receivedDate': 'received_date',
            'organizationType': 'organization_type'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize text fields
        for field in ['description', 'organization']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split()).strip()

        # Normalize dates
        for date_field in ['received_date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Normalize latest action
        if 'latest_action' in cleaned and isinstance(cleaned['latest_action'], dict):
            cleaned['latest_action'] = {
                'text': cleaned['latest_action'].get('text', '').strip(),
                'action_date': self._normalize_date(cleaned['latest_action'].get('actionDate', ''))
            }

        # Normalize nomination type
        if 'nomination_type' in cleaned:
            cleaned['nomination_type'] = {
                'is_civilian': cleaned['nomination_type'].get('isCivilian', True)
            }

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Ensure type field is set
        cleaned['type'] = 'nomination'

        return cleaned

    def cleanup_treaty(self, treaty: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize treaty data"""
        cleaned = treaty.copy()

        # Map API response fields to schema fields
        field_mapping = {
            'treatyNumber': 'treaty_number',
            'receivedDate': 'received_date',
            'submittedDate': 'submitted_date',
            'description': 'description',
            'subject': 'subject'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize text fields
        for field in ['description', 'subject', 'title', 'country']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split()).strip()

        # Normalize dates to YYYY-MM-DD format
        for date_field in ['received_date', 'submitted_date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def get_validation_stats(self) -> Dict[str, Any]:
        """Return current validation statistics"""
        return self.validation_stats.copy()

    def reset_validation_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {}

    def validate_committee(self, committee: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'name', 'chamber']

        for field in required_fields:
            if field not in committee:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(committee))

            # Committee type validation - map from API response field
            committee_type = committee.get('committee_type') or committee.get('committeeTypeCode', '').lower()
            if committee_type:
                committee['committee_type'] = committee_type
                valid_types = ['standing', 'select', 'joint', 'special', 'subcommittee']
                if committee_type.lower() not in valid_types:
                    errors.append(f"Invalid committee_type: {committee_type}")
            else:
                committee['committee_type'] = 'standing'  # Default type if not specified

            # Chamber validation
            valid_chambers = ['house','senate', 'joint']
            if committee.get('chamber', '').lower() not in valid_chambers:
                errors.append(f"Invalid chamber: {committee.get('chamber')}")

            # Parent committee validation
            if 'parent' in committee:
                if not isinstance(committee['parent'], dict):
                    errors.append("Parent committee must be a dictionary")
                else:
                    required_parent_fields = ['name', 'systemCode']
                    for field in required_parent_fields:
                        if field not in committee['parent']:
                            errors.append(f"Missing required parent committee field: {field}")

            # Subcommittees validation
            if 'subcommittees' in committee:
                if not isinstance(committee['subcommittees'], list):
                    errors.append("Subcommittees must be a list")
                else:
                    for idx, subcommittee in enumerate(committee['subcommittees']):
                        if not isinstance(subcommittee, dict):
                            errors.append(f"Subcommittee {idx} must be a dictionary")
                        else:
                            for field in ['name', 'systemCode']:
                                if field not in subcommittee:
                                    errors.append(f"Missing required field {field} in subcommittee {idx}")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee', is_valid)
        if not is_valid:
            self.logger.warning(f"Committee validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid committee data: {json.dumps(committee, indent=2)}")

        return is_valid, errors

    def cleanup_committee(self, committee: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee data"""
        cleaned = self._cleanup_common_fields(committee.copy())

        # Map API response fields to schema fields
        field_mapping = {
            'committeeTypeCode': 'committee_type',
            'systemCode': 'system_code',
            'parentCommittee': 'parent_committee'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize committee type
        if 'committee_type' in cleaned:
            cleaned['committee_type'] = cleaned['committee_type'].lower()
        else:
            cleaned['committee_type'] = 'standing'  # Default type
        
        # Normalize chamber to lowercase
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower()

        # Clean up parent committee structure
        if 'parent' in cleaned and isinstance(cleaned['parent'], dict):
            cleaned['parent_committee'] = {
                'name': cleaned['parent'].get('name', ''),
                'system_code': cleaned['parent'].get('systemCode', ''),
                'url': cleaned['parent'].get('url', '')
            }
            del cleaned['parent']

        # Ensure subcommittees is a list with proper structure
        if 'subcommittees' not in cleaned:
            cleaned['subcommittees'] = []
        elif isinstance(cleaned['subcommittees'], list):
            normalized_subcommittees = []
            for sub in cleaned['subcommittees']:
                if isinstance(sub, dict):
                    normalized_sub = {
                        'name': sub.get('name', ''),
                        'system_code': sub.get('systemCode', ''),
                        'url': sub.get('url', '')
                    }
                    normalized_subcommittees.append(normalized_sub)
            cleaned['subcommittees'] = normalized_subcommittees

        # Ensure jurisdiction is present
        if 'jurisdiction' not in cleaned:
            cleaned['jurisdiction'] = ''

        return cleaned

    def validate_hearing(self, hearing: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate hearing data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'committee', 'date']

        for field in required_fields:
            if field not in hearing:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(hearing))

        is_valid = len(errors) == 0
        self._update_validation_stats('hearing', is_valid)
        return is_valid, errors

    def cleanup_hearing(self, hearing: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize hearing data"""
        cleaned = hearing.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['title', 'committee', 'subcommittee', 'hearing_type', 'location', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure documents is a list
        if 'documents' not in cleaned or not isinstance(cleaned['documents'], list):
            cleaned['documents'] = []
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_house_requirement(self, req: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate house requirement data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'requirement_type', 'date']

        for field in required_fields:
            if field not in req:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(req))

        is_valid = len(errors) == 0
        self._update_validation_stats('house-requirement', is_valid)
        return is_valid, errors

    def cleanup_house_requirement(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize house requirement data"""
        cleaned = req.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['title', 'requirement_type', 'description', 'submitted_by', 'status']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure documents is a list
        if 'documents' not in cleaned or not isinstance(cleaned['documents'], list):
            cleaned['documents'] = []
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_senate_communication(self, comm: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate senate communication data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'communication_type', 'number']

        for field in required_fields:
            if field not in comm:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(comm))

            # Communication number validation
            try:
                comm_num = int(comm['number'])
                if comm_num < 1:
                    errors.append("Communication number must be positive")
            except (ValueError, TypeError):
                errors.append("Communication number must be a valid number")

            # Date validations
            for date_field in ['update_date', 'received_date']:
                if date_field in comm and comm[date_field]:
                    if not self._is_valid_date(comm[date_field]):
                        errors.append(f"Invalid {date_field} format: {comm[date_field]}")

            # Referred to validation
            if 'referred_to' in comm:
                if not isinstance(comm['referred_to'], list):
                    errors.append("referred_to must be a list")
                else:
                    for ref in comm['referred_to']:
                        if not isinstance(ref, dict):
                            errors.append("Each referral must be a dictionary")
                        elif 'committee' not in ref or 'date' not in ref:
                            errors.append("Each referral must contain 'committee' and 'date'")

        is_valid = len(errors) == 0
        self._update_validation_stats('senate-communication', is_valid)
        return is_valid, errors

    def cleanup_senate_communication(self, comm: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize senate communication data"""
        cleaned = comm.copy()

        # Convert numeric fields
        for field in ['congress', 'number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['title', 'description', 'from_agency', 'communication_type']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure referred_to is a list
        if 'referred_to' not in cleaned or not isinstance(cleaned['referred_to'], list):
            cleaned['referred_to'] = []
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_member(self, member: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate member data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'bioguide_id', 'first_name', 'last_name']

        for field in required_fields:
            if field not in member:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(member))

            # Role validation
            valid_roles = ['representative', 'senator', 'delegate', 'commissioner']
            if 'role' in member and member['role'].lower() not in valid_roles:
                errors.append(f"Invalid role: {member['role']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('member', is_valid)
        return is_valid, errors

    def cleanup_member(self, member: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize member data"""
        cleaned = member.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['first_name', 'last_name', 'state', 'party', 'leadership_role']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_summary(self, summary: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate summary data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'text', 'associated_bill']

        for field in required_fields:
            if field not in summary:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(summary))

            # Text validation
            if not isinstance(summary.get('text', ''), str) or not summary.get('text', '').strip():
                errors.append("Text must be a non-empty string")

            # Associated bill validation
            if 'associated_bill' in summary:
                if not isinstance(summary['associated_bill'], dict):
                    errors.append("associated_bill must be a dictionary")
                else:
                    for field in ['congress', 'type', 'number']:
                        if field not in summary['associated_bill']:
                            errors.append(f"Missing {field} in associated_bill")
                        elif field == 'type' and not summary['associated_bill']['type'].strip():
                            errors.append("Bill type cannot be empty")
                        elif field in ['congress', 'number']:
                            try:
                                value = int(summary['associated_bill'][field])
                                if value < 1:
                                    errors.append(f"Invalid {field} in associated_bill: must be positive")
                            except (ValueError, TypeError):
                                errors.append(f"Invalid {field} in associated_bill: must be a number")

            # Version code validation
            if 'version_code' in summary and not summary['version_code'].strip():
                errors.append("Version code cannot be empty")

            # Action date validation
            if 'action_date' in summary and summary['action_date']:
                if not self._is_valid_date(summary['action_date']):
                    errors.append(f"Invalid action_date format: {summary['action_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('summary', is_valid)
        if not is_valid:
            self.logger.warning(f"Summary validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid summary data: {json.dumps(summary, indent=2)}")

        return is_valid, errors

    def cleanup_summary(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize summary data"""
        cleaned = summary.copy()

        # Generate ID if missing
        if 'id' not in cleaned and 'associated_bill' in cleaned:
            bill = cleaned['associated_bill']
            version = cleaned.get('version_code', '00')
            try:
                cleaned['id'] = f"sum_{bill['congress']}_{bill['type'].lower()}_{bill['number']}_{version}"
            except (KeyError, AttributeError) as e:
                self.logger.warning(f"Could not generate summary ID: {e}")
                cleaned['id'] = f"sum_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Map API response fields to schema fields
        field_mapping = {
            'actionDate': 'action_date',
            'updateDate': 'update_date',
            'versionCode': 'version_code',
            'associatedBill': 'associated_bill'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize associated bill if present
        if 'associated_bill' in cleaned and isinstance(cleaned['associated_bill'], dict):
            cleaned['associated_bill'] = {
                'congress': int(cleaned['associated_bill'].get('congress', 0)),
                'type': cleaned['associated_bill'].get('type', '').lower().strip(),
                'number': int(cleaned['associated_bill'].get('number', 0))
            }

        # Normalize text field
        if 'text' in cleaned:
            cleaned['text'] = cleaned['text'].strip()

        # Normalize dates
        for date_field in ['action_date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def validate_committee_print(self, print_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee print data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'print_number', 'committee', 'title']

        for field in required_fields:
            if field not in print_data:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(print_data))

            # Committee validation
            if 'committee' in print_data:
                if not isinstance(print_data['committee'], dict):
                    errors.append("committee must be a dictionary")
                else:
                    for field in ['name', 'system_code']:
                        if field not in print_data['committee']:
                            errors.append(f"Missing {field} in committee")

            # Chamber validation
            if 'chamber' in print_data:
                valid_chambers = ['house', 'senate', 'joint']
                chamber = print_data['chamber'].lower()
                if chamber not in valid_chambers:
                    errors.append(f"Invalid chamber: {print_data['chamber']}. Must be one of: {', '.join(valid_chambers)}")

            # Print number validation
            if 'print_number' in print_data and not print_data['print_number'].strip():
                errors.append("Print number cannot be empty")

            # Date validations
            for date_field in ['date', 'update_date']:
                if date_field in print_data and print_data[date_field]:
                    if not self._is_valid_date(print_data[date_field]):
                        errors.append(f"Invalid {date_field} format: {print_data[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-print', is_valid)
        if not is_valid:
            self.logger.warning(f"Committee print validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid committee print data: {json.dumps(print_data, indent=2)}")

        return is_valid, errors

    def cleanup_committee_print(self, print_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee print data"""
        cleaned = print_data.copy()

        # Normalize chamber
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower().strip()

        # Map API response fields to schema fields
        field_mapping = {
            'printNumber': 'print_number',
            'committeeSystemCode': 'committee_system_code',
            'publishDate': 'date',
            'description': 'description'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize committee data
        if 'committee' in cleaned and isinstance(cleaned['committee'], dict):
            cleaned['committee'] = {
                'name': cleaned['committee'].get('name', '').strip(),
                'system_code': cleaned['committee'].get('system_code', '').strip()
            }

        # Normalize text fields
        for field in ['title', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split()).strip()

        # Ensure dates are in correct format
        for date_field in ['date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def validate_daily_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate daily congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'date', 'volume_number', 'issue_number']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # First apply general congressional record validation
            is_valid, record_errors = self.validate_congressional_record(record)
            errors.extend(record_errors)

            # Additional daily-specific validations
            if 'issue_number' in record:
                try:
                    issue_num = int(record['issue_number'])
                    if issue_num < 1:
                        errors.append("Issue number must be positive")
                except (ValueError, TypeError):
                    errors.append("Issue number must be a valid number")

        is_valid = len(errors) == 0
        self._update_validation_stats('daily-congressional-record', is_valid)
        if not is_valid:
            self.logger.warning(f"Daily congressional record validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid record data: {json.dumps(record, indent=2)}")

        return is_valid, errors

    def validate_bound_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate bound congressional record data structure"""
        # First apply general congressional record validation
        is_valid, errors = self.validate_congressional_record(record)

        # Additional bound-specific validations can be added here if needed

        self._update_validation_stats('bound-congressional-record', is_valid)
        if not is_valid:
            self.logger.warning(f"Bound congressional record validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid record data: {json.dumps(record, indent=2)}")

        return is_valid, errors

    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date string format (YYYY-MM-DD)"""
        if not date_str:
            return False
        try:
            from datetime import datetime
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def get_validation_stats(self) -> Dict[str, Any]:
        """Return current validation statistics"""
        return self.validation_stats.copy()

    def reset_validation_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {}

    def validate_committee_meeting(self, meeting: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee meeting data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'committee', 'date', 'meeting_type', 'location', 'title']

        for field in required_fields:
            if field not in meeting:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(meeting))

            # Committee validation
            if 'committee' in meeting:
                if not isinstance(meeting['committee'], dict):
                    errors.append("Committee must be a dictionary")
                else:
                    for field in ['name', 'system_code']:
                        if field not in meeting['committee']:
                            errors.append(f"Missing required committee field: {field}")

            # Date validation
            if 'date' in meeting and not self._is_valid_date(meeting['date']):
                errors.append(f"Invalid date format: {meeting['date']}")

            # Time validation
            if 'time' in meeting and meeting['time']:
                time_pattern = re.compile(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$')
                if not time_pattern.match(meeting['time']):
                    errors.append(f"Invalid time format: {meeting['time']}")

            # Documents validation
            if 'documents' in meeting:
                if not isinstance(meeting['documents'], list):
                    errors.append("Documents must be a list")
                else:
                    for idx, doc in enumerate(meeting['documents']):
                        if not isinstance(doc, dict):
                            errors.append(f"Document {idx} must be a dictionary")
                        elif 'url' not in doc:
                            errors.append(f"Document {idx} must have a URL")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-meeting', is_valid)
        if not is_valid:
            self.logger.warning(f"Committee meeting validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid meeting data: {json.dumps(meeting, indent=2)}")

        return is_valid, errors

    def cleanup_committee_meeting(self, meeting: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee meeting data"""
        # First apply common cleanup
        cleaned = self._cleanup_common_fields(meeting)

        # Map API response fields to schema fields
        field_mapping = {
            'meetingType': 'meeting_type',
            'committeeCode': 'system_code',
            'documentUrl': 'url'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize committee structure
        if 'committee' in cleaned and isinstance(cleaned['committee'], dict):
            cleaned['committee'] = {
                'name': cleaned['committee'].get('name', ''),
                'system_code': cleaned['committee'].get('systemCode', ''),
                'url': cleaned['committee'].get('url', '')
            }

        # Normalize time format if present
        if 'time' in cleaned and cleaned['time']:
            try:
                # Convert any time format to HH:MM
                time_obj = datetime.strptime(cleaned['time'], '%H:%M').strftime('%H:%M')
                cleaned['time'] = time_obj
            except ValueError:
                self.logger.warning(f"Could not normalize time format: {cleaned['time']}")
                del cleaned['time']

        # Ensure documents is a list with proper structure
        if 'documents' not in cleaned:
            cleaned['documents'] = []
        elif isinstance(cleaned['documents'], list):
            normalized_docs = []
            for doc in cleaned['documents']:
                if isinstance(doc, dict):
                    normalized_doc = {
                        'url': doc.get('url', ''),
                        'type': doc.get('type', ''),
                        'description': doc.get('description', '')
                    }
                    normalized_docs.append(normalized_doc)
            cleaned['documents'] = normalized_docs

        # Ensure type field is set
        cleaned['type'] = 'committee-meeting'

        return cleaned

    def cleanup_daily_congressional_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize daily congressional record data"""
        # First apply general congressional record cleanup
        cleaned = self.cleanup_congressional_record(record)

        # Additional daily-specific cleanup
        # Ensure issue number is present and valid
        if 'issue_number' in cleaned:
            try:
                cleaned['issue_number'] = int(cleaned['issue_number'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert issue_number to integer: {cleaned['issue_number']}")
                cleaned['issue_number'] = 1  # Default for required numeric field

        # Set correct type
        cleaned['type'] = 'daily-congressional-record'

        return cleaned

    def cleanup_bound_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize bound congressional record data"""
        # First apply general congressional record cleanup
        cleaned = self.cleanup_congressional_record(record)

        # Set correct type
        cleaned['type'] = 'bound-congressional-record'

        # Remove issue_number if present (bound records don't have issues)
        if 'issue_number' in cleaned:
            del cleaned['issue_number']

        return cleaned

    def validate_congress(self, congress: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate congress data structure and content"""
        errors = []

        # Required fields
        required_fields = ['id', 'congress', 'type', 'update_date']
        for field in required_fields:
            if field not in congress:
                errors.append(f"Missing required field: {field}")

        if not errors:  # Only proceed with other validations if required fields exist
            # Congress number validation
            try:
                congress_num = int(congress['congress'])
                if congress_num < 1 or congress_num > 150:  # Reasonable range check
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date format validations
            for date_field in ['update_date', 'start_date', 'end_date']:
                if date_field in congress and congress[date_field]:
                    if not self._is_valid_date(congress[date_field]):
                        errors.append(f"Invalid {date_field} format: {congress[date_field]}")

            # Type validation
            if congress.get('type') != 'congress':
                errors.append(f"Invalid type: {congress`.get('type')}. Expected: congress")

            # Validate house and senate data if present
            if 'house' in congress and not isinstance(congress['house'], dict):
                errors.append("House data must be a dictionary")

            if 'senate' in congress and not isinstance(congress['senate'], dict):
                errors.append("Senate data must be a dictionary")

        is_valid = len(errors) == 0
        self._update_validation_stats('congress', is_valid)
        if is_valid:
            self.logger.debug(f"Congress {congress['id']} passed validation")
        else:
            self.logger.warning(f"Congress {congress['id']} failed validation: {', '.join(errors)}")

        return is_valid, errors

    def cleanup_congress(self, congress: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize congress data"""
        cleaned = congress.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize dates to YYYY-MM-DD format
        for date_field in ['update_date', 'start_date', 'end_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Ensure house and senate data are dictionaries if present
        for chamber in ['house', 'senate']:
            if chamber in cleaned and not isinstance(cleaned[chamber], dict):
                cleaned[chamber] = {}

        # Map common field names from API response to schema names
        field_mapping = {
            'updateDate': 'update_date',
            'startDate': 'start_date',
            'endDate': 'end_date',
            'number': 'congress',
            'congressType': 'congress_type'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def cleanup_bill(self, bill: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize bill data"""
        cleaned = bill.copy()

        # Normalize bill type to lowercase
        if 'bill_type' in cleaned:
            cleaned['bill_type'] = cleaned['bill_type'].lower()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Convert bill number to integer
        if 'bill_number' in cleaned:
            try:
                cleaned['bill_number'] = int(cleaned['bill_number'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert bill number to integer: {cleaned['bill_number']}")

        # Clean title (remove extra whitespace)
        if 'title' in cleaned:
            cleaned['title'] = ' '.join(cleaned['title'].split())

        # Normalize chamber names
        if 'origin_chamber' in cleaned and cleaned['origin_chamber']:
            chamber_mapping = {
                'house': 'House',
                'senate': 'Senate',
                'HOUSE': 'House',
                'SENATE': 'Senate'
            }
            cleaned['origin_chamber'] = chamber_mapping.get(cleaned['origin_chamber'], cleaned['origin_chamber'])

        # Ensure lists are present
        list_fields = ['sponsors', 'committees']
        for field in list_fields:
            if field not in cleaned or cleaned[field] is None:
                cleaned[field] = []

        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def cleanup_amendment(self, amendment: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize amendment data"""
        cleaned = amendment.copy()

        # Convert numeric fields
        for field in ['congress', 'number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize type field
        if 'type' in cleaned:
            cleaned['type'] = cleaned['type'].lower().strip()

        # Normalize chamber
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower().strip()

        # Map API response fields to schema fields
        field_mapping = {
            'amendmentNumber': 'number',
            'amendmentType': 'type',
            'purpose': 'purpose',
            'submitDate': 'submit_date',
            'congressionalRecord': 'congressional_record',
            'associatedBill': 'associated_bill'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize associated bill if present
        if 'associated_bill' in cleaned and isinstance(cleaned['associated_bill'], dict):
            cleaned['associated_bill'] = {
                'congress': int(cleaned['associated_bill'].get('congress', 0)),
                'type': cleaned['associated_bill'].get('type', '').lower(),
                'number': int(cleaned['associated_bill'].get('number', 0))
            }

        # Ensure submit_date is in correct format
        if 'submit_date' in cleaned and cleaned['submit_date']:
            cleaned['submit_date'] = self._normalize_date(cleaned['submit_date'])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def cleanup_nomination(self, nomination: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize nomination data"""
        cleaned = nomination.copy()

        # Extract number from citation if present
        if 'citation' in cleaned:
            try:
                number_str = cleaned['citation'].replace('PN', '').split('-')[0].strip()
                cleaned['number'] = int(number_str)
            except (ValueError, IndexError):
                self.logger.warning(f"Could not extract number from citation: {cleaned['citation']}")
                cleaned['number'] = 0  # Default value for invalid number

        # Map API response fields to schema fields
        field_mapping = {
            'latestAction': 'latest_action',
            'nominationType': 'nomination_type',
            'receivedDate': 'received_date',
            'organizationType': 'organization_type'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize text fields
        for field in ['description', 'organization']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split()).strip()

        # Normalize dates
        for date_field in ['received_date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Normalize latest action
        if 'latest_action' in cleaned and isinstance(cleaned['latest_action'], dict):
            cleaned['latest_action'] = {
                'text': cleaned['latest_action'].get('text', '').strip(),
                'action_date': self._normalize_date(cleaned['latest_action'].get('actionDate', ''))
            }

        # Normalize nomination type
        if 'nomination_type' in cleaned:
            cleaned['nomination_type'] = {
                'is_civilian': cleaned['nomination_type'].get('isCivilian', True)
            }

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Ensure type field is set
        cleaned['type'] = 'nomination'

        return cleaned

    def cleanup_treaty(self, treaty: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize treaty data"""
        cleaned = treaty.copy()

        # Map API response fields to schema fields
        field_mapping = {
            'treatyNumber': 'treaty_number',
            'receivedDate': 'received_date',
            'submittedDate': 'submitted_date',
            'description': 'description',
            'subject': 'subject'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize text fields
        for field in ['description', 'subject', 'title', 'country']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split()).strip()

        # Normalize dates to YYYY-MM-DD format
        for date_field in ['received_date', 'submitted_date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def get_validation_stats(self) -> Dict[str, Any]:
        """Return current validation statistics"""
        return self.validation_stats.copy()

    def reset_validation_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {}

    def validate_committee(self, committee: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'name', 'chamber']

        for field in required_fields:
            if field not in committee:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(committee))

            # Committee type validation - map from API response field
            committee_type = committee.get('committee_type') or committee.get('committeeTypeCode', '').lower()
            if committee_type:
                committee['committee_type'] = committee_type
                valid_types = ['standing', 'select', 'joint', 'special', 'subcommittee']
                if committee_type.lower() not in valid_types:
                    errors.append(f"Invalid committee_type: {committee_type}")
            else:
                committee['committee_type'] = 'standing'  # Default type if not specified

            # Chamber validation
            valid_chambers = ['house','senate', 'joint']
            if committee.get('chamber', '').lower() not in valid_chambers:
                errors.append(f"Invalid chamber: {committee.get('chamber')}")

            # Parent committee validation
            if 'parent' in committee:
                if not isinstance(committee['parent'], dict):
                    errors.append("Parent committee must be a dictionary")
                else:
                    required_parent_fields = ['name', 'systemCode']
                    for field in required_parent_fields:
                        if field not in committee['parent']:
                            errors.append(f"Missing required parent committee field: {field}")

            # Subcommittees validation
            if 'subcommittees' in committee:
                if not isinstance(committee['subcommittees'], list):
                    errors.append("Subcommittees must be a list")
                else:
                    for idx, subcommittee in enumerate(committee['subcommittees']):
                        if not isinstance(subcommittee, dict):
                            errors.append(f"Subcommittee {idx} must be a dictionary")
                        else:
                            for field in ['name', 'systemCode']:
                                if field not in subcommittee:
                                    errors.append(f"Missing required field {field} in subcommittee {idx}")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee', is_valid)
        if not is_valid:
            self.logger.warning(f"Committee validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid committee data: {json.dumps(committee, indent=2)}")

        return is_valid, errors

    def cleanup_committee(self, committee: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee data"""
        cleaned = self._cleanup_common_fields(committee.copy())

        # Map API response fields to schema fields
        field_mapping = {
            'committeeTypeCode': 'committee_type',
            'systemCode': 'system_code',
            'parentCommittee': 'parent_committee'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize committee type
        if 'committee_type' in cleaned:
            cleaned['committee_type'] = cleaned['committee_type'].lower()
        else:
            cleaned['committee_type'] = 'standing'  # Default type
        
        # Normalize chamber to lowercase
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower()

        # Clean up parent committee structure
        if 'parent' in cleaned and isinstance(cleaned['parent'], dict):
            cleaned['parent_committee'] = {
                'name': cleaned['parent'].get('name', ''),
                'system_code': cleaned['parent'].get('systemCode', ''),
                'url': cleaned['parent'].get('url', '')
            }
            del cleaned['parent']

        # Ensure subcommittees is a list with proper structure
        if 'subcommittees' not in cleaned:
            cleaned['subcommittees'] = []
        elif isinstance(cleaned['subcommittees'], list):
            normalized_subcommittees = []
            for sub in cleaned['subcommittees']:
                if isinstance(sub, dict):
                    normalized_sub = {
                        'name': sub.get('name', ''),
                        'system_code': sub.get('systemCode', ''),
                        'url': sub.get('url', '')
                    }
                    normalized_subcommittees.append(normalized_sub)
            cleaned['subcommittees'] = normalized_subcommittees

        # Ensure jurisdiction is present
        if 'jurisdiction' not in cleaned:
            cleaned['jurisdiction'] = ''

        return cleaned

    def validate_hearing(self, hearing: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate hearing data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'committee', 'date']

        for field in required_fields:
            if field not in hearing:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(hearing))

        is_valid = len(errors) == 0
        self._update_validation_stats('hearing', is_valid)
        return is_valid, errors

    def cleanup_hearing(self, hearing: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize hearing data"""
        cleaned = hearing.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['title', 'committee', 'subcommittee', 'hearing_type', 'location', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure documents is a list
        if 'documents' not in cleaned or not isinstance(cleaned['documents'], list):
            cleaned['documents'] = []
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_house_requirement(self, req: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate house requirement data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'requirement_type', 'date']

        for field in required_fields:
            if field not in req:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(req))

        is_valid = len(errors) == 0
        self._update_validation_stats('house-requirement', is_valid)
        return is_valid, errors

    def cleanup_house_requirement(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize house requirement data"""
        cleaned = req.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['title', 'requirement_type', 'description', 'submitted_by', 'status']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure documents is a list
        if 'documents' not in cleaned or not isinstance(cleaned['documents'], list):
            cleaned['documents'] = []
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_senate_communication(self, comm: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate senate communication data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'communication_type', 'number']

        for field in required_fields:
            if field not in comm:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(comm))

            # Communication number validation
            try:
                comm_num = int(comm['number'])
                if comm_num < 1:
                    errors.append("Communication number must be positive")
            except (ValueError, TypeError):
                errors.append("Communication number must be a valid number")

            # Date validations
            for date_field in ['update_date', 'received_date']:
                if date_field in comm and comm[date_field]:
                    if not self._is_valid_date(comm[date_field]):
                        errors.append(f"Invalid {date_field} format: {comm[date_field]}")

            # Referred to validation
            if 'referred_to' in comm:
                if not isinstance(comm['referred_to'], list):
                    errors.append("referred_to must be a list")
                else:
                    for ref in comm['referred_to']:
                        if not isinstance(ref, dict):
                            errors.append("Each referral must be a dictionary")
                        elif 'committee' not in ref or 'date' not in ref:
                            errors.append("Each referral must contain 'committee' and 'date'")

        is_valid = len(errors) == 0
        self._update_validation_stats('senate-communication', is_valid)
        return is_valid, errors

    def cleanup_senate_communication(self, comm: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize senate communication data"""
        cleaned = comm.copy()

        # Convert numeric fields
        for field in ['congress', 'number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['title', 'description', 'from_agency', 'communication_type']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure referred_to is a list
        if 'referred_to' not in cleaned or not isinstance(cleaned['referred_to'], list):
            cleaned['referred_to'] = []
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_member(self, member: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate member data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'bioguide_id', 'first_name', 'last_name']

        for field in required_fields:
            if field not in member:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(member))

            # Role validation
            valid_roles = ['representative', 'senator', 'delegate', 'commissioner']
            if 'role' in member and member['role'].lower() not in valid_roles:
                errors.append(f"Invalid role: {member['role']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('member', is_valid)
        return is_valid, errors

    def cleanup_member(self, member: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize member data"""
        cleaned = member.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['first_name', 'last_name', 'state', 'party', 'leadership_role']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_summary(self, summary: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate summary data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'text', 'associated_bill']

        for field in required_fields:
            if field not in summary:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(summary))

            # Text validation
            if not isinstance(summary.get('text', ''), str) or not summary.get('text', '').strip():
                errors.append("Text must be a non-empty string")

            # Associated bill validation
            if 'associated_bill' in summary:
                if not isinstance(summary['associated_bill'], dict):
                    errors.append("associated_bill must be a dictionary")
                else:
                    for field in ['congress', 'type', 'number']:
                        if field not in summary['associated_bill']:
                            errors.append(f"Missing {field} in associated_bill")
                        elif field == 'type' and not summary['associated_bill']['type'].strip():
                            errors.append("Bill type cannot be empty")
                        elif field in ['congress', 'number']:
                            try:
                                value = int(summary['associated_bill'][field])
                                if value < 1:
                                    errors.append(f"Invalid {field} in associated_bill: must be positive")
                            except (ValueError, TypeError):
                                errors.append(f"Invalid {field} in associated_bill: must be a number")

            # Version code validation
            if 'version_code' in summary and not summary['version_code'].strip():
                errors.append("Version code cannot be empty")

            # Action date validation
            if 'action_date' in summary and summary['action_date']:
                if not self._is_valid_date(summary['action_date']):
                    errors.append(f"Invalid action_date format: {summary['action_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('summary', is_valid)
        if not is_valid:
            self.logger.warning(f"Summary validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid summary data: {json.dumps(summary, indent=2)}")

        return is_valid, errors

    def cleanup_summary(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize summary data"""
        cleaned = summary.copy()

        # Generate ID if missing
        if 'id' not in cleaned and 'associated_bill' in cleaned:
            bill = cleaned['associated_bill']
            version = cleaned.get('version_code', '00')
            try:
                cleaned['id'] = f"sum_{bill['congress']}_{bill['type'].lower()}_{bill['number']}_{version}"
            except (KeyError, AttributeError) as e:
                self.logger.warning(f"Could not generate summary ID: {e}")
                cleaned['id'] = f"sum_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Map API response fields to schema fields
        field_mapping = {
            'actionDate': 'action_date',
            'updateDate': 'update_date',
            'versionCode': 'version_code',
            'associatedBill': 'associated_bill'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize associated bill if present
        if 'associated_bill' in cleaned and isinstance(cleaned['associated_bill'], dict):
            cleaned['associated_bill'] = {
                'congress': int(cleaned['associated_bill'].get('congress', 0)),
                'type': cleaned['associated_bill'].get('type', '').lower().strip(),
                'number': int(cleaned['associated_bill'].get('number', 0))
            }

        # Normalize text field
        if 'text' in cleaned:
            cleaned['text'] = cleaned['text'].strip()

        # Normalize dates
        for date_field in ['action_date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def validate_committee_print(self, print_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee print data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'print_number', 'committee', 'title']

        for field in required_fields:
            if field not in print_data:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(print_data))

            # Committee validation
            if 'committee' in print_data:
                if not isinstance(print_data['committee'], dict):
                    errors.append("committee must be a dictionary")
                else:
                    for field in ['name', 'system_code']:
                        if field not in print_data['committee']:
                            errors.append(f"Missing {field} in committee")

            # Chamber validation
            if 'chamber' in print_data:
                valid_chambers = ['house', 'senate', 'joint']
                chamber = print_data['chamber'].lower()
                if chamber not in valid_chambers:
                    errors.append(f"Invalid chamber: {print_data['chamber']}. Must be one of: {', '.join(valid_chambers)}")

            # Print number validation
            if 'print_number' in print_data and not print_data['print_number'].strip():
                errors.append("Print number cannot be empty")

            # Date validations
            for date_field in ['date', 'update_date']:
                if date_field in print_data and print_data[date_field]:
                    if not self._is_valid_date(print_data[date_field]):
                        errors.append(f"Invalid {date_field} format: {print_data[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-print', is_valid)
        if not is_valid:
            self.logger.warning(f"Committee print validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid committee print data: {json.dumps(print_data, indent=2)}")

        return is_valid, errors

    def cleanup_committee_print(self, print_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee print data"""
        cleaned = print_data.copy()

        # Normalize chamber
        if 'chamber' in cleaned:
            cleaned['chamber'] = cleaned['chamber'].lower().strip()

        # Map API response fields to schema fields
        field_mapping = {
            'printNumber': 'print_number',
            'committeeSystemCode': 'committee_system_code',
            'publishDate': 'date',
            'description': 'description'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize committee data
        if 'committee' in cleaned and isinstance(cleaned['committee'], dict):
            cleaned['committee'] = {
                'name': cleaned['committee'].get('name', '').strip(),
                'system_code': cleaned['committee'].get('system_code', '').strip()
            }

        # Normalize text fields
        for field in ['title', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split()).strip()

        # Ensure dates are in correct format
        for date_field in ['date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Remove any empty fields
        cleaned = {k: v for k, v in cleaned.items() if v is not None and v != ''}

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)

        return cleaned

    def validate_daily_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate daily congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'date', 'volume_number', 'issue_number']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # First apply general congressional record validation
            is_valid, record_errors = self.validate_congressional_record(record)
            errors.extend(record_errors)

            # Additional daily-specific validations
            if 'issue_number' in record:
                try:
                    issue_num = int(record['issue_number'])
                    if issue_num < 1:
                        errors.append("Issue number must be positive")
                except (ValueError, TypeError):
                    errors.append("Issue number must be a valid number")

        is_valid = len(errors) == 0
        self._update_validation_stats('daily-congressional-record', is_valid)
        if not is_valid:
            self.logger.warning(f"Daily congressional record validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid record data: {json.dumps(record, indent=2)}")

        return is_valid, errors

    def validate_bound_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate bound congressional record data structure"""
        # First apply general congressional record validation
        is_valid, errors = self.validate_congressional_record(record)

        # Additional bound-specific validations can be added here if needed

        self._update_validation_stats('bound-congressional-record', is_valid)
        if not is_valid:
            self.logger.warning(f"Bound congressional record validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid record data: {json.dumps(record, indent=2)}")

        return is_valid, errors

    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date string format (YYYY-MM-DD)"""
        if not date_str:
            return False
        try:
            from datetime import datetime
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def get_validation_stats(self) -> Dict[str, Any]:
        """Return current validation statistics"""
        return self.validation_stats.copy()

    def reset_validation_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {}

    def validate_committee_meeting(self, meeting: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee meeting data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'committee', 'date', 'meeting_type', 'location', 'title']

        for field in required_fields:
            if field not in meeting:                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(meeting))

            # Committee validation
            if 'committee' in meeting:
                if not isinstance(meeting['committee'], dict):
                    errors.append("Committee must be a dictionary")
                else:
                    for field in ['name', 'system_code']:
                        if field not in meeting['committee']:
                            errors.append(f"Missing required committee field: {field}")

            # Date validation
            if 'date' in meeting and not self._is_valid_date(meeting['date']):
                errors.append(f"Invalid date format: {meeting['date']}")

            # Time validation
            if 'time' in meeting and meeting['time']:
                time_pattern = re.compile(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$')
                if not time_pattern.match(meeting['time']):
                    errors.append(f"Invalid time format: {meeting['time']}")

            # Documents validation
            if 'documents' in meeting:
                if not isinstance(meeting['documents'], list):
                    errors.append("Documents must be a list")
                else:
                    for idx, doc in enumerate(meeting['documents']):
                        if not isinstance(doc, dict):
                            errors.append(f"Document {idx} must be a dictionary")
                        elif 'url' not in doc:
                            errors.append(f"Document {idx} must have a URL")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-meeting', is_valid)
        if not is_valid:
            self.logger.warning(f"Committee meeting validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid meeting data: {json.dumps(meeting, indent=2)}")

        return is_valid, errors

    def cleanup_committee_meeting(self, meeting: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee meeting data"""
        # First apply common cleanup
        cleaned = self._cleanup_common_fields(meeting)

        # Map API response fields to schema fields
        field_mapping = {
            'meetingType': 'meeting_type',
            'committeeCode': 'system_code',
            'documentUrl': 'url'
        }
        for api_field, schema_field in field_mapping.items():
            if api_field in cleaned:
                cleaned[schema_field] = cleaned[api_field]
                del cleaned[api_field]

        # Normalize committee structure
        if 'committee' in cleaned and isinstance(cleaned['committee'], dict):
            cleaned['committee'] = {
                'name': cleaned['committee'].get('name', ''),
                'system_code': cleaned['committee'].get('systemCode', ''),
                'url': cleaned['committee'].get('url', '')
            }

        # Normalize time format if present
        if 'time' in cleaned and cleaned['time']:
            try:
                # Convert any time format to HH:MM
                time_obj = datetime.strptime(cleaned['time'], '%H:%M').strftime('%H:%M')
                cleaned['time'] = time_obj
            except ValueError:
                self.logger.warning(f"Could not normalize time format: {cleaned['time']}")
                del cleaned['time']

        # Ensure documents is a list with proper structure
        if 'documents' not in cleaned:
            cleaned['documents'] = []
        elif isinstance(cleaned['documents'], list):
            normalized_docs = []
            for doc in cleaned['documents']:
                if isinstance(doc, dict):
                    normalized_doc = {
                        'url': doc.get('url', ''),
                        'type': doc.get('type', ''),
                        'description': doc.get('description', '')
                    }
                    normalized_docs.append(normalized_doc)
            cleaned['documents'] = normalized_docs

        # Ensure type field is set
        cleaned['type'] = 'committee-meeting'

        return cleaned

    def cleanup_daily_congressional_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize daily congressional record data"""
        # First apply general congressional record cleanup
        cleaned = self.cleanup_congressional_record(record)

        # Additional daily-specific cleanup
        # Ensure issue number is present and valid
        if 'issue_number' in cleaned:
            try:
                cleaned['issue_number'] = int(cleaned['issue_number'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert issue_number to integer: {cleaned['issue_number']}")
                cleaned['issue_number'] = 1  # Default for required numeric field

        # Set correct type
        cleaned['type'] = 'daily-congressional-record'

        return cleaned

    def cleanup_bound_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize bound congressional record data"""
        # First apply general congressional record cleanup
        cleaned = self.cleanup_congressional_record(record)

        # Set correct type
        cleaned['type'] = 'bound-congressional-record'

        # Remove issue_number if present (bound records don't have issues)
        if 'issue_number' in cleaned:
            del cleaned['issue_number']

        return cleaned