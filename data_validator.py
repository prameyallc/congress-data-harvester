import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
import json

class DataValidator:
    """Validates and cleanups Congress.gov data before storage"""

    def __init__(self):
        self.logger = logging.getLogger('congress_downloader')
        self.validation_stats = {'total_processed': 0, 'by_type': {}}

    def _update_validation_stats(self, record_type: str, is_valid: bool) -> None:
        """Update validation statistics for monitoring"""
        if 'by_type' not in self.validation_stats:
            self.validation_stats['by_type'] = {}
            
        if record_type not in self.validation_stats['by_type']:
            self.validation_stats['by_type'][record_type] = {
                'processed': 0,
                'valid': 0,
                'invalid': 0
            }

        self.validation_stats['by_type'][record_type]['processed'] += 1
        if is_valid:
            self.validation_stats['by_type'][record_type]['valid'] += 1
        else:
            self.validation_stats['by_type'][record_type]['invalid'] += 1

    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date string format"""
        if not date_str:
            return False
        try:
            # Handle ISO format with time zone  
            if 'T' in date_str:
                if 'Z' in date_str:
                    datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
                else:
                    datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
                return True
            else:
                datetime.strptime(date_str, '%Y-%m-%d')
                return True
        except ValueError:
            return False

    def get_validation_stats(self) -> Dict[str, Any]:
        """Get current validation statistics"""
        return self.validation_stats

    def reset_validation_stats(self) -> None:
        """Reset validation statistics"""
        self.validation_stats = {'total_processed': 0, 'by_type': {}}

    def _normalize_date(self, date_str: str) -> str:
        """Convert any valid date format to YYYY-MM-DD"""
        try:
            if 'T' in date_str:
                if 'Z' in date_str:
                    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
                else:
                    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            else:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
        except ValueError as e:
            self.logger.error(f"Failed to normalize date {date_str}: {str(e)}")
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

    # Stub methods for validators
    def validate_bill(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for bill validation"""
        return True, []

    def validate_amendment(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for amendment validation"""
        return True, []

    def validate_nomination(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for nomination validation"""
        return True, []

    def validate_treaty(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for treaty validation"""
        return True, []

    def validate_committee_report(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for committee report validation"""
        return True, []

    def validate_congressional_record(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for congressional record validation"""
        return True, []

    def validate_house_communication(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for house communication validation"""
        return True, []

    def validate_committee(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for committee validation"""
        return True, []

    def validate_hearing(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for hearing validation"""
        return True, []

    def validate_senate_communication(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for senate communication validation"""
        return True, []

    def validate_member(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for member validation"""
        return True, []

    def validate_summary(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for summary validation"""
        return True, []

    def validate_committee_print(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for committee print validation"""
        return True, []

    def validate_committee_meeting(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for committee meeting validation"""
        return True, []

    def validate_congress(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Stub for congress validation"""
        return True, []

    # Stubs for cleanup methods
    def cleanup_bill(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for bill cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_amendment(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for amendment cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_nomination(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for nomination cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_treaty(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for treaty cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_committee_report(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for committee report cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_congressional_record(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for congressional record cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_house_communication(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for house communication cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_committee(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for committee cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_hearing(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for hearing cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_senate_communication(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for senate communication cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_member(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for member cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_summary(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for summary cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_committee_print(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for committee print cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_committee_meeting(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for committee meeting cleanup"""
        return self._cleanup_common_fields(data)

    def cleanup_congress(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Stub for congress cleanup"""
        return self._cleanup_common_fields(data)

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
        return False, [f"Unknown data type: {data_type}"]

    def cleanup_data(self, data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
        """
        Cleanup data based on its type
        Returns cleaned data ready for storage
        """
        # Route to appropriate cleanup function
        cleanup_functions = {
            'bill': self.cleanup_bill,
            'amendment': self.cleanup_amendment,
            'nomination': self.cleanup_nomination,
            'treaty': self.cleanup_treaty,
            'committee-report': self.cleanup_committee_report,
            'congressional-record': self.cleanup_congressional_record,
            'house-communication': self.cleanup_house_communication,
            'committee': self.cleanup_committee,
            'hearing': self.cleanup_hearing,
            'house-requirement': self.cleanup_house_requirement,
            'senate-communication': self.cleanup_senate_communication,
            'member': self.cleanup_member,
            'summaries': self.cleanup_summary,
            'committee-print': self.cleanup_committee_print,
            'daily-congressional-record': self.cleanup_daily_congressional_record,
            'bound-congressional-record': self.cleanup_bound_record,
            'committee-meeting': self.cleanup_committee_meeting,
            'congress': self.cleanup_congress
        }

        if data_type in cleanup_functions:
            return cleanup_functions[data_type](data)
        else:
            self.logger.warning(f"No cleanup function for data type: {data_type}")
            return data

    def validate_daily_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate daily congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'date', 'year', 'month', 'day']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(record))

            # Validate year, month, day
            try:
                year = int(record['year'])
                if year < 1789 or year > datetime.now().year:
                    errors.append(f"Invalid year: {year}")

                month = int(record['month'])
                if month < 1 or month > 12:
                    errors.append(f"Invalid month: {month}")

                day = int(record['day'])
                if day < 1 or day > 31:
                    errors.append(f"Invalid day: {day}")

                # Validate that date matches year/month/day
                if 'date' in record and record['date']:
                    date_parts = record['date'].split('-')
                    if len(date_parts) == 3:
                        date_year = int(date_parts[0])
                        date_month = int(date_parts[1])
                        date_day = int(date_parts[2])
                        if date_year != year or date_month != month or date_day != day:
                            errors.append(f"Date {record['date']} does not match year/month/day: {year}/{month}/{day}")
            except (ValueError, TypeError):
                errors.append("Year, month, and day must be valid numbers")

            # Date validations
            for date_field in ['date', 'update_date']:
                if date_field in record and record[date_field]:
                    if not self._is_valid_date(record[date_field]):
                        errors.append(f"Invalid {date_field} format: {record[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('daily-congressional-record', is_valid)
        if not is_valid:
            self.logger.warning(f"Daily congressional record validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid record data: {json.dumps(record, indent=2)}")

        return is_valid, errors

    def cleanup_daily_congressional_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize daily congressional record data"""
        cleaned = record.copy()

        # Convert numeric fields
        for field in ['congress', 'year', 'month', 'day']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['title', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Normalize dates
        for date_field in ['date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_bound_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate bound congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'volume', 'year']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(record))

            # Validate volume
            try:
                volume = int(record['volume'])
                if volume < 1:
                    errors.append(f"Invalid volume: {volume}")
            except (ValueError, TypeError):
                errors.append("Volume must be a valid number")

            # Validate year
            try:
                year = int(record['year'])
                if year < 1789 or year > datetime.now().year:
                    errors.append(f"Invalid year: {year}")
            except (ValueError, TypeError):
                errors.append("Year must be a valid number")

            # Validate month if present
            if 'month' in record and record['month']:
                try:
                    month = int(record['month'])
                    if month < 1 or month > 12:
                        errors.append(f"Invalid month: {month}")
                except (ValueError, TypeError):
                    errors.append("Month must be a valid number")

            # Date validations
            for date_field in ['update_date']:
                if date_field in record and record[date_field]:
                    if not self._is_valid_date(record[date_field]):
                        errors.append(f"Invalid {date_field} format: {record[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('bound-congressional-record', is_valid)
        if not is_valid:
            self.logger.warning(f"Bound congressional record validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid record data: {json.dumps(record, indent=2)}")

        return is_valid, errors

    def cleanup_bound_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize bound congressional record data"""
        cleaned = record.copy()

        # Convert numeric fields
        for field in ['congress', 'volume', 'year', 'month', 'part']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['title', 'description', 'page_range']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Normalize dates
        for date_field in ['update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned

    def validate_house_requirement(self, requirement: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate house requirement data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'title', 'category']

        for field in required_fields:
            if field not in requirement:
                errors.append(f"Missing required field: {field}")

        if not errors:
            errors.extend(self._validate_common_fields(requirement))

            # Validate category
            if 'category' in requirement and not requirement['category'].strip():
                errors.append("Category cannot be empty")

            # Validate title
            if 'title' in requirement and not requirement['title'].strip():
                errors.append("Title cannot be empty")

            # Date validations
            for date_field in ['date', 'update_date']:
                if date_field in requirement and requirement[date_field]:
                    if not self._is_valid_date(requirement[date_field]):
                        errors.append(f"Invalid {date_field} format: {requirement[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('house-requirement', is_valid)
        if not is_valid:
            self.logger.warning(f"House requirement validation failed: {', '.join(errors)}")
            self.logger.debug(f"Invalid requirement data: {json.dumps(requirement, indent=2)}")

        return is_valid, errors

    def cleanup_house_requirement(self, requirement: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize house requirement data"""
        cleaned = requirement.copy()

        # Convert numeric fields
        for field in ['congress']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['title', 'category', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Normalize dates
        for date_field in ['date', 'update_date']:
            if date_field in cleaned and cleaned[date_field]:
                cleaned[date_field] = self._normalize_date(cleaned[date_field])

        # Apply common cleanup
        cleaned = self._cleanup_common_fields(cleaned)
        return cleaned