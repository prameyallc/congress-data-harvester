import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging

class DataValidator:
    """Validates and cleanups Congress.gov data before storage"""

    def __init__(self):
        self.logger = logging.getLogger('congress_downloader')
        self.validation_stats = {
            'total_processed': 0,
            'total_valid': 0,
            'total_invalid': 0,
            'by_type': {}
        }

    def validate_data(self, data: Dict[str, Any], data_type: str) -> Tuple[bool, List[str]]:
        """
        Validate data based on its type
        Returns: (is_valid, list of validation errors)
        """
        self.validation_stats['total_processed'] += 1

        # Initialize stats for this type if not exists
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
            'daily-congressional-record': self.validate_congressional_record_daily,
            'bound-congressional-record': self.validate_bound_congressional_record,
            'committee-meeting': self.validate_committee_meeting
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
        if is_valid:
            self.validation_stats['total_valid'] += 1
            self.validation_stats['by_type']['bill']['valid'] += 1
            self.logger.debug(f"Bill {bill['id']} passed validation")
        else:
            self.validation_stats['total_invalid'] += 1
            self.validation_stats['by_type']['bill']['invalid'] += 1
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
            # Congress number validation
            try:
                congress_num = int(amendment['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Amendment number validation
            try:
                amdt_num = int(amendment['number'])
                if amdt_num < 1:
                    errors.append("Amendment number must be positive")
            except (ValueError, TypeError):
                errors.append("Amendment number must be a valid number")

            # Date validation
            if not self._is_valid_date(amendment['update_date']):
                errors.append(f"Invalid update_date format: {amendment['update_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('amendment', is_valid)
        return is_valid, errors

    def validate_nomination(self, nomination: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate nomination data structure"""
        errors = []
        required_fields = ['id', 'congress', 'nomination_number', 'update_date']

        for field in required_fields:
            if field not in nomination:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(nomination['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Nomination number validation
            try:
                nom_num = int(nomination['nomination_number'])
                if nom_num < 1:
                    errors.append("Nomination number must be positive")
            except (ValueError, TypeError):
                errors.append("Nomination number must be a valid number")

            # Date validation
            if not self._is_valid_date(nomination['update_date']):
                errors.append(f"Invalid update_date format: {nomination['update_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('nomination', is_valid)
        return is_valid, errors

    def validate_treaty(self, treaty: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate treaty data structure"""
        errors = []
        required_fields = ['id', 'congress', 'treaty_number', 'update_date']

        for field in required_fields:
            if field not in treaty:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(treaty['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Treaty number validation
            try:
                treaty_num = int(treaty['treaty_number'])
                if treaty_num < 1:
                    errors.append("Treaty number must be positive")
            except (ValueError, TypeError):
                errors.append("Treaty number must be a valid number")

            # Date validation
            if not self._is_valid_date(treaty['update_date']):
                errors.append(f"Invalid update_date format: {treaty['update_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('treaty', is_valid)
        return is_valid, errors

    def validate_committee_report(self, report: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee report data structure"""
        errors = []
        required_fields = ['id', 'congress', 'report_number', 'update_date', 'committee']

        for field in required_fields:
            if field not in report:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(report['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Report number validation
            try:
                report_num = int(report['report_number'])
                if report_num < 1:
                    errors.append("Report number must be positive")
            except (ValueError, TypeError):
                errors.append("Report number must be a valid number")

            # Date validation
            if not self._is_valid_date(report['update_date']):
                errors.append(f"Invalid update_date format: {report['update_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-report', is_valid)
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

        return cleaned

    def validate_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'record_type', 'date']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(record['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validations
            for date_field in ['update_date', 'date']:
                if not self._is_valid_date(record[date_field]):
                    errors.append(f"Invalid {date_field} format: {record[date_field]}")

            # Pages validation
            if 'pages' in record:
                pages = record['pages']
                if not isinstance(pages, dict):
                    errors.append("Pages must be a dictionary")
                else:
                    if 'start' not in pages or 'end' not in pages:
                        errors.append("Pages must contain 'start' and 'end' fields")

        is_valid = len(errors) == 0
        self._update_validation_stats('congressional-record', is_valid)
        return is_valid, errors

    def cleanup_congressional_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize congressional record data"""
        cleaned = record.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['title', 'record_type', 'session']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure pages structure
        if 'pages' not in cleaned or not isinstance(cleaned['pages'], dict):
            cleaned['pages'] = {'start': '', 'end': ''}

        return cleaned

    def validate_house_communication(self, comm: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate house communication data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'communication_type', 'number']

        for field in required_fields:
            if field not in comm:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(comm['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

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

        return cleaned

    def _update_validation_stats(self, data_type: str, is_valid: bool):
        """Update validation statistics for a specific data type"""
        if is_valid:
            self.validation_stats['total_valid'] += 1
            self.validation_stats['by_type'][data_type]['valid'] += 1
        else:
            self.validation_stats['total_invalid'] += 1
            self.validation_stats['by_type'][data_type]['invalid'] += 1

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
            return self.cleanup_bound_congressional_record(data)
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

        # Normalize text fields
        if 'purpose' in cleaned:
            cleaned['purpose'] = ' '.join(cleaned['purpose'].split())

        return cleaned

    def cleanup_nomination(self, nomination: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize nomination data"""
        cleaned = nomination.copy()

        # Convert numeric fields
        for field in ['congress', 'nomination_number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        if 'description' in cleaned:
            cleaned['description'] = ' '.join(cleaned['description'].split())

        return cleaned

    def cleanup_treaty(self, treaty: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize treaty data"""
        cleaned = treaty.copy()

        # Convert numeric fields
        for field in ['congress', 'treaty_number']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        if 'title' in cleaned:
            cleaned['title'] = ' '.join(cleaned['title'].split())

        return cleaned

    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date string format (YYYY-MM-DD)"""
        try:
            if not date_str:
                return False
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    def get_validation_stats(self) -> Dict[str, Any]:
        """Return current validation statistics"""
        return self.validation_stats.copy()

    def reset_validation_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {
            'total_processed': 0,
            'total_valid': 0,
            'total_invalid': 0,
            'by_type': {}
        }

    def validate_committee(self, committee: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'name', 'chamber']

        for field in required_fields:
            if field not in committee:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(committee['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validation
            if not self._is_valid_date(committee['update_date']):
                errors.append(f"Invalid update_date format: {committee['update_date']}")

            # Subcommittees validation
            if 'subcommittees' in committee:
                if not isinstance(committee['subcommittees'], list):
                    errors.append("Subcommittees must be a list")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee', is_valid)
        return is_valid, errors

    def cleanup_committee(self, committee: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee data"""
        cleaned = committee.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['name', 'committee_type']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure subcommittees is a list
        if 'subcommittees' not in cleaned or not isinstance(cleaned['subcommittees'], list):
            cleaned['subcommittees'] = []

        # Ensure parent_committee is a dictionary
        if 'parent_committee' not in cleaned or not isinstance(cleaned['parent_committee'], dict):
            cleaned['parent_committee'] = {}

        return cleaned

    def validate_hearing(self, hearing: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate hearing data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'committee', 'date']

        for field in required_fields:
            if field not in hearing:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(hearing['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validations
            for date_field in ['update_date', 'date']:
                if not self._is_valid_date(hearing[date_field]):
                    errors.append(f"Invalid {date_field} format: {hearing[date_field]}")

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

        return cleaned

    def validate_house_requirement(self, req: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate house requirement data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'requirement_type', 'date']

        for field in required_fields:
            if field not in req:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(req['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validations
            for date_field in ['update_date', 'date']:
                if not self._is_valid_date(req[date_field]):
                    errors.append(f"Invalid {date_field} format: {req[date_field]}")

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

        return cleaned

    def validate_senate_communication(self, comm: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate senate communication data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'communication_type', 'number']

        for field in required_fields:
            if field not in comm:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(comm['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

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

        return cleaned

    def validate_member(self, member: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate member data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'bioguide_id', 'first_name', 'last_name']

        for field in required_fields:
            if field not in member:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(member['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number:{congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validation
            if not self._is_valid_date(member['update_date']):
                errors.append(f"Invalid update_date format: {member['update_date']}")

            # Served until date validation if present
            if member.get('served_until'):
                if not self._is_valid_date(member['served_until']):
                    errors.append(f"Invalid served_until format: {member['served_until']}")

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

        return cleaned

    def validate_summary(self, summary: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate summary data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'bill_id', 'version', 'text']

        for field in required_fields:
            if field not in summary:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(summary['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validation
            if not self._is_valid_date(summary['update_date']):
                errors.append(f"Invalid update_date format: {summary['update_date']}")

            # Version validation
            if not isinstance(summary['version'], int) or summary['version'] < 1:
                errors.append("Version must be a positive integer")

            # Text validation
            if not isinstance(summary['text'], str) or not summary['text'].strip():
                errors.append("Text must be a non-empty string")

        is_valid = len(errors) == 0
        self._update_validation_stats('summary', is_valid)
        return is_valid, errors

    def cleanup_summary(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize summary data"""
        cleaned = summary.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Convert version to integer
        if 'version' in cleaned:
            try:
                cleaned['version'] = int(cleaned['version'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert version to integer: {cleaned['version']}")

        # Normalize text fields
        if 'text' in cleaned:
            cleaned['text'] = ' '.join(cleaned['text'].split())

        # Ensure related_bills is a list
        if 'related_bills' not in cleaned or not isinstance(cleaned['related_bills'], list):
            cleaned['related_bills'] = []

        return cleaned

    def validate_committee_print(self, print_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee print data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'committee', 'chamber']

        for field in required_fields:
            if field not in print_data:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(print_data['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validation
            if not self._is_valid_date(print_data['update_date']):
                errors.append(f"Invalid update_date format: {print_data['update_date']}")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-print', is_valid)
        return is_valid, errors

    def validate_congressional_record_daily(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate daily congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'date', 'type']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(record['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validations
            for date_field in ['update_date', 'date']:
                if not self._is_valid_date(record[date_field]):
                    errors.append(f"Invalid {date_field} format: {record[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('daily-congressional-record', is_valid)
        return is_valid, errors

    def validate_bound_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate bound congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'volume', 'pages']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(record['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validation
            if not self._is_valid_date(record['update_date']):
                errors.append(f"Invalid update_date format: {record['update_date']}")

            # Pages validation
            if 'pages' in record:
                pages = record['pages']
                if not isinstance(pages, dict):
                    errors.append("Pages must be a dictionary")
                else:
                    if 'start' not in pages or 'end' not in pages:
                        errors.append("Pages must contain 'start' and 'end' fields")

        is_valid = len(errors) == 0
        self._update_validation_stats('bound-congressional-record', is_valid)
        return is_valid, errors

    def cleanup_committee_print(self, print_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee print data"""
        cleaned = print_data.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['title', 'chamber']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure committee is a dictionary
        if 'committee' not in cleaned or not isinstance(cleaned['committee'], dict):
            cleaned['committee'] = {'name': '', 'system_code': '', 'url': ''}

        # Ensure text_versions is a list
        if 'text_versions' not in cleaned or not isinstance(cleaned['text_versions'], list):
            cleaned['text_versions'] = []

        return cleaned
    def validate_committee_meeting(self, meeting: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate committee meeting data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'committee', 'date', 'meeting_type']

        for field in required_fields:
            if field not in meeting:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(meeting['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validations
            for date_field in ['update_date', 'date']:
                if not self._is_valid_date(meeting[date_field]):
                    errors.append(f"Invalid {date_field} format: {meeting[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('committee-meeting', is_valid)
        return is_valid, errors

    def cleanup_committee_meeting(self, meeting: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize committee meeting data"""
        cleaned = meeting.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['title', 'committee', 'subcommittee', 'meeting_type', 'location', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure documents is a list
        if 'documents' not in cleaned or not isinstance(cleaned['documents'], list):
            cleaned['documents'] = []

        return cleaned

    def validate_daily_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate daily congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'chamber', 'date']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(record['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validations
            for date_field in ['update_date', 'date']:
                if not self._is_valid_date(record[date_field]):
                    errors.append(f"Invalid {date_field} format: {record[date_field]}")

        is_valid = len(errors) == 0
        self._update_validation_stats('daily-congressional-record', is_valid)
        return is_valid, errors

    def cleanup_daily_congressional_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize daily congressional record data"""
        cleaned = record.copy()

        # Convert congress to integer
        if 'congress' in cleaned:
            try:
                cleaned['congress'] = int(cleaned['congress'])
            except (ValueError, TypeError):
                self.logger.warning(f"Could not convert congress to integer: {cleaned['congress']}")

        # Normalize text fields
        for field in ['chamber', 'description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure pages is a list
        if 'pages' not in cleaned or not isinstance(cleaned['pages'], list):
            cleaned['pages'] = []

        return cleaned

    def validate_bound_congressional_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate bound congressional record data structure"""
        errors = []
        required_fields = ['id', 'congress', 'update_date', 'volume', 'date']

        for field in required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        if not errors:
            # Congress number validation
            try:
                congress_num = int(record['congress'])
                if congress_num < 1 or congress_num > 150:
                    errors.append(f"Invalid congress number: {congress_num}")
            except (ValueError, TypeError):
                errors.append("Congress must be a valid number")

            # Date validation
            for date_field in ['update_date', 'date']:
                if not self._is_valid_date(record[date_field]):
                    errors.append(f"Invalid {date_field} format: {record[date_field]}")

            # Volume validation
            if 'volume' in record:
                try:
                    vol_num = int(record['volume'])
                    if vol_num < 1:
                        errors.append("Volume number must be positive")
                except (ValueError, TypeError):
                    errors.append("Volume must be a valid number")

        is_valid = len(errors) == 0
        self._update_validation_stats('bound-congressional-record', is_valid)
        return is_valid, errors

    def cleanup_bound_congressional_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize bound congressional record data"""
        cleaned = record.copy()

        # Convert numeric fields
        for field in ['congress', 'volume']:
            if field in cleaned:
                try:
                    cleaned[field] = int(cleaned[field])
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field} to integer: {cleaned[field]}")

        # Normalize text fields
        for field in ['description']:
            if field in cleaned:
                cleaned[field] = ' '.join(cleaned[field].split())

        # Ensure pages is a list
        if 'pages' not in cleaned or not isinstance(cleaned['pages'], list):
            cleaned['pages'] = []

        return cleaned