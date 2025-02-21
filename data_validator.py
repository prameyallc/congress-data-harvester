import re
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging

class DataValidator:
    """Validates and cleanups Congress.gov data before storage"""

    def __init__(self):
        self.logger = logging.getLogger('congress_downloader')
        self.validation_stats = {
            'total_processed': 0,
            'total_valid': 0,
            'total_invalid': 0
        }

    def validate_bill(self, bill: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate bill data structure and content
        Returns: (is_valid, list of validation errors)
        """
        self.validation_stats['total_processed'] += 1
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

            # Chamber code validation
            if 'origin_chamber_code' in bill:
                valid_chamber_codes = ['H', 'S']
                if bill['origin_chamber_code'] not in valid_chamber_codes:
                    errors.append(f"Invalid origin_chamber_code: {bill['origin_chamber_code']}")

            # URL validation
            if 'url' in bill and bill['url']:
                if not bill['url'].startswith('https://api.congress.gov/'):
                    errors.append("Invalid API URL format")

        is_valid = len(errors) == 0
        if is_valid:
            self.validation_stats['total_valid'] += 1
            self.logger.debug(f"Bill {bill['id']} passed validation")
        else:
            self.validation_stats['total_invalid'] += 1
            self.logger.warning(f"Bill {bill['id']} failed validation: {', '.join(errors)}")

        return (is_valid, errors)

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

        # Ensure latest action is a dictionary with required fields
        if 'latest_action' in cleaned:
            if isinstance(cleaned['latest_action'], str):
                cleaned['latest_action'] = {'text': cleaned['latest_action'], 'action_date': ''}
            elif cleaned['latest_action'] is None:
                cleaned['latest_action'] = {'text': '', 'action_date': ''}
            elif not isinstance(cleaned['latest_action'], dict):
                cleaned['latest_action'] = {'text': str(cleaned['latest_action']), 'action_date': ''}

        # Ensure lists are present
        list_fields = ['sponsors', 'committees']
        for field in list_fields:
            if field not in cleaned or cleaned[field] is None:
                cleaned[field] = []

        self.logger.debug(f"Cleaned and normalized bill {cleaned['id']}")

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

    def validate_sponsor(self, sponsor: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate sponsor data structure"""
        errors = []
        required_fields = ['bioguideId', 'firstName', 'lastName']

        for field in required_fields:
            if field not in sponsor:
                errors.append(f"Missing required sponsor field: {field}")

        return (len(errors) == 0, errors)

    def validate_committee(self, committee: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate committee data structure"""
        errors = []
        required_fields = ['systemCode', 'name', 'chamber']

        for field in required_fields:
            if field not in committee:
                errors.append(f"Missing required committee field: {field}")

        if 'chamber' in committee:
            valid_chambers = ['House', 'Senate', 'Joint']
            if committee['chamber'] not in valid_chambers:
                errors.append(f"Invalid chamber: {committee['chamber']}")

        return (len(errors) == 0, errors)

    def get_validation_stats(self) -> Dict[str, int]:
        """Return current validation statistics"""
        return self.validation_stats.copy()

    def reset_validation_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {
            'total_processed': 0,
            'total_valid': 0,
            'total_invalid': 0
        }