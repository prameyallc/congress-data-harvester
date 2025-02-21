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
        if data_type == 'bill':
            return self.validate_bill(data)
        elif data_type == 'amendment':
            return self.validate_amendment(data)
        elif data_type == 'nomination':
            return self.validate_nomination(data)
        elif data_type == 'treaty':
            return self.validate_treaty(data)
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