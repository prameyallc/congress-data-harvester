#!/usr/bin/env python3

"""
Fix broken Congress API implementation
"""

import os
import re
import json
import logging
from typing import Dict, Any

def fix_get_available_endpoints():
    """Fix the missing get_available_endpoints method in CongressAPI"""
    with open('congress_api.py', 'r') as file:
        content = file.readlines()
    
    # Find the CongressAPI class
    class_start_line = None
    for i, line in enumerate(content):
        if line.strip() == 'class CongressAPI(CongressBaseAPI):':
            class_start_line = i
            break
    
    if class_start_line is None:
        logging.error("Could not find CongressAPI class")
        return False
    
    # Add proper indentation to the method content
    method_content = """
    def get_available_endpoints(self) -> Dict[str, Any]:
        \"\"\"Get list of available API endpoints and their details\"\"\"
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
            return {
                'endpoints': available_endpoints,
                'endpoint_count': len(available_endpoints)
            }
        except Exception as e:
            self.logger.error(f"Failed to get available endpoints: {str(e)}")
            return {
                'endpoints': {},
                'endpoint_count': 0,
                'error': str(e)
            }
"""
    
    # Check if the method already exists
    for i in range(class_start_line, len(content)):
        if "def get_available_endpoints" in content[i]:
            # Method already exists - replace it
            method_end = i
            # Find method end
            brackets_count = 0
            for j in range(i, len(content)):
                if "{" in content[j]:
                    brackets_count += content[j].count("{")
                if "}" in content[j]:
                    brackets_count -= content[j].count("}")
                if "def " in content[j] and j > i and brackets_count <= 0:
                    method_end = j
                    break
            
            # Replace the existing method
            content[i:method_end] = method_content.split('\n')
            break
    else:
        # Method doesn't exist - add it
        docstring_end = None
        for i in range(class_start_line + 1, min(class_start_line + 20, len(content))):
            if '"""' in content[i]:
                docstring_end = i
                break
        
        if docstring_end:
            content.insert(docstring_end + 1, method_content)
        else:
            content.insert(class_start_line + 1, method_content)
    
    # Write back the modified content
    with open('congress_api.py', 'w') as file:
        file.writelines(content)
    
    print("Fixed get_available_endpoints method in CongressAPI")
    return True

def fix_generate_committee_id():
    """Fix the missing _generate_committee_id method in CongressAPI"""
    with open('congress_api.py', 'r') as file:
        content = file.readlines()
    
    # Find the CongressAPI class
    class_start_line = None
    for i, line in enumerate(content):
        if line.strip() == 'class CongressAPI(CongressBaseAPI):':
            class_start_line = i
            break
    
    if class_start_line is None:
        logging.error("Could not find CongressAPI class")
        return False
    
    # Check if the method already exists
    for i in range(class_start_line, len(content)):
        if "def _generate_committee_id" in content[i]:
            print("_generate_committee_id method already exists")
            return True
    
    # Add the method
    method_content = """
    def _generate_committee_id(self, committee: Dict, current_congress: int) -> Optional[str]:
        \"\"\"Generate a committee ID from committee data\"\"\"
        try:
            # Extract committee identifiers
            chamber = committee.get('chamber', '').lower()
            name = committee.get('name', '')
            committee_code = committee.get('systemCode', '')
            
            # Validate required fields
            if not all([chamber, name]) and not committee_code:
                self.logger.warning(
                    f"Missing required fields for committee ID generation: "
                    f"chamber={chamber}, name={name}, code={committee_code}"
                )
                return None
                
            # Prefer using the system code if available
            if committee_code:
                committee_id = f"comm_{current_congress}_{committee_code}"
            else:
                # Create a slug from the committee name
                committee_slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
                committee_id = f"comm_{current_congress}_{chamber}_{committee_slug[:30]}"
                
            self.logger.debug(f"Generated committee ID: {committee_id}")
            return committee_id
                
        except Exception as e:
            self.logger.error(f"Failed to generate committee ID: {str(e)}")
            return None
"""
    
    # Find a good place to insert the method - after an existing method
    for i in range(class_start_line, len(content)):
        if "def _generate_" in content[i]:
            # Find the end of this method
            method_end = i
            for j in range(i + 1, len(content)):
                if "def " in content[j]:
                    method_end = j - 1
                    break
            
            # Insert our new method after this one
            content.insert(method_end + 1, method_content)
            break
    else:
        # No existing method found, insert after class definition
        content.insert(class_start_line + 1, method_content)
    
    # Write back the modified content
    with open('congress_api.py', 'w') as file:
        file.writelines(content)
    
    print("Added _generate_committee_id method to CongressAPI")
    return True

def fix_urllib3_import():
    """Fix the urllib3.util import issue"""
    with open('congress_api.py', 'r') as file:
        content = file.readlines()
    
    # Find the import line
    for i, line in enumerate(content):
        if "from urllib3.util import Retry" in line:
            # Replace with a safer import
            content[i] = "try:\n    from urllib3.util.retry import Retry\nexcept ImportError:\n    from urllib3.util import Retry\n"
            break
    
    # Write back the modified content
    with open('congress_api.py', 'w') as file:
        file.writelines(content)
    
    print("Fixed urllib3.util import in congress_api.py")
    return True

def fix_return_none_typos():
    """Fix 'return Non' typos in congress_api.py"""
    with open('congress_api.py', 'r') as file:
        content = file.read()
    
    # Replace 'return Non' with 'return None'
    fixed_content = re.sub(r'return\s+Non\b', 'return None', content)
    
    # Write back the modified content
    with open('congress_api.py', 'w') as file:
        file.write(fixed_content)
    
    print("Fixed 'return Non' typos in congress_api.py")
    return True

def main():
    print("Starting fixes for Congress API...")
    
    # Fix the urllib3 import issue
    fix_urllib3_import()
    
    # Fix 'return Non' typos
    fix_return_none_typos()
    
    # Fix missing methods
    fix_get_available_endpoints()
    fix_generate_committee_id()
    
    print("All fixes applied!")

if __name__ == "__main__":
    main()