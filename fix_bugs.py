#!/usr/bin/env python3
import os
import re

def fix_typos_in_file(file_path):
    """Fix 'return Non' typos in specified file"""
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Replace 'return Non' with 'return None'
    fixed_content = re.sub(r'return\s+Non\b', 'return None', content)
    
    with open(file_path, 'w') as file:
        file.write(fixed_content)

def fix_string_get_bug(file_path):
    """Fix string .get() bug in process_bill method"""
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Find all _process_bill methods and add string type checking
    pattern = r'(def _process_bill.*?bill_data = bill\.get\(.*?\))'
    
    def add_string_check(match):
        method_start = match.group(1)
        if "isinstance(bill_data, str)" not in method_start:
            replacement = method_start + "\n            if isinstance(bill_data, str):\n                self.logger.error(f\"Received string instead of dictionary for bill: {bill_data}\")\n                return None"
            return replacement
        return match.group(0)
    
    fixed_content = re.sub(pattern, add_string_check, content, flags=re.DOTALL)
    
    with open(file_path, 'w') as file:
        file.write(fixed_content)

def main():
    print("Starting bug fixes...")
    file_path = 'congress_api.py'
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found")
        return
    
    # Fix 'return Non' typos
    fix_typos_in_file(file_path)
    print(f"Fixed 'return Non' typos in {file_path}")
    
    # Fix string .get() bug
    fix_string_get_bug(file_path)
    print(f"Fixed string .get() bug in {file_path}")
    
    print("Bug fixes completed!")

if __name__ == "__main__":
    main()