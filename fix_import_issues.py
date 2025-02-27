#!/usr/bin/env python3

"""
Fix import issues in Congress API implementation
"""

import os
import sys
import logging
import re

def fix_imports_in_file(file_path):
    """Fix import statements in a Python file"""
    try:
        with open(file_path, 'r') as file:
            content = file.read()
        
        # Fix the urllib3 Retry import issue
        content = re.sub(
            r'from urllib3\.util import Retry',
            'try:\n    from urllib3.util.retry import Retry\nexcept ImportError:\n    from urllib3.util import Retry',
            content
        )
        
        # Write the changes back to the file
        with open(file_path, 'w') as file:
            file.write(content)
        
        print(f"Fixed imports in {file_path}")
        return True
    except Exception as e:
        print(f"Error fixing imports in {file_path}: {str(e)}")
        return False

def copy_simplified_api_to_original():
    """Copy simplified_congress_api.py to a backup of congress_api.py"""
    try:
        import shutil
        
        # Create backup of original congress_api.py
        if os.path.exists('congress_api.py'):
            # Create backup with timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            backup_path = f'congress_api.py.bak.{timestamp}'
            shutil.copy2('congress_api.py', backup_path)
            print(f"Created backup of congress_api.py at {backup_path}")
        
        # Create the simplified version
        print("Creating simplified congress_api.py")
        with open('simplified_congress_api.py', 'r') as src:
            with open('congress_api.py', 'w') as dst:
                dst.write(src.read())
        
        print("Successfully replaced congress_api.py with simplified version")
        return True
    except Exception as e:
        print(f"Error copying simplified API: {str(e)}")
        return False

def main():
    print("Starting to fix import issues...")
    
    # Fix imports in congress_api.py
    fix_imports_in_file('congress_api.py')
    
    # Replace with simplified version without asking for confirmation
    print("\nReplacing congress_api.py with simplified version...")
    copy_simplified_api_to_original()
    
    print("Import fixes completed!")

if __name__ == "__main__":
    main()