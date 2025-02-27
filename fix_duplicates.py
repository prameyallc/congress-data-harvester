#!/usr/bin/env python3
import os
import re
import json

def find_duplicates(file_path):
    """Find duplicate method declarations in a Python file"""
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Find all method declarations
    method_pattern = r'def\s+(_[a-zA-Z0-9_]+)\s*\('
    matches = re.finditer(method_pattern, content)
    
    # Count occurrences of each method name
    method_counts = {}
    method_positions = {}
    
    for match in matches:
        method_name = match.group(1)
        if method_name not in method_counts:
            method_counts[method_name] = 0
            method_positions[method_name] = []
            
        method_counts[method_name] += 1
        method_positions[method_name].append(match.start())
    
    # Find duplicates
    duplicates = {name: positions for name, positions in method_positions.items() 
                 if method_counts[name] > 1}
    
    return duplicates

def remove_duplicate_methods(file_path, duplicates):
    """Remove duplicate method declarations from a Python file, 
    keeping only the first occurrence of each method"""
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Build a map of line numbers to method starts
    method_start_lines = {}
    for i, line in enumerate(lines):
        if re.match(r'\s*def\s+_[a-zA-Z0-9_]+\s*\(', line):
            method_name = re.search(r'def\s+(_[a-zA-Z0-9_]+)\s*\(', line).group(1)
            if method_name in duplicates:
                method_start_lines[i] = method_name
    
    # Find ranges of methods to remove (keeping only the first occurrence)
    keep_methods = set()
    remove_ranges = []
    
    # For each method name, determine which occurrences to keep vs remove
    for method_name, positions in duplicates.items():
        first_pos = None
        for i, line_num in enumerate(sorted(
            [ln for ln, name in method_start_lines.items() if name == method_name]
        )):
            if i == 0:  # Keep the first occurrence
                first_pos = line_num
                keep_methods.add(line_num)
            else:  # Mark subsequent occurrences for removal
                start = line_num
                end = find_method_end(lines, start)
                remove_ranges.append((start, end))
    
    # Sort ranges by start line in reverse order
    remove_ranges.sort(reverse=True)
    
    # Remove duplicate methods (working backwards to maintain line numbers)
    for start, end in remove_ranges:
        del lines[start:end+1]
    
    # Write the cleaned file
    with open(file_path, 'w') as file:
        file.writelines(lines)
    
    return len(remove_ranges)

def find_method_end(lines, start):
    """Find the end line of a method definition (looking for the next def or class)"""
    indent_level = len(lines[start]) - len(lines[start].lstrip())
    
    # Find the end of the method
    for i in range(start + 1, len(lines)):
        # If we find a line with same or less indentation that is a new def or class,
        # or we reach the end of the file, we've found the end
        line = lines[i] if i < len(lines) else ""
        if line.strip() and len(line) - len(line.lstrip()) <= indent_level:
            if line.lstrip().startswith(('def ', 'class ')):
                return i - 1
    
    return len(lines) - 1

def add_deduplication_notice(file_path):
    """Add a notice about method deduplication at the beginning of the CongressAPI class"""
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Find the CongressAPI class declaration
    for i, line in enumerate(lines):
        if "class CongressAPI" in line:
            notice = [
                '    """\n',
                '    NOTE: This class was automatically deduplicated.\n',
                '    Duplicate method declarations were removed, keeping only the first occurrence of each method.\n',
                '    Please ensure you modify only the canonical version of each method.\n',
                '    """\n'
            ]
            # Insert after the class docstring if present, otherwise after the class declaration
            for j in range(i+1, len(lines)):
                if '"""' in lines[j]:
                    for k in range(j+1, len(lines)):
                        if '"""' in lines[k]:
                            lines.insert(k+1, ''.join(notice))
                            break
                    break
            else:
                # No docstring found, insert after class line
                lines.insert(i+1, ''.join(notice))
            break
    
    with open(file_path, 'w') as file:
        file.writelines(lines)

def fix_urllib3_import(file_path):
    """Fix the urllib3.util import issue"""
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Find the import line
    for i, line in enumerate(lines):
        if "from urllib3.util import Retry" in line:
            # We need to install the package first, then replace with a safer import
            lines[i] = "try:\n    from urllib3.util.retry import Retry\nexcept ImportError:\n    print('Missing urllib3 module. Please install with \"pip install urllib3\"')\n    Retry = None\n"
            break
    
    with open(file_path, 'w') as file:
        file.writelines(lines)

def main():
    print("Starting code deduplication...")
    file_path = 'congress_api.py'
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found")
        return
    
    # Find duplicate methods
    duplicates = find_duplicates(file_path)
    print(f"Found {len(duplicates)} methods with duplicates")
    
    # Print details of duplicates
    for method, positions in duplicates.items():
        print(f"Method {method} has {len(positions)} occurrences")
    
    # Fix the urllib3 import issue
    fix_urllib3_import(file_path)
    print(f"Fixed urllib3 import in {file_path}")
    
    # Remove duplicate methods
    removed_count = remove_duplicate_methods(file_path, duplicates)
    print(f"Removed {removed_count} duplicate method declarations")
    
    # Add notice about deduplication
    add_deduplication_notice(file_path)
    print(f"Added deduplication notice to {file_path}")
    
    print("Code deduplication completed!")

if __name__ == "__main__":
    main()