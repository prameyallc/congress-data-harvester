from datetime import datetime

def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected format: YYYY-MM-DD")

def validate_date_range(start_date, end_date):
    """Validate that start_date is before end_date and not in future"""
    now = datetime.now()
    
    if start_date > end_date:
        return False
    
    if end_date > now:
        return False
    
    return True

def format_date_for_api(date):
    """Format date object for API requests"""
    return date.strftime('%Y-%m-%d')

def calculate_timestamp(date):
    """Convert date to Unix timestamp"""
    return int(date.timestamp())

def chunk_list(lst, chunk_size):
    """Split list into chunks of specified size"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
