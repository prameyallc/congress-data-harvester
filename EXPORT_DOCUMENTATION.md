# Congress Data Export Documentation

This document provides comprehensive information about the export functionality in the Congress Data Downloader project. The export feature allows users to extract congressional data from DynamoDB into standard formats (JSON and CSV) for external analysis and reporting.

## Overview

The export functionality enables users to:
- Extract data from DynamoDB based on various filters
- Export in either JSON or CSV format
- Use command-line interface or RESTful API access methods
- Apply filtering by data type, congress number, and date ranges
- Generate files with automatic timestamping

## Usage Methods

### 1. Command Line Interface

The export functionality is integrated directly into the main Congress Downloader CLI application.

```bash
python congress_downloader.py --mode export [options]
```

#### Required Parameters:
- `--mode export`: Specifies export operation

#### Optional Parameters:
- `--data-type`: Type of data to export (e.g., bill, committee, hearing, amendment, nomination, treaty)
- `--congress`: Congress number (e.g., 117)
- `--start-date`: Start date for filtering in YYYY-MM-DD format
- `--end-date`: End date for filtering in YYYY-MM-DD format
- `--format`: Export format (json or csv, default: json)
- `--output`: Custom output file path (default: exports/[data_type]_[timestamp].[format])
- `--verbose`: Enable verbose logging

#### Examples:

Export all bills from the 117th Congress to JSON:
```bash
python congress_downloader.py --mode export --data-type bill --congress 117 --format json
```

Export committee data for a specific date range to CSV:
```bash
python congress_downloader.py --mode export --data-type committee --start-date 2023-01-01 --end-date 2023-06-30 --format csv
```

### 2. REST API Endpoint

The export functionality is also accessible through the API server as a RESTful endpoint.

```
GET /api/export
```

#### Query Parameters:
- `format`: Export format (json or csv, default: json)
- `data_type`: Type of data to export (bill, committee, hearing, amendment, nomination, treaty)
- `congress`: Filter by congress number (e.g., 117)
- `start_date`: Filter by update date (start date, format YYYY-MM-DD)
- `end_date`: Filter by update date (end date, format YYYY-MM-DD)

#### Examples:

Export all bills from the 117th Congress to JSON:
```
GET /api/export?format=json&data_type=bill&congress=117
```

Export committee data for a specific date range to CSV:
```
GET /api/export?format=csv&data_type=committee&start_date=2023-01-01&end_date=2023-06-30
```

## Predefined Workflows

The system includes predefined workflows for common export operations:

1. **Export Bills (JSON)**:
   - Command: `python congress_downloader.py --mode export --data-type bill --format json --verbose`
   - Purpose: Quickly export all bills in JSON format

2. **Export Committees (CSV)**:
   - Command: `python congress_downloader.py --mode export --data-type committee --format csv --verbose`
   - Purpose: Quickly export all committee data in CSV format

These workflows can be started from the Replit interface or via script.

## Output Formats

### JSON Export

JSON exports preserve the full data structure including nested objects and arrays. The output follows this format:

```json
[
  {
    "id": "unique-identifier",
    "type": "data-type",
    "congress": 117,
    "update_date": "2023-01-15",
    "title": "Example Title",
    "number": "HR1234",
    "chamber": "House",
    ...additional fields...
  },
  ...more records...
]
```

### CSV Export

CSV exports flatten nested structures and prioritize key fields for better readability:

1. Priority fields are ordered first: 'id', 'type', 'congress', 'update_date', 'title', 'number', 'chamber'
2. Remaining fields are ordered alphabetically
3. Complex nested objects (lists/dictionaries) are JSON-serialized into string columns

## Implementation Details

### Directory Structure

The export functionality generates files in the `/exports` directory (which is git-ignored except for the `.gitkeep` file).

Files are named using the pattern:
```
exports/[data_type]_[timestamp].[format]
```

For example:
- `exports/bill_20250227010348.json`
- `exports/committee_20250227010358.csv`

### Data Source

All exported data comes from the DynamoDB table configured in `config.json`. The export function supports different query methods:

1. By congress and type (most efficient):
   ```python
   db_handler.query_by_congress_and_type(congress, data_type)
   ```

2. By type and date range:
   ```python
   db_handler.query_by_type_and_date_range(data_type, start_date, end_date)
   ```

3. By type only:
   ```python
   db_handler.scan_by_type(data_type)
   ```

4. Full scan (limited to 1000 items for safety):
   ```python
   db_handler.table.scan(Limit=1000)
   ```

### Error Handling

The export functionality includes comprehensive error handling:

- Invalid date formats are caught and reported
- Non-existent data types are validated
- Empty result sets generate appropriate warnings
- File system errors (permissions, space issues) are trapped
- JSON serialization errors for complex objects are handled

## Security and Performance Considerations

### Security
- Only authorized users with DynamoDB read access can export data
- The API endpoint is properly secured through the API's authentication mechanisms
- Exported files do not contain AWS credentials or sensitive configuration data

### Performance
- Full table scans are limited to 1000 items by default
- Targeted queries are used wherever possible
- CSV exports carefully handle complex nested objects
- Large exports are processed efficiently with proper memory management

## Troubleshooting

Common issues and their solutions:

1. **"No data found matching the criteria"**
   - Verify the data exists in DynamoDB using the test workflows
   - Check filters (congress, date range) are not too restrictive

2. **Export times out for large datasets**
   - Use more specific filters
   - Increase timeout settings in API configuration
   - Export smaller batches with date range filters

3. **CSV contains serialized JSON instead of flat data**
   - This is by design for complex nested objects
   - For purely flat data, post-process the CSV or create a custom exporter

## Future Enhancements

Planned improvements to the export functionality:

1. Additional output formats (Excel, XML)
2. Compression options for large exports (ZIP, GZIP)
3. Streaming API for very large datasets
4. Custom field selection to reduce output size
5. Email delivery of scheduled exports