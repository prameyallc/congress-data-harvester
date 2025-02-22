# Comprehensive DynamoDB Schema for Congress.gov Data

## Common Attributes (Present in All Records)
- `id` (String, Primary Key) - Unique identifier for each record
- `type` (String, GSI Hash) - Record type (e.g., 'bill', 'committee', 'hearing')
- `update_date` (String, GSI Range) - Last update date in YYYY-MM-DD format
- `congress` (Number) - Congress number
- `version` (Number) - Schema version number
- `url` (String) - API URL for the record

## Type-Specific Attributes

### Bills
- `bill_type` (String) - Type of bill (hr, s, etc.)
- `bill_number` (Number) - Bill number
- `title` (String) - Bill title
- `origin_chamber` (String) - Originating chamber
- `origin_chamber_code` (String) - Chamber code (H/S)
- `latest_action` (Map)
  - `text` (String) - Action text
  - `action_date` (String) - Date of action
- `sponsors` (List) - List of bill sponsors
- `committees` (List) - List of committees

### Amendments
- `amendment_number` (Number) - Amendment number
- `amendment_type` (String) - Type of amendment
- `associated_bill` (Map)
  - `congress` (Number)
  - `type` (String)
  - `number` (Number)
- `purpose` (String) - Amendment purpose
- `submit_date` (String) - Submission date

### Committees
- `name` (String) - Committee name
- `chamber` (String) - Chamber (House/Senate)
- `committee_type` (String) - Type of committee
- `system_code` (String) - Committee system code
- `parent_committee` (Map)
  - `name` (String)
  - `system_code` (String)
  - `url` (String)
- `subcommittees` (List<Map>)
  - `name` (String)
  - `system_code` (String)
  - `url` (String)

### Hearings
- `chamber` (String) - Chamber (House/Senate)
- `committee` (Map)
  - `name` (String)
  - `system_code` (String)
  - `url` (String)
- `date` (String) - Hearing date
- `time` (String) - Hearing time
- `location` (String) - Hearing location
- `title` (String) - Hearing title

### Nominations
- `citation` (String) - Nomination citation (e.g., 'PN123')
- `number` (Number) - Nomination number (extracted from citation)
- `description` (String) - Nomination description
- `organization` (String) - Organization
- `nomination_type` (Map)
  - `is_civilian` (Boolean)
- `latest_action` (Map)
  - `text` (String)
  - `action_date` (String)

### Treaties
- `treaty_number` (String) - Treaty number
- `description` (String) - Treaty description
- `title` (String) - Treaty title
- `country` (String) - Country involved
- `subject` (String) - Treaty subject
- `status` (String) - Treaty status
- `received_date` (String) - Date received

### Congressional Records (Bound and Daily)
- `date` (String) - Record date
- `session_number` (Number) - Session number
- `volume_number` (Number) - Volume number
- `issue_number` (Number, Daily only) - Issue number
- `chamber` (String) - Chamber (House/Senate/Joint)
- `sections` (Map) - Record sections

### Committee Reports
- `report_number` (String) - Report number
- `associated_bill` (Map)
  - `congress` (Number)
  - `type` (String)
  - `number` (Number)
- `report_type` (String) - Type of report
- `chamber` (String) - Chamber
- `committee` (Map)
  - `name` (String)
  - `system_code` (String)

### Committee Meetings
- `committee` (Map)
  - `name` (String)
  - `system_code` (String)
- `date` (String) - Meeting date
- `meeting_type` (String) - Type of meeting
- `location` (String) - Meeting location
- `title` (String) - Meeting title
- `documents` (List) - Meeting documents

### Members
- `bioguide_id` (String) - Bioguide ID
- `first_name` (String) - First name
- `last_name` (String) - Last name
- `state` (String) - State
- `party` (String) - Political party
- `chamber` (String) - Chamber
- `start_date` (String) - Term start date
- `end_date` (String) - Term end date

### Summaries
- `text` (String) - Summary text
- `associated_bill` (Map)
  - `congress` (Number)
  - `type` (String)
  - `number` (Number)
- `version_code` (String) - Summary version
- `action_date` (String) - Action date


## DynamoDB Table Configuration

### Primary Key
- Hash Key: `id` (String)

### Global Secondary Indexes
1. Type-Update Index
   - Hash Key: `type` (String)
   - Range Key: `update_date` (String)
   - Used for: Querying records by type within a date range

2. Congress-Type Index
   - Hash Key: `congress` (Number)
   - Range Key: `type` (String)
   - Used for: Querying records by congress and type

3. Chamber-Date Index
   - Hash Key: `chamber` (String)
   - Range Key: `date` (String)
   - Used for: Querying records by chamber and date range

4. Version-Update Index
   - Hash Key: `version` (Number)
   - Range Key: `update_date` (String)
   - Used for: Querying records by version and update date

## Data Validation Rules
1. Required Fields (All Records)
   - id
   - type
   - update_date
   - congress
   - version

2. Type-Specific Required Fields
   - Vary by record type (see individual sections)
   - Always include fields needed for GSI

3. Date Format
   - All dates must be in YYYY-MM-DD format
   - Must be valid calendar dates

4. Numeric Fields
   - Must be positive integers where applicable
   - Congress number must be between 1 and current congress

5. String Fields
   - No empty strings stored
   - Trimmed of whitespace
   - URLs must be valid Congress.gov URLs

6. Chamber Values
   - Must be one of: 'house', 'senate', 'joint'
   - Stored in lowercase

## TTL Configuration
- `expiry_time` (Number, Optional) - Unix timestamp for record expiration
- `timestamp` (Number) - Record creation/update timestamp

## Stream Configuration
- Stream: Enabled
- View Type: NEW_AND_OLD_IMAGES

## Notes
1. All dates use YYYY-MM-DD format
2. All IDs are strings
3. Nested objects are stored as DynamoDB maps
4. Lists are stored as DynamoDB lists
5. Numbers are stored as DynamoDB number type
6. Empty strings are omitted (not stored)
7. Null values are omitted (not stored)