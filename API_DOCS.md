# Congress Data API Documentation

## Introduction

The Congress Data API provides programmatic access to Congress.gov data stored in DynamoDB. This API allows you to query information about bills, committees, hearings, amendments, nominations, and treaties with flexible filtering options.

## Base URL

```
https://your-replit-url.repl.co
```

## Authentication

Currently, the API does not require authentication. Future versions will implement API key authentication for production use.

## Interactive Documentation

Interactive API documentation is available via Swagger UI at:

```
/swagger/
```

This interface allows you to:
- Explore available endpoints
- View request/response schemas
- Test API calls directly from your browser

## Available Endpoints

| Endpoint | Description | Response Type |
|----------|-------------|---------------|
| `GET /api/bills` | Retrieve bills with filtering | List of bills |
| `GET /api/committees` | Retrieve committees with filtering | List of committees |
| `GET /api/hearings` | Retrieve hearings with filtering | List of hearings |
| `GET /api/amendments` | Retrieve amendments with filtering | List of amendments |
| `GET /api/nominations` | Retrieve nominations with filtering | List of nominations |
| `GET /api/treaties` | Retrieve treaties with filtering | List of treaties |

## Common Query Parameters

All endpoints support the following query parameters:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `congress` | Integer | Filter by congress number | `117` |
| `start_date` | String (YYYY-MM-DD) | Filter by start date | `2024-01-01` |
| `end_date` | String (YYYY-MM-DD) | Filter by end date | `2024-01-31` |
| `limit` | Integer | Maximum number of results to return (default: 20, max: 100) | `50` |
| `next_token` | String | Token for pagination | See pagination section |

## Endpoint-Specific Parameters

### Bills (`/api/bills`)
- `bill_type`: Filter by bill type (e.g., `hr`, `s`)

### Committees (`/api/committees`)
- `chamber`: Filter by chamber (`House`, `Senate`)

### Hearings (`/api/hearings`)
- `committee`: Filter by committee system code
- `chamber`: Filter by chamber (`House`, `Senate`)

### Amendments (`/api/amendments`)
- `amendment_type`: Filter by amendment type

### Nominations (`/api/nominations`)
- `organization`: Filter by organization

### Treaties (`/api/treaties`)
- `country`: Filter by country

## Response Format

All responses are returned in JSON format and follow a consistent structure:

```json
{
  "data_type": [
    {
      "id": "unique-identifier",
      "type": "data-type",
      "congress": 117,
      "update_date": "2024-01-20",
      ...other fields specific to the data type
    }
  ],
  "count": 1,
  "next_token": "pagination-token"
}
```

Where `data_type` will be the plural form of the requested data type (e.g., `bills`, `committees`).

## Pagination

For endpoints that return multiple items, results are paginated. The response includes:

- `count`: Number of items in the current response
- `next_token`: Token to retrieve the next page of results (if available)

To get the next page, include the `next_token` in your next request:

```
GET /api/bills?next_token=eyJpZCI6Imxhc3QtZXZhbHVhdGVkLWtleSJ9
```

## Examples

### Retrieve bills from the 117th Congress

```
GET /api/bills?congress=117
```

### Retrieve House committees with date filtering

```
GET /api/committees?chamber=House&start_date=2024-01-01&end_date=2024-01-31
```

### Retrieve hearings for a specific committee

```
GET /api/hearings?committee=HSJU&chamber=House
```

## Error Handling

The API returns standard HTTP status codes:

- `200 OK`: The request was successful
- `400 Bad Request`: The request was invalid (e.g., invalid parameters)
- `500 Internal Server Error`: An error occurred on the server

Error responses include:

```json
{
  "error": "Error message",
  "status": 400
}
```

## Best Practices

1. **Use Filtering**: Always use filtering parameters to limit results
2. **Handle Pagination**: Implement pagination handling for large result sets
3. **Cache Responses**: Cache API responses to reduce load
4. **Handle Errors**: Implement proper error handling for robust applications

## Data Models

### Bill
```json
{
  "id": "bill-id",
  "type": "bill",
  "congress": 117,
  "update_date": "2024-01-20",
  "bill_type": "hr",
  "bill_number": 123,
  "title": "A bill to...",
  "origin_chamber": "House",
  "latest_action": {
    "text": "Referred to Committee",
    "action_date": "2024-01-19"
  }
}
```

### Committee
```json
{
  "id": "committee-id",
  "type": "committee",
  "congress": 117,
  "update_date": "2024-01-20",
  "name": "Committee on the Judiciary",
  "chamber": "House",
  "committee_type": "standing",
  "system_code": "HSJU",
  "parent_committee": {
    "name": "Parent Committee",
    "system_code": "HSXX",
    "url": "https://api.congress.gov/v3/committee/..."
  },
  "subcommittees": [
    {
      "name": "Subcommittee on Immigration",
      "system_code": "HSJU10",
      "url": "https://api.congress.gov/v3/committee/..."
    }
  ]
}
```

### Hearing
```json
{
  "id": "hearing-id",
  "type": "hearing",
  "congress": 117,
  "update_date": "2024-01-20",
  "chamber": "House",
  "date": "2024-01-25",
  "time": "10:00AM",
  "location": "2141 RHOB",
  "title": "Oversight Hearing on...",
  "committee": {
    "name": "Committee on the Judiciary",
    "system_code": "HSJU",
    "url": "https://api.congress.gov/v3/committee/..."
  }
}
```

### Amendment
```json
{
  "id": "amendment-id",
  "type": "amendment",
  "congress": 117,
  "update_date": "2024-01-20",
  "amendment_number": 123,
  "amendment_type": "house-amendment",
  "title": "Amendment to H.R. 123",
  "description": "Description of the amendment",
  "purpose": "To provide for...",
  "latest_action": {
    "text": "Agreed to by voice vote",
    "action_date": "2024-01-19"
  }
}
```

### Nomination
```json
{
  "id": "nomination-id",
  "type": "nomination",
  "congress": 117,
  "update_date": "2024-01-20",
  "number": 123,
  "received_date": "2024-01-10",
  "description": "Nomination for position",
  "organization": "Department of State",
  "nomination_type": {
    "is_civilian": true
  },
  "latest_action": {
    "text": "Received in the Senate",
    "action_date": "2024-01-10"
  }
}
```

### Treaty
```json
{
  "id": "treaty-id",
  "type": "treaty",
  "congress": 117,
  "update_date": "2024-01-20",
  "treaty_number": "117-1",
  "description": "Treaty between the United States and...",
  "country": "Canada",
  "subject": "Trade",
  "received_date": "2024-01-05",
  "latest_action": {
    "text": "Treaty signed",
    "action_date": "2024-01-05"
  }
}
```

## Rate Limiting

The API currently does not implement rate limiting. In production environments, rate limits would be enforced to ensure fair usage.

## Contact

For questions or support, please contact:
- Email: support@example.com
