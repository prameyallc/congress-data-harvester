# API Documentation

This document provides detailed information about the Congress Data API endpoints, request parameters, and responses.

## Base URL

```
http://localhost:5000
```

## Authentication

Currently, the API does not require authentication for local development. For production deployments, implement appropriate authentication mechanisms.

## API Endpoints

### Home Endpoint

Provides general information about the API.

```
GET /
```

**Response:**

```json
{
  "name": "Congress Data API",
  "version": "1.0.0",
  "description": "API for accessing Congress.gov data",
  "documentation": "/swagger/"
}
```

### Bills

Retrieve bills with optional filtering by congress, bill_type, and date range.

```
GET /api/bills
```

**Query Parameters:**

| Parameter   | Type    | Description                                        | Example      |
|-------------|---------|----------------------------------------------------|--------------|
| congress    | integer | Filter by congress number                           | 117          |
| bill_type   | string  | Filter by bill type (e.g., hr, s)                  | hr           |
| start_date  | string  | Filter by update date (start date, YYYY-MM-DD)     | 2024-01-01   |
| end_date    | string  | Filter by update date (end date, YYYY-MM-DD)       | 2024-01-31   |
| limit       | integer | Maximum number of results to return (default: 20)  | 50           |

**Response:**

```json
{
  "bills": [
    {
      "bill_id": "117-hr1234",
      "congress": 117,
      "bill_type": "hr",
      "bill_number": "1234",
      "title": "Example Bill Title",
      "introduced_date": "2023-05-15",
      "sponsor": {
        "bioguide_id": "A000001",
        "name": "Representative Name",
        "state": "NY",
        "party": "D"
      },
      "cosponsors": [
        {
          "bioguide_id": "B000002",
          "name": "Representative Name 2",
          "state": "CA",
          "party": "R"
        }
      ],
      "committees": [
        {
          "committee_id": "HSJU",
          "name": "House Judiciary",
          "chamber": "House"
        }
      ],
      "latest_action": {
        "action_date": "2023-06-01",
        "text": "Referred to the Subcommittee on..."
      },
      "update_date": "2023-06-02"
    }
  ],
  "count": 1,
  "next_token": null
}
```

### Committees

Retrieve committees with optional filtering by congress, chamber, and date range.

```
GET /api/committees
```

**Query Parameters:**

| Parameter   | Type    | Description                                        | Example      |
|-------------|---------|----------------------------------------------------|--------------|
| congress    | integer | Filter by congress number                           | 117          |
| chamber     | string  | Filter by chamber (House, Senate)                  | House        |
| start_date  | string  | Filter by update date (start date, YYYY-MM-DD)     | 2024-01-01   |
| end_date    | string  | Filter by update date (end date, YYYY-MM-DD)       | 2024-01-31   |
| limit       | integer | Maximum number of results to return (default: 20)  | 50           |

**Response:**

```json
{
  "committees": [
    {
      "committee_id": "HSJU",
      "name": "House Committee on the Judiciary",
      "chamber": "House",
      "committee_type": "Standing",
      "subcommittees": [
        {
          "committee_id": "HSJU10",
          "name": "Subcommittee on Crime, Terrorism, and Homeland Security",
          "parent_committee_id": "HSJU"
        }
      ],
      "url": "https://judiciary.house.gov/",
      "congress": 117,
      "update_date": "2023-01-03"
    }
  ],
  "count": 1,
  "next_token": null
}
```

### Hearings

Retrieve hearings with optional filtering by congress, committee, and date range.

```
GET /api/hearings
```

**Query Parameters:**

| Parameter   | Type    | Description                                        | Example      |
|-------------|---------|----------------------------------------------------|--------------|
| congress    | integer | Filter by congress number                           | 117          |
| committee   | string  | Filter by committee system code                    | HSJU         |
| chamber     | string  | Filter by chamber (House, Senate)                  | House        |
| start_date  | string  | Filter by hearing date (start date, YYYY-MM-DD)    | 2024-01-01   |
| end_date    | string  | Filter by hearing date (end date, YYYY-MM-DD)      | 2024-01-31   |
| limit       | integer | Maximum number of results to return (default: 20)  | 50           |

**Response:**

```json
{
  "hearings": [
    {
      "hearing_id": "117-house-hsju-20230410",
      "congress": 117,
      "chamber": "House",
      "committee": {
        "committee_id": "HSJU",
        "name": "House Committee on the Judiciary"
      },
      "subcommittee": {
        "committee_id": "HSJU10",
        "name": "Subcommittee on Crime, Terrorism, and Homeland Security"
      },
      "hearing_title": "Example Hearing Title",
      "hearing_date": "2023-04-10",
      "hearing_time": "10:00:00",
      "location": "2141 Rayburn House Office Building",
      "witnesses": [
        {
          "name": "Dr. John Smith",
          "organization": "University Research Institute"
        }
      ],
      "url": "https://judiciary.house.gov/calendar/eventsingle.aspx?EventID=12345",
      "update_date": "2023-04-05"
    }
  ],
  "count": 1,
  "next_token": null
}
```

### Amendments

Retrieve amendments with optional filtering by congress, amendment type, and date range.

```
GET /api/amendments
```

**Query Parameters:**

| Parameter      | Type    | Description                                        | Example      |
|----------------|---------|----------------------------------------------------|--------------|
| congress       | integer | Filter by congress number                           | 117          |
| amendment_type | string  | Filter by amendment type                           | hamdt        |
| start_date     | string  | Filter by update date (start date, YYYY-MM-DD)     | 2024-01-01   |
| end_date       | string  | Filter by update date (end date, YYYY-MM-DD)       | 2024-01-31   |
| limit          | integer | Maximum number of results to return (default: 20)  | 50           |

**Response:**

```json
{
  "amendments": [
    {
      "amendment_id": "117-hamdt123",
      "congress": 117,
      "amendment_type": "hamdt",
      "amendment_number": "123",
      "bill": {
        "bill_id": "117-hr1234",
        "bill_type": "hr",
        "bill_number": "1234"
      },
      "sponsor": {
        "bioguide_id": "A000001",
        "name": "Representative Name",
        "state": "NY",
        "party": "D"
      },
      "title": "Amendment to increase funding for...",
      "purpose": "To amend section 2 to include...",
      "latest_action": {
        "action_date": "2023-06-15",
        "text": "Amendment agreed to in House by voice vote."
      },
      "update_date": "2023-06-16"
    }
  ],
  "count": 1,
  "next_token": null
}
```

### Nominations

Retrieve nominations with optional filtering by congress, organization, and date range.

```
GET /api/nominations
```

**Query Parameters:**

| Parameter    | Type    | Description                                        | Example      |
|--------------|---------|----------------------------------------------------|--------------|
| congress     | integer | Filter by congress number                           | 117          |
| organization | string  | Filter by organization                             | EPA          |
| start_date   | string  | Filter by update date (start date, YYYY-MM-DD)     | 2024-01-01   |
| end_date     | string  | Filter by update date (end date, YYYY-MM-DD)       | 2024-01-31   |
| limit        | integer | Maximum number of results to return (default: 20)  | 50           |

**Response:**

```json
{
  "nominations": [
    {
      "nomination_id": "117-pn123",
      "congress": 117,
      "nomination_number": "123",
      "nominee": "Jane Doe",
      "position": "Administrator",
      "organization": "Environmental Protection Agency",
      "received_date": "2023-01-20",
      "referred_to": "Committee on Environment and Public Works",
      "actions": [
        {
          "date": "2023-01-20",
          "text": "Received in the Senate and referred to the Committee on Environment and Public Works."
        },
        {
          "date": "2023-03-15",
          "text": "Committee on Environment and Public Works. Ordered to be reported favorably."
        }
      ],
      "status": "Confirmed",
      "update_date": "2023-03-20"
    }
  ],
  "count": 1,
  "next_token": null
}
```

### Treaties

Retrieve treaties with optional filtering by congress, country, and date range.

```
GET /api/treaties
```

**Query Parameters:**

| Parameter   | Type    | Description                                        | Example      |
|-------------|---------|----------------------------------------------------|--------------|
| congress    | integer | Filter by congress number                           | 117          |
| country     | string  | Filter by country                                  | Japan        |
| start_date  | string  | Filter by update date (start date, YYYY-MM-DD)     | 2024-01-01   |
| end_date    | string  | Filter by update date (end date, YYYY-MM-DD)       | 2024-01-31   |
| limit       | integer | Maximum number of results to return (default: 20)  | 50           |

**Response:**

```json
{
  "treaties": [
    {
      "treaty_id": "117-treaty123",
      "congress": 117,
      "treaty_number": "123",
      "title": "Treaty between the United States of America and Japan on...",
      "country": "Japan",
      "date_submitted": "2023-02-15",
      "referred_to": "Committee on Foreign Relations",
      "actions": [
        {
          "date": "2023-02-15",
          "text": "Received in the Senate and referred to the Committee on Foreign Relations."
        },
        {
          "date": "2023-04-20",
          "text": "Committee on Foreign Relations. Ordered to be reported favorably."
        }
      ],
      "status": "Resolution of Ratification Agreed to in Senate",
      "update_date": "2023-05-01"
    }
  ],
  "count": 1,
  "next_token": null
}
```

## Error Responses

All endpoints return standardized error responses:

### Bad Request (400)

```json
{
  "error": {
    "code": 400,
    "message": "Invalid query parameters: 'congress' must be an integer between 93 and 118."
  }
}
```

### Not Found (404)

```json
{
  "error": {
    "code": 404,
    "message": "Resource not found."
  }
}
```

### Server Error (500)

```json
{
  "error": {
    "code": 500,
    "message": "An internal server error occurred. Please try again later."
  }
}
```

## Pagination

All list endpoints support pagination through the `limit` parameter and the `next_token` response field.

To get the next page of results, pass the `next_token` value from the previous response as a query parameter:

```
GET /api/bills?limit=20&next_token=eyJsYXN0X2tleV9zZWVuIjogeyJiaWxsX2lkIjogIjExNy1ocjEyMzQifX0=
```

## Rate Limiting

The API implements rate limiting to ensure fair usage. Clients should respect the following headers in responses:

- `X-RateLimit-Limit`: Maximum number of requests per hour
- `X-RateLimit-Remaining`: Number of requests remaining in the current window
- `X-RateLimit-Reset`: Time in seconds until the rate limit resets

When rate limited, the API will respond with a 429 status code:

```json
{
  "error": {
    "code": 429,
    "message": "Too many requests. Please try again after 30 seconds."
  }
}
```