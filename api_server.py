#!/usr/bin/env python3
"""
Congress Data API Server

This module implements a Flask API server that provides access to Congress.gov data
stored in DynamoDB, with comprehensive Swagger/OpenAPI documentation.
"""
import os
import json
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_from_directory
from flask_swagger_ui import get_swaggerui_blueprint
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin
import boto3
from botocore.exceptions import ClientError
import logging
from logger_config import setup_logger

# Set up logging
log_config = {
    'level': logging.INFO,
    'file': 'logs/api_server.log',
    'max_size': 10485760,
    'backup_count': 5
}
logger = setup_logger(log_config)

# Initialize Flask app
app = Flask(__name__)

# Initialize DynamoDB client
try:
    dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-west-2'))
    table_name = os.environ.get('DYNAMODB_TABLE', 'prameya-development-dynamodb-table')
    table = dynamodb.Table(table_name)
    logger.info(f"Connected to DynamoDB table: {table_name}")
except Exception as e:
    logger.error(f"Failed to initialize DynamoDB client: {str(e)}")
    dynamodb = None
    table = None

# Create APISpec
spec = APISpec(
    title="Congress Data API",
    version="1.0.0",
    openapi_version="3.0.2",
    info=dict(
        description="API for accessing Congress.gov data",
        contact=dict(email="support@example.com")
    ),
    plugins=[FlaskPlugin(), MarshmallowPlugin()],
)

# Define schemas for Swagger documentation
spec.components.schema("Error", {
    "type": "object",
    "properties": {
        "error": {"type": "string", "description": "Error message"},
        "status": {"type": "integer", "description": "HTTP status code"}
    }
})

spec.components.schema("Bill", {
    "type": "object",
    "properties": {
        "id": {"type": "string", "description": "Unique identifier for the bill"},
        "type": {"type": "string", "description": "Type of data (always 'bill')"},
        "congress": {"type": "integer", "description": "Congress number"},
        "update_date": {"type": "string", "format": "date", "description": "Last update date"},
        "bill_type": {"type": "string", "description": "Type of bill (hr, s, etc.)"},
        "bill_number": {"type": "integer", "description": "Bill number"},
        "title": {"type": "string", "description": "Bill title"},
        "origin_chamber": {"type": "string", "description": "Chamber where bill originated"},
        "latest_action": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Latest action text"},
                "action_date": {"type": "string", "format": "date", "description": "Action date"}
            }
        }
    }
})

spec.components.schema("Committee", {
    "type": "object",
    "properties": {
        "id": {"type": "string", "description": "Unique identifier for the committee"},
        "type": {"type": "string", "description": "Type of data (always 'committee')"},
        "congress": {"type": "integer", "description": "Congress number"},
        "update_date": {"type": "string", "format": "date", "description": "Last update date"},
        "name": {"type": "string", "description": "Committee name"},
        "chamber": {"type": "string", "description": "Chamber (House/Senate)"},
        "committee_type": {"type": "string", "description": "Committee type (standing, etc.)"},
        "system_code": {"type": "string", "description": "Committee system code"},
        "parent_committee": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Parent committee name"},
                "system_code": {"type": "string", "description": "Parent committee system code"},
                "url": {"type": "string", "description": "URL to parent committee data"}
            }
        },
        "subcommittees": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Subcommittee name"},
                    "system_code": {"type": "string", "description": "Subcommittee system code"},
                    "url": {"type": "string", "description": "URL to subcommittee data"}
                }
            }
        }
    }
})

spec.components.schema("Hearing", {
    "type": "object",
    "properties": {
        "id": {"type": "string", "description": "Unique identifier for the hearing"},
        "type": {"type": "string", "description": "Type of data (always 'hearing')"},
        "congress": {"type": "integer", "description": "Congress number"},
        "update_date": {"type": "string", "format": "date", "description": "Last update date"},
        "chamber": {"type": "string", "description": "Chamber (House/Senate)"},
        "date": {"type": "string", "format": "date", "description": "Hearing date"},
        "time": {"type": "string", "description": "Hearing time"},
        "location": {"type": "string", "description": "Hearing location"},
        "title": {"type": "string", "description": "Hearing title"},
        "committee": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Committee name"},
                "system_code": {"type": "string", "description": "Committee system code"},
                "url": {"type": "string", "description": "URL to committee data"}
            }
        }
    }
})

spec.components.schema("Amendment", {
    "type": "object",
    "properties": {
        "id": {"type": "string", "description": "Unique identifier for the amendment"},
        "type": {"type": "string", "description": "Type of data (always 'amendment')"},
        "congress": {"type": "integer", "description": "Congress number"},
        "update_date": {"type": "string", "format": "date", "description": "Last update date"},
        "amendment_number": {"type": "integer", "description": "Amendment number"},
        "amendment_type": {"type": "string", "description": "Type of amendment"},
        "title": {"type": "string", "description": "Amendment title"},
        "description": {"type": "string", "description": "Amendment description"},
        "purpose": {"type": "string", "description": "Amendment purpose"},
        "latest_action": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Latest action text"},
                "action_date": {"type": "string", "format": "date", "description": "Action date"}
            }
        }
    }
})

spec.components.schema("Nomination", {
    "type": "object",
    "properties": {
        "id": {"type": "string", "description": "Unique identifier for the nomination"},
        "type": {"type": "string", "description": "Type of data (always 'nomination')"},
        "congress": {"type": "integer", "description": "Congress number"},
        "update_date": {"type": "string", "format": "date", "description": "Last update date"},
        "number": {"type": "integer", "description": "Nomination number"},
        "received_date": {"type": "string", "format": "date", "description": "Date nomination was received"},
        "description": {"type": "string", "description": "Nomination description"},
        "organization": {"type": "string", "description": "Organization"},
        "nomination_type": {
            "type": "object",
            "properties": {
                "is_civilian": {"type": "boolean", "description": "Whether the nomination is civilian"}
            }
        },
        "latest_action": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Latest action text"},
                "action_date": {"type": "string", "format": "date", "description": "Action date"}
            }
        }
    }
})

spec.components.schema("Treaty", {
    "type": "object",
    "properties": {
        "id": {"type": "string", "description": "Unique identifier for the treaty"},
        "type": {"type": "string", "description": "Type of data (always 'treaty')"},
        "congress": {"type": "integer", "description": "Congress number"},
        "update_date": {"type": "string", "format": "date", "description": "Last update date"},
        "treaty_number": {"type": "string", "description": "Treaty number"},
        "description": {"type": "string", "description": "Treaty description"},
        "country": {"type": "string", "description": "Country"},
        "subject": {"type": "string", "description": "Subject"},
        "received_date": {"type": "string", "format": "date", "description": "Date received"},
        "latest_action": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Latest action text"},
                "action_date": {"type": "string", "format": "date", "description": "Action date"}
            }
        }
    }
})

# Flask routes
@app.route("/")
def home():
    """Home endpoint that provides API information."""
    return jsonify({
        "api": "Congress Data API",
        "version": "1.0.0",
        "documentation": "/swagger/",
        "endpoints": {
            "bills": "/api/bills",
            "committees": "/api/committees",
            "hearings": "/api/hearings",
            "amendments": "/api/amendments",
            "nominations": "/api/nominations",
            "treaties": "/api/treaties",
        },
        "status": "operational"
    })


@app.route("/api/bills")
def get_bills():
    """
    Get bills with optional filtering.
    ---
    get:
      summary: Get bills
      description: Retrieve bills with optional filtering by congress, bill_type, and date range
      parameters:
        - in: query
          name: congress
          schema:
            type: integer
          description: Filter by congress number (e.g., 117)
        - in: query
          name: bill_type
          schema:
            type: string
          description: Filter by bill type (e.g., hr, s)
        - in: query
          name: start_date
          schema:
            type: string
            format: date
          description: Filter by update date (start date, format YYYY-MM-DD)
        - in: query
          name: end_date
          schema:
            type: string
            format: date
          description: Filter by update date (end date, format YYYY-MM-DD)
        - in: query
          name: limit
          schema:
            type: integer
            default: 20
          description: Maximum number of results to return
      responses:
        200:
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  bills:
                    type: array
                    items:
                      $ref: '#/components/schemas/Bill'
                  count:
                    type: integer
                  next_token:
                    type: string
        400:
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        500:
          description: Server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
    """
    try:
        if not table:
            return jsonify({"error": "DynamoDB not configured", "status": 500}), 500

        # Parse query parameters
        congress = request.args.get('congress')
        bill_type = request.args.get('bill_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        limit = min(int(request.args.get('limit', 20)), 100)  # Cap at 100
        next_token = request.args.get('next_token')

        # Build query parameters
        query_params = {
            'FilterExpression': 'attribute_exists(id) AND #type = :type_val',
            'ExpressionAttributeNames': {
                '#type': 'type'
            },
            'ExpressionAttributeValues': {
                ':type_val': 'bill'
            },
            'Limit': limit
        }

        # Apply congress filter if provided
        if congress:
            query_params['FilterExpression'] += ' AND congress = :congress_val'
            query_params['ExpressionAttributeValues'][':congress_val'] = int(congress)

        # Apply bill_type filter if provided
        if bill_type:
            query_params['FilterExpression'] += ' AND bill_type = :bill_type_val'
            query_params['ExpressionAttributeValues'][':bill_type_val'] = bill_type

        # Apply date range filter if provided
        if start_date:
            query_params['FilterExpression'] += ' AND update_date BETWEEN :start_date AND :end_date'
            query_params['ExpressionAttributeValues'][':start_date'] = start_date
            query_params['ExpressionAttributeValues'][':end_date'] = end_date

        # Add pagination token if provided
        if next_token:
            query_params['ExclusiveStartKey'] = json.loads(next_token)

        # Execute query
        logger.info(f"Executing DynamoDB query with params: {query_params}")
        response = table.scan(**query_params)

        # Extract results
        bills = response.get('Items', [])
        count = len(bills)

        # Handle pagination
        pagination = {}
        if 'LastEvaluatedKey' in response:
            pagination['next_token'] = json.dumps(response['LastEvaluatedKey'])

        return jsonify({
            "bills": bills,
            "count": count,
            **pagination
        })

    except ValueError as e:
        logger.error(f"Invalid parameter: {str(e)}")
        return jsonify({"error": f"Invalid parameter: {str(e)}", "status": 400}), 400
    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}", "status": 500}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}", "status": 500}), 500


@app.route("/api/committees")
def get_committees():
    """
    Get committees with optional filtering.
    ---
    get:
      summary: Get committees
      description: Retrieve committees with optional filtering by congress, chamber, and date range
      parameters:
        - in: query
          name: congress
          schema:
            type: integer
          description: Filter by congress number (e.g., 117)
        - in: query
          name: chamber
          schema:
            type: string
          description: Filter by chamber (House, Senate)
        - in: query
          name: start_date
          schema:
            type: string
            format: date
          description: Filter by update date (start date, format YYYY-MM-DD)
        - in: query
          name: end_date
          schema:
            type: string
            format: date
          description: Filter by update date (end date, format YYYY-MM-DD)
        - in: query
          name: limit
          schema:
            type: integer
            default: 20
          description: Maximum number of results to return
      responses:
        200:
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  committees:
                    type: array
                    items:
                      $ref: '#/components/schemas/Committee'
                  count:
                    type: integer
                  next_token:
                    type: string
        400:
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        500:
          description: Server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
    """
    try:
        if not table:
            return jsonify({"error": "DynamoDB not configured", "status": 500}), 500

        # Parse query parameters
        congress = request.args.get('congress')
        chamber = request.args.get('chamber')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        limit = min(int(request.args.get('limit', 20)), 100)  # Cap at 100
        next_token = request.args.get('next_token')

        # Build query parameters
        query_params = {
            'FilterExpression': 'attribute_exists(id) AND #type = :type_val',
            'ExpressionAttributeNames': {
                '#type': 'type'
            },
            'ExpressionAttributeValues': {
                ':type_val': 'committee'
            },
            'Limit': limit
        }

        # Apply congress filter if provided
        if congress:
            query_params['FilterExpression'] += ' AND congress = :congress_val'
            query_params['ExpressionAttributeValues'][':congress_val'] = int(congress)

        # Apply chamber filter if provided
        if chamber:
            query_params['FilterExpression'] += ' AND chamber = :chamber_val'
            query_params['ExpressionAttributeValues'][':chamber_val'] = chamber

        # Apply date range filter if provided
        if start_date:
            query_params['FilterExpression'] += ' AND update_date BETWEEN :start_date AND :end_date'
            query_params['ExpressionAttributeValues'][':start_date'] = start_date
            query_params['ExpressionAttributeValues'][':end_date'] = end_date

        # Add pagination token if provided
        if next_token:
            query_params['ExclusiveStartKey'] = json.loads(next_token)

        # Execute query
        logger.info(f"Executing DynamoDB query with params: {query_params}")
        response = table.scan(**query_params)

        # Extract results
        committees = response.get('Items', [])
        count = len(committees)

        # Handle pagination
        pagination = {}
        if 'LastEvaluatedKey' in response:
            pagination['next_token'] = json.dumps(response['LastEvaluatedKey'])

        return jsonify({
            "committees": committees,
            "count": count,
            **pagination
        })

    except ValueError as e:
        logger.error(f"Invalid parameter: {str(e)}")
        return jsonify({"error": f"Invalid parameter: {str(e)}", "status": 400}), 400
    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}", "status": 500}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}", "status": 500}), 500


@app.route("/api/hearings")
def get_hearings():
    """
    Get hearings with optional filtering.
    ---
    get:
      summary: Get hearings
      description: Retrieve hearings with optional filtering by congress, committee, and date range
      parameters:
        - in: query
          name: congress
          schema:
            type: integer
          description: Filter by congress number (e.g., 117)
        - in: query
          name: committee
          schema:
            type: string
          description: Filter by committee system code
        - in: query
          name: chamber
          schema:
            type: string
          description: Filter by chamber (House, Senate)
        - in: query
          name: start_date
          schema:
            type: string
            format: date
          description: Filter by hearing date (start date, format YYYY-MM-DD)
        - in: query
          name: end_date
          schema:
            type: string
            format: date
          description: Filter by hearing date (end date, format YYYY-MM-DD)
        - in: query
          name: limit
          schema:
            type: integer
            default: 20
          description: Maximum number of results to return
      responses:
        200:
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  hearings:
                    type: array
                    items:
                      $ref: '#/components/schemas/Hearing'
                  count:
                    type: integer
                  next_token:
                    type: string
        400:
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        500:
          description: Server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
    """
    try:
        if not table:
            return jsonify({"error": "DynamoDB not configured", "status": 500}), 500

        # Parse query parameters
        congress = request.args.get('congress')
        committee = request.args.get('committee')
        chamber = request.args.get('chamber')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        limit = min(int(request.args.get('limit', 20)), 100)  # Cap at 100
        next_token = request.args.get('next_token')

        # Build query parameters
        query_params = {
            'FilterExpression': 'attribute_exists(id) AND #type = :type_val',
            'ExpressionAttributeNames': {
                '#type': 'type'
            },
            'ExpressionAttributeValues': {
                ':type_val': 'hearing'
            },
            'Limit': limit
        }

        # Apply congress filter if provided
        if congress:
            query_params['FilterExpression'] += ' AND congress = :congress_val'
            query_params['ExpressionAttributeValues'][':congress_val'] = int(congress)

        # Apply committee filter if provided
        if committee:
            query_params['FilterExpression'] += ' AND committee.system_code = :committee_val'
            query_params['ExpressionAttributeValues'][':committee_val'] = committee

        # Apply chamber filter if provided
        if chamber:
            query_params['FilterExpression'] += ' AND chamber = :chamber_val'
            query_params['ExpressionAttributeValues'][':chamber_val'] = chamber

        # Apply date range filter if provided
        if start_date:
            query_params['FilterExpression'] += ' AND #date BETWEEN :start_date AND :end_date'
            query_params['ExpressionAttributeNames']['#date'] = 'date'
            query_params['ExpressionAttributeValues'][':start_date'] = start_date
            query_params['ExpressionAttributeValues'][':end_date'] = end_date

        # Add pagination token if provided
        if next_token:
            query_params['ExclusiveStartKey'] = json.loads(next_token)

        # Execute query
        logger.info(f"Executing DynamoDB query with params: {query_params}")
        response = table.scan(**query_params)

        # Extract results
        hearings = response.get('Items', [])
        count = len(hearings)

        # Handle pagination
        pagination = {}
        if 'LastEvaluatedKey' in response:
            pagination['next_token'] = json.dumps(response['LastEvaluatedKey'])

        return jsonify({
            "hearings": hearings,
            "count": count,
            **pagination
        })

    except ValueError as e:
        logger.error(f"Invalid parameter: {str(e)}")
        return jsonify({"error": f"Invalid parameter: {str(e)}", "status": 400}), 400
    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}", "status": 500}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}", "status": 500}), 500


@app.route("/api/amendments")
def get_amendments():
    """
    Get amendments with optional filtering.
    ---
    get:
      summary: Get amendments
      description: Retrieve amendments with optional filtering by congress, amendment type, and date range
      parameters:
        - in: query
          name: congress
          schema:
            type: integer
          description: Filter by congress number (e.g., 117)
        - in: query
          name: amendment_type
          schema:
            type: string
          description: Filter by amendment type
        - in: query
          name: start_date
          schema:
            type: string
            format: date
          description: Filter by update date (start date, format YYYY-MM-DD)
        - in: query
          name: end_date
          schema:
            type: string
            format: date
          description: Filter by update date (end date, format YYYY-MM-DD)
        - in: query
          name: limit
          schema:
            type: integer
            default: 20
          description: Maximum number of results to return
      responses:
        200:
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  amendments:
                    type: array
                    items:
                      $ref: '#/components/schemas/Amendment'
                  count:
                    type: integer
                  next_token:
                    type: string
        400:
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        500:
          description: Server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
    """
    try:
        if not table:
            return jsonify({"error": "DynamoDB not configured", "status": 500}), 500

        # Parse query parameters
        congress = request.args.get('congress')
        amendment_type = request.args.get('amendment_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        limit = min(int(request.args.get('limit', 20)), 100)  # Cap at 100
        next_token = request.args.get('next_token')

        # Build query parameters
        query_params = {
            'FilterExpression': 'attribute_exists(id) AND #type = :type_val',
            'ExpressionAttributeNames': {
                '#type': 'type'
            },
            'ExpressionAttributeValues': {
                ':type_val': 'amendment'
            },
            'Limit': limit
        }

        # Apply congress filter if provided
        if congress:
            query_params['FilterExpression'] += ' AND congress = :congress_val'
            query_params['ExpressionAttributeValues'][':congress_val'] = int(congress)

        # Apply amendment_type filter if provided
        if amendment_type:
            query_params['FilterExpression'] += ' AND amendment_type = :amendment_type_val'
            query_params['ExpressionAttributeValues'][':amendment_type_val'] = amendment_type

        # Apply date range filter if provided
        if start_date:
            query_params['FilterExpression'] += ' AND update_date BETWEEN :start_date AND :end_date'
            query_params['ExpressionAttributeValues'][':start_date'] = start_date
            query_params['ExpressionAttributeValues'][':end_date'] = end_date

        # Add pagination token if provided
        if next_token:
            query_params['ExclusiveStartKey'] = json.loads(next_token)

        # Execute query
        logger.info(f"Executing DynamoDB query with params: {query_params}")
        response = table.scan(**query_params)

        # Extract results
        amendments = response.get('Items', [])
        count = len(amendments)

        # Handle pagination
        pagination = {}
        if 'LastEvaluatedKey' in response:
            pagination['next_token'] = json.dumps(response['LastEvaluatedKey'])

        return jsonify({
            "amendments": amendments,
            "count": count,
            **pagination
        })

    except ValueError as e:
        logger.error(f"Invalid parameter: {str(e)}")
        return jsonify({"error": f"Invalid parameter: {str(e)}", "status": 400}), 400
    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}", "status": 500}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}", "status": 500}), 500


@app.route("/api/nominations")
def get_nominations():
    """
    Get nominations with optional filtering.
    ---
    get:
      summary: Get nominations
      description: Retrieve nominations with optional filtering by congress, organization, and date range
      parameters:
        - in: query
          name: congress
          schema:
            type: integer
          description: Filter by congress number (e.g., 117)
        - in: query
          name: organization
          schema:
            type: string
          description: Filter by organization
        - in: query
          name: start_date
          schema:
            type: string
            format: date
          description: Filter by update date (start date, format YYYY-MM-DD)
        - in: query
          name: end_date
          schema:
            type: string
            format: date
          description: Filter by update date (end date, format YYYY-MM-DD)
        - in: query
          name: limit
          schema:
            type: integer
            default: 20
          description: Maximum number of results to return
      responses:
        200:
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  nominations:
                    type: array
                    items:
                      $ref: '#/components/schemas/Nomination'
                  count:
                    type: integer
                  next_token:
                    type: string
        400:
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        500:
          description: Server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
    """
    try:
        if not table:
            return jsonify({"error": "DynamoDB not configured", "status": 500}), 500

        # Parse query parameters
        congress = request.args.get('congress')
        organization = request.args.get('organization')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        limit = min(int(request.args.get('limit', 20)), 100)  # Cap at 100
        next_token = request.args.get('next_token')

        # Build query parameters
        query_params = {
            'FilterExpression': 'attribute_exists(id) AND #type = :type_val',
            'ExpressionAttributeNames': {
                '#type': 'type'
            },
            'ExpressionAttributeValues': {
                ':type_val': 'nomination'
            },
            'Limit': limit
        }

        # Apply congress filter if provided
        if congress:
            query_params['FilterExpression'] += ' AND congress = :congress_val'
            query_params['ExpressionAttributeValues'][':congress_val'] = int(congress)

        # Apply organization filter if provided
        if organization:
            query_params['FilterExpression'] += ' AND contains(organization, :organization_val)'
            query_params['ExpressionAttributeValues'][':organization_val'] = organization

        # Apply date range filter if provided
        if start_date:
            query_params['FilterExpression'] += ' AND update_date BETWEEN :start_date AND :end_date'
            query_params['ExpressionAttributeValues'][':start_date'] = start_date
            query_params['ExpressionAttributeValues'][':end_date'] = end_date

        # Add pagination token if provided
        if next_token:
            query_params['ExclusiveStartKey'] = json.loads(next_token)

        # Execute query
        logger.info(f"Executing DynamoDB query with params: {query_params}")
        response = table.scan(**query_params)

        # Extract results
        nominations = response.get('Items', [])
        count = len(nominations)

        # Handle pagination
        pagination = {}
        if 'LastEvaluatedKey' in response:
            pagination['next_token'] = json.dumps(response['LastEvaluatedKey'])

        return jsonify({
            "nominations": nominations,
            "count": count,
            **pagination
        })

    except ValueError as e:
        logger.error(f"Invalid parameter: {str(e)}")
        return jsonify({"error": f"Invalid parameter: {str(e)}", "status": 400}), 400
    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}", "status": 500}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}", "status": 500}), 500


@app.route("/api/treaties")
def get_treaties():
    """
    Get treaties with optional filtering.
    ---
    get:
      summary: Get treaties
      description: Retrieve treaties with optional filtering by congress, country, and date range
      parameters:
        - in: query
          name: congress
          schema:
            type: integer
          description: Filter by congress number (e.g., 117)
        - in: query
          name: country
          schema:
            type: string
          description: Filter by country
        - in: query
          name: start_date
          schema:
            type: string
            format: date
          description: Filter by update date (start date, format YYYY-MM-DD)
        - in: query
          name: end_date
          schema:
            type: string
            format: date
          description: Filter by update date (end date, format YYYY-MM-DD)
        - in: query
          name: limit
          schema:
            type: integer
            default: 20
          description: Maximum number of results to return
      responses:
        200:
          description: Successful response
          content:
            application/json:
              schema:
                type: object
                properties:
                  treaties:
                    type: array
                    items:
                      $ref: '#/components/schemas/Treaty'
                  count:
                    type: integer
                  next_token:
                    type: string
        400:
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        500:
          description: Server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
    """
    try:
        if not table:
            return jsonify({"error": "DynamoDB not configured", "status": 500}), 500

        # Parse query parameters
        congress = request.args.get('congress')
        country = request.args.get('country')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        limit = min(int(request.args.get('limit', 20)), 100)  # Cap at 100
        next_token = request.args.get('next_token')

        # Build query parameters
        query_params = {
            'FilterExpression': 'attribute_exists(id) AND #type = :type_val',
            'ExpressionAttributeNames': {
                '#type': 'type'
            },
            'ExpressionAttributeValues': {
                ':type_val': 'treaty'
            },
            'Limit': limit
        }

        # Apply congress filter if provided
        if congress:
            query_params['FilterExpression'] += ' AND congress = :congress_val'
            query_params['ExpressionAttributeValues'][':congress_val'] = int(congress)

        # Apply country filter if provided
        if country:
            query_params['FilterExpression'] += ' AND contains(country, :country_val)'
            query_params['ExpressionAttributeValues'][':country_val'] = country

        # Apply date range filter if provided
        if start_date:
            query_params['FilterExpression'] += ' AND update_date BETWEEN :start_date AND :end_date'
            query_params['ExpressionAttributeValues'][':start_date'] = start_date
            query_params['ExpressionAttributeValues'][':end_date'] = end_date

        # Add pagination token if provided
        if next_token:
            query_params['ExclusiveStartKey'] = json.loads(next_token)

        # Execute query
        logger.info(f"Executing DynamoDB query with params: {query_params}")
        response = table.scan(**query_params)

        # Extract results
        treaties = response.get('Items', [])
        count = len(treaties)

        # Handle pagination
        pagination = {}
        if 'LastEvaluatedKey' in response:
            pagination['next_token'] = json.dumps(response['LastEvaluatedKey'])

        return jsonify({
            "treaties": treaties,
            "count": count,
            **pagination
        })

    except ValueError as e:
        logger.error(f"Invalid parameter: {str(e)}")
        return jsonify({"error": f"Invalid parameter: {str(e)}", "status": 400}), 400
    except ClientError as e:
        logger.error(f"DynamoDB error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}", "status": 500}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Unexpected error: {str(e)}", "status": 500}), 500


# Create Swagger UI blueprint
SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.json'

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Congress Data API"
    }
)

# Register blueprint
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Create static directory if it doesn't exist
os.makedirs('static', exist_ok=True)

# Generate OpenAPI spec
with app.test_request_context():
    # Add paths to spec
    spec.path(view=get_bills)
    spec.path(view=get_committees)
    spec.path(view=get_hearings)
    spec.path(view=get_amendments)
    spec.path(view=get_nominations)
    spec.path(view=get_treaties)

# Write OpenAPI spec to file
with open('static/swagger.json', 'w') as f:
    json.dump(spec.to_dict(), f)

# Serve static files
@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)