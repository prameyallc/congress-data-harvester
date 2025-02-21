#!/usr/bin/env python3
import json
from dynamo_handler import DynamoHandler
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('committee_test')

def analyze_committee_data(committee):
    """Analyze a committee record and return key information"""
    return {
        'name': committee.get('name', 'N/A'),
        'type': committee.get('committee_type', 'N/A'),
        'chamber': committee.get('chamber', 'N/A'),
        'congress': committee.get('congress', 'N/A'),
        'last_updated': committee.get('update_date', 'N/A')
    }

def main():
    # Load config
    with open('config.json', 'r') as f:
        config = json.load(f)

    # Initialize DynamoDB handler
    dynamo = DynamoHandler(config['dynamodb'])

    # Query committees by type
    committees = dynamo.scan_by_type('committee')

    logger.info(f"\nFound {len(committees)} total committees")

    if committees:
        # Analyze committee distribution
        chamber_count = {}
        type_count = {}

        for committee in committees:
            info = analyze_committee_data(committee)
            chamber_count[info['chamber']] = chamber_count.get(info['chamber'], 0) + 1
            type_count[info['type']] = type_count.get(info['type'], 0) + 1

        logger.info("\nCommittee distribution by chamber:")
        for chamber, count in chamber_count.items():
            logger.info(f"  {chamber}: {count}")

        logger.info("\nCommittee distribution by type:")
        for comm_type, count in type_count.items():
            logger.info(f"  {comm_type}: {count}")

        logger.info("\nSample committee data:")
        logger.info(json.dumps(analyze_committee_data(committees[0]), indent=2))

    # Query by date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    recent_committees = dynamo.query_by_type_and_date_range(
        'committee',
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )

    logger.info(f"\nFound {len(recent_committees)} committees updated in the last 30 days")

    if recent_committees:
        logger.info("\nMost recently updated committees:")
        for committee in sorted(
            recent_committees,
            key=lambda x: x.get('update_date', ''),
            reverse=True
        )[:5]:
            info = analyze_committee_data(committee)
            logger.info(f"  {info['name']} ({info['chamber']}) - Last updated: {info['last_updated']}")

if __name__ == "__main__":
    main()