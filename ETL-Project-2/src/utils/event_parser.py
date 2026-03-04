"""
Event parsing utilities for Kinesis messages.

Handles JSON parsing, timestamp extraction, and schema validation.
"""

import json
import logging

logger = logging.getLogger(__name__)


def parse_kinesis_message(message):
    """
    Parse a raw Kinesis record and return the event as a dict.

    Args:
        message: Kinesis record dict with a 'data' key (bytes or str).

    Returns:
        Parsed event dict, or None if decoding/JSON parsing fails.
    """
    try:
        # Extract JSON data from Kinesis record
        data = message.get('data', {})
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        if isinstance(data, str):
            data = json.loads(data)

        return data
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        logger.warning(f"Failed to parse message: {e}")
        return None


def validate_event(event):
    """
    Check that an event contains all required fields.

    Args:
        event: Event dict.

    Returns:
        True if user_id, track_id and timestamp are all present, False otherwise.
    """
    required_fields = {'user_id', 'track_id', 'timestamp'}
    return all(field in event for field in required_fields)
