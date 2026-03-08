"""Unit tests for ETL-Project-2."""

import pytest
from src.utils.event_parser import validate_event, parse_kinesis_message


def test_validate_event_valid():
    """Test that valid events pass validation."""
    event = {
        'user_id': '123',
        'track_id': 'abc',
        'timestamp': '2024-03-02T10:30:00'
    }
    assert validate_event(event) is True


def test_validate_event_missing_field():
    """Test that missing fields fail validation."""
    event = {
        'user_id': '123',
        'track_id': 'abc'
    }
    assert validate_event(event) is False


def test_parse_kinesis_message():
    """Test parsing Kinesis message."""
    message = {
        'data': b'{"user_id": "123", "track_id": "abc", "timestamp": "2024-03-02T10:30:00"}'
    }
    result = parse_kinesis_message(message)
    assert result is not None
    assert result['user_id'] == '123'


def test_parse_kinesis_invalid_json():
    """Test handling of invalid JSON."""
    message = {
        'data': b'invalid json'
    }
    result = parse_kinesis_message(message)
    assert result is None
