import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from guardrails import is_off_limits, is_low_confidence, OFF_LIMITS_RESPONSE


def test_salary_question_is_off_limits():
    assert is_off_limits("What is Praveen's expected salary?") is True


def test_ctc_question_is_off_limits():
    assert is_off_limits("What CTC is Praveen looking for?") is True


def test_interview_status_is_off_limits():
    assert is_off_limits("Which companies is Praveen interviewing with?") is True


def test_notice_period_is_off_limits():
    assert is_off_limits("What is Praveen's notice period?") is True


def test_normal_project_question_is_not_off_limits():
    assert is_off_limits("How does the F1 pipeline handle upserts?") is False


def test_background_question_is_not_off_limits():
    assert is_off_limits("Tell me about Praveen's experience at Amazon") is False


def test_off_limits_response_contains_email():
    assert "pravbala30@gmail.com" in OFF_LIMITS_RESPONSE


def test_off_limits_response_contains_linkedin():
    assert "LinkedIn" in OFF_LIMITS_RESPONSE


def test_low_confidence_all_below_threshold():
    chunks = [
        {"score": 0.20},
        {"score": 0.25},
        {"score": 0.30},
    ]
    assert is_low_confidence(chunks) is True


def test_low_confidence_one_above_threshold():
    chunks = [
        {"score": 0.20},
        {"score": 0.40},
        {"score": 0.25},
    ]
    assert is_low_confidence(chunks) is False


def test_low_confidence_empty_chunks():
    assert is_low_confidence([]) is True
