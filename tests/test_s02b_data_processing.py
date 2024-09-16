import pytest
from unittest.mock import patch, MagicMock
from solution.s02b_sentiment_pipeline_v2 import (
    lowercase_text,
    strip_url,
    strip_user,
    replace_emoji,
    lemmatize_text,
    process_text,
)

@pytest.mark.parametrize("input_text, expected_output", [
    ("Hello World!", "hello world!"),
    ("   Whitespace    ", "whitespace"),
    ("MIXED CASE", "mixed case")
])
def test_lowercase_text(input_text, expected_output):
    assert lowercase_text.fn(input_text) == expected_output

@pytest.mark.parametrize("input_text, expected_output", [
    ("Check out this link: https://example.com", "Check out this link: WEBADDRESS"),
    ("Visit www.example.org for more info", "Visit WEBADDRESS for more info")
])
def test_strip_url(input_text, expected_output):
    assert strip_url.fn(input_text) == expected_output

@pytest.mark.parametrize("input_text, expected_output", [
    ("Hello @user!", "Hello USERHANDLE"),
    ("@JohnDoe mentioned me", "USERHANDLE mentioned me")
])
def test_strip_user(input_text, expected_output):
    assert strip_user.fn(input_text) == expected_output

@pytest.mark.parametrize("input_text, expected_output", [
    (":) :-D", "smile EMOJI smile EMOJI"),
    (":(", "sad EMOJI")
])
def test_replace_emoji(input_text, expected_output):
    assert replace_emoji.fn(input_text) == expected_output

@pytest.mark.parametrize("input_text, expected_output", [
    ("running running running", "running running running"),
    ("better better best", "better better best")
])
def test_lemmatize_text(input_text, expected_output):
    assert lemmatize_text.fn(input_text) == expected_output


# Testing the flow
@pytest.mark.parametrize("input_text, expected_output", [
    ("Hello :) https://example.com @user", "hello smile EMOJI WEBADDRESS USERHANDLE"),
    ("No URLs or handles", "no url or handle")
])
@patch("solution.s02b_sentiment_pipeline_v2.get_run_logger")
def test_process_text(mock_logger, input_text, expected_output):
    mock_logger.return_value = MagicMock()  # Mock the logger to avoid logging issues
    result = process_text.fn(input_text)
    assert result == expected_output