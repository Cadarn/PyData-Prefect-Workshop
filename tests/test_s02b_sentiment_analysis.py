import pytest
from unittest.mock import patch, MagicMock
from solution.s02b_sentiment_pipeline_v2 import calc_sentiment, sentiment_analysis

@pytest.mark.parametrize("input_text, expected_sentiment", [
    ("I love this!", 0.625),
    ("I hate this!", -1.0)
])
def test_calc_sentiment(input_text, expected_sentiment):
    sentiment = calc_sentiment(input_text)
    assert abs(sentiment - expected_sentiment) < 0.01

@pytest.mark.parametrize("input_text, expected_sentiment", [
    ("I love this!", 0.625),
    ("I hate this!", -1.0)
])
@patch("solution.s02b_sentiment_pipeline_v2.get_run_logger")
def test_sentiment_analysis(mock_logger, input_text, expected_sentiment):
    mock_logger.return_value = MagicMock()  # Mock the logger to avoid logging issues
    sentiment = sentiment_analysis(input_text)
    assert abs(sentiment - expected_sentiment) < 0.01  # Allow a small tolerance

