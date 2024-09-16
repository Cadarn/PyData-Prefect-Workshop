import pytest
from solution.s01_my_first_flow import add, square_num, add_and_square

# Reusable pytest fixture to provide test data
@pytest.fixture
def number_data():
    return {
        "a": 2,
        "b": 3,
        "expected_sum": 5,
        "expected_sum_square": 25
    }

# Test the `add` task directly using core logic
def test_add(number_data):
    result = add.fn(number_data["a"], number_data["b"])  # Bypassing Prefect's task layer
    assert result == number_data["expected_sum"], f"Expected {number_data['expected_sum']}, got {result}"

# Test the `square_num` task directly
def test_square_num(number_data):
    result = square_num.fn(number_data["expected_sum"])  # Passing the result from `add`
    assert result == number_data["expected_sum_square"], f"Expected {number_data['expected_sum_square']}, got {result}"


# Test the flow directly, bypassing the Prefect orchestration
def test_add_and_square_sysout_flow(number_data, capsys):
    _ = add_and_square.fn(number_data["a"], number_data["b"])  # Run flow logic directly
    captured = capsys.readouterr()  # Capture print output
    assert str(number_data["expected_sum_square"]) in captured.out, "Flow output does not match expected value"
