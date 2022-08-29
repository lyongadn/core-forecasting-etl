"""Unit tests for functions in the lambda function."""
# Instructs pylint to be quite about docstrings in this module.
# IMHO a test's name should adequately describe its intent.
# pylint: disable=missing-docstring
from core_forecast_qc_dl_initial import lambda_function


def test_lambda_handler(tmpdir, monkeypatch):
    event = "Running test cases"
    context = {
        "Context": "This is the conext"
    }
    response = lambda_function.lambda_handler(event, context)
    expected = {
        "message": "Hello World",
        "event": event
    }

    assert response == expected
