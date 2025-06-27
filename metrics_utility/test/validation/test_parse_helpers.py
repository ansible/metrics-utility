import datetime

from unittest.mock import patch

import pytest

from metrics_utility.exceptions import MetricsException
from metrics_utility.management.validation import (
    handle_datelike,
    parse_date_param,
    parse_number_of_days,
)


def test_parse_date_param():
    assert parse_date_param(None) is None
    assert parse_date_param('1d') is not None
    assert parse_date_param('2mo') is not None
    assert parse_date_param('3m') is not None
    assert parse_date_param('2024-01-01') == datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)

    # patch now() to be 2024-02-29 13:59:00 (w/o tz)
    with patch('metrics_utility.management.validation.now') as mock:
        mock.return_value = datetime.datetime(2024, 2, 29, 13, 59, 0)

        assert parse_date_param('1d') == datetime.datetime(2024, 2, 29, 0, 0, tzinfo=datetime.timezone.utc)
        assert parse_date_param('2mo') == datetime.datetime(2023, 12, 29, 0, 0, tzinfo=datetime.timezone.utc)
        assert parse_date_param('3m') == datetime.datetime(2024, 2, 29, 13, 56, tzinfo=datetime.timezone.utc)

    # ensure timezone
    assert parse_date_param('3m').tzinfo == datetime.timezone.utc
    assert parse_date_param('2024-01-01').tzinfo == datetime.timezone.utc


def test_handle_datelike():
    assert handle_datelike(None) is None
    assert handle_datelike('1d') is not None
    assert handle_datelike('3m') is not None
    assert handle_datelike('2024-01-01') == datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)

    # patch now() to be 2024-02-29 13:59:00 (w/o tz)
    with patch('metrics_utility.management.validation.now') as mock:
        mock.return_value = datetime.datetime(2024, 2, 29, 13, 59, 0)

        assert handle_datelike('1d') == datetime.datetime(2024, 2, 29, 0, 0, tzinfo=datetime.timezone.utc)
        assert handle_datelike('3m') == datetime.datetime(2024, 2, 29, 13, 56, tzinfo=datetime.timezone.utc)

    # ensure timezone
    assert handle_datelike('3m').tzinfo == datetime.timezone.utc
    assert handle_datelike('2024-01-01').tzinfo == datetime.timezone.utc


def test_parse_number_of_days():
    assert parse_number_of_days(None) is None
    assert parse_number_of_days('1d') == 1
    assert parse_number_of_days('2mo') == 60

    # bare number invalid
    with pytest.raises(MetricsException):
        parse_number_of_days('3')
