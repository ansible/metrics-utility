import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
from django.utils import timezone
from metrics_utility.exceptions import UnparsableParameter


def parse_date_param(date_option):
    parsed_date = None
    if date_option and date_option.endswith('d'):
        days_ago = int(date_option[0:-1])
        parsed_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_option and (date_option.endswith('mo') or date_option.endswith('month') or date_option.endswith('months')):
        if date_option.endswith('mo'):
            suffix_length = len('mo')
        elif date_option.endswith('month'):
            suffix_length = len('month')
        elif date_option.endswith('months'):
            suffix_length = len('months')
        months_ago = int(date_option[0:-suffix_length])
        parsed_date = (datetime.datetime.now() - relativedelta(months=months_ago)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_option and date_option.endswith('m'):
        minutes_ago = int(date_option[0:-1])
        parsed_date = datetime.datetime.now() - datetime.timedelta(minutes=minutes_ago)
    else:
        parsed_date = parser.parse(date_option) if date_option else None
    # Add default utc timezone
    if parsed_date and parsed_date.tzinfo is None:
        parsed_date = parsed_date.replace(tzinfo=timezone.utc)

    return parsed_date


def parse_number_of_days(date_option):
    if date_option and (date_option.endswith('d') or date_option.endswith('day') or date_option.endswith('days')):
        if date_option.endswith('d'):
            suffix_length = len('d')
        elif date_option.endswith('day'):
            suffix_length = len('day')
        elif date_option.endswith('days'):
            suffix_length = len('days')

        days = int(date_option[0:-suffix_length])
    elif date_option and (date_option.endswith('mo') or date_option.endswith('month') or date_option.endswith('months')):
        if date_option.endswith('mo'):
            suffix_length = len('mo')
        elif date_option.endswith('month'):
            suffix_length = len('month')
        elif date_option.endswith('months'):
            suffix_length = len('months')

        days = int(date_option[0:-suffix_length]) * 30  # using 30 days per month
    else:
        raise UnparsableParameter(f"Can't parse parameter value {date_option}")

    return days
