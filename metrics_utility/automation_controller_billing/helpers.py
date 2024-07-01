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

# HOST 1
{
    'ansible_board_serial': 'abc',
    'ansible_machine_id': '111',
    'ansible_host_variable': ['test_real_host_11', 'test_real_host_21'],
    'host_name': ['test_host_11', 'test_host_21']
}

# HOST 2 duplicate
{
    'ansible_board_serial': 'abc',
    'ansible_machine_id': '111',
    'ansible_host_variable': ['test_real_host_11', 'test_real_host_21'],
    'host_name': ['test_host_11', 'test_host_21']
}

# HOST 3 duplicate missing board_serial
{
    'ansible_board_serial': '',
    'ansible_machine_id': '111',
    'ansible_host_variable': ['test_real_host_11', 'test_real_host_21'],
    'host_name': ['test_host_11', 'test_host_21']
}

# HOST 4 duplicate different machine id same hosts
{
    'ansible_board_serial': '',
    'ansible_machine_id': '222',
    'ansible_host_variable': ['test_real_host_11', 'test_real_host_21'],
    'host_name': ['test_host_11', 'test_host_21']
}

# HOST 5 duplicate different board_serial same hosts
{
    'ansible_board_serial': 'def',
    'ansible_machine_id': '',
    'ansible_host_variable': ['test_real_host_11', 'test_real_host_21'],
    'host_name': ['test_host_11', 'test_host_21']
}

# HOST 6 duplicate different machine_id
{
    'ansible_board_serial': 'abc',
    'ansible_machine_id': '1234',
    'ansible_host_variable': ['test_real_host_11', 'test_real_host_21'],
    'host_name': ['test_host_11', 'test_host_21']
}

# HOST 7 duplicate different machine_id and hosts
{
    'ansible_board_serial': 'abc',
    'ansible_machine_id': '12345',
    'ansible_host_variable': ['test_real_host_51', 'test_real_host_61'],
    'host_name': ['test_host_51', 'test_host_61']
}

# HOST 8 duplicate differernt machine_id and overlapping hosts
{
    'ansible_board_serial': 'abc',
    'ansible_machine_id': '12345',
    'ansible_host_variable': ['test_real_host_11', 'test_real_host_51'],
    'host_name': ['test_host_11', 'test_real_host_51']
}

# HOST 9 duplicate matching host_variable
{
    'ansible_board_serial': '',
    'ansible_machine_id': '',
    'ansible_host_variable': ['test_real_host_11'],
    'host_name': ['test_host_11']
}

# HOST 10 duplicate matching host_name
{
    'ansible_board_serial': '',
    'ansible_machine_id': '',
    'ansible_host_variable': [],
    'host_name': ['test_host_11']
}

ELEVATED_CANONICAL_FACT_FIELDS = ("provider_id", "insights_id", "subscription_manager_id")

def contains_no_elevated_facts_query(query):
    return query.filter(not_(Host.canonical_facts.has_any(array(ELEVATED_CANONICAL_FACT_FIELDS))))

# Get hosts by the highest elevated canonical fact present
def find_host_list_by_elevated_canonical_facts(elevated_cfs, query, logger):
    """
    First check if multiple hosts are returned.  If they are then retain the one with the highest
    priority elevated fact
    """
    logger.debug("find_host_by_elevated_canonical_facts(%s)", elevated_cfs)

    if elevated_cfs.get("provider_id"):
        elevated_cfs.pop("subscription_manager_id", None)
        elevated_cfs.pop("insights_id", None)
    elif elevated_cfs.get("insights_id"):
        elevated_cfs.pop("subscription_manager_id", None)

    hosts = multiple_canonical_facts_host_query(elevated_cfs, query).order_by(Host.modified_on.desc()).all()

    return hosts

def matches_at_least_one_canonical_fact_filter(canonical_facts):
    # Contains at least one correct CF value
    # Correct value = contains key:value
    # -> OR( *correct values )
    return or_(Host.canonical_facts.contains({key: value}) for key, value in canonical_facts.items())


def contains_no_incorrect_facts_filter(canonical_facts):
    # Does not contain any incorrect CF values
    # Incorrect value = AND( key exists, NOT( contains key:value ) )
    # -> NOT( OR( *Incorrect values ) )
    filter_ = ()
    for key, value in canonical_facts.items():
        filter_ += (
            and_(Host.canonical_facts.has_key(key), not_(Host.canonical_facts.contains({key: value}))),  # noqa: W601
        )

    return not_(or_(*filter_))


def multiple_canonical_facts_host_query(canonical_facts, query):
    query = query.filter(
        (contains_no_incorrect_facts_filter(canonical_facts))
        & (matches_at_least_one_canonical_fact_filter(canonical_facts))
    )
    return query
