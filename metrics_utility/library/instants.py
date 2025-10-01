from datetime import datetime, timedelta, timezone


def last_day():
    print("library.instants last_day")
    return datetime.now(tz=timezone.utc) - timedelta(days=1)


def this_day():
    print("library.instants this_day")
    return datetime.now(tz=timezone.utc)


def last_week():
    print("library.instants last_week")
    return datetime.now(tz=timezone.utc) - timedelta(weeks=1)


def this_week():
    print("library.instants this_week")
    return datetime.now(tz=timezone.utc)


def last_month():
    print("library.instants last_month")
    return datetime.now(tz=timezone.utc) - timedelta(days=30)


def this_month():
    print("library.instants this_month")
    return datetime.now(tz=timezone.utc)


def months_ago(months):
    print("library.instants months_ago")
    return datetime.now(tz=timezone.utc) - timedelta(days=30 * months)


def minutes_ago(minutes):
    print("library.instants minutes_ago")
    return datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)


def now():
    print("library.instants now")
    return datetime.now(tz=timezone.utc)