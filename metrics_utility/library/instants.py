from datetime import datetime, timedelta, timezone

from .debug import log


def last_day():
    log('library.instants last_day')
    return datetime.now(tz=timezone.utc) - timedelta(days=1)


def this_day():
    log('library.instants this_day')
    return datetime.now(tz=timezone.utc)


def last_week():
    log('library.instants last_week')
    return datetime.now(tz=timezone.utc) - timedelta(weeks=1)


def this_week():
    log('library.instants this_week')
    return datetime.now(tz=timezone.utc)


def last_month():
    log('library.instants last_month')
    return datetime.now(tz=timezone.utc) - timedelta(days=30)


def this_month():
    log('library.instants this_month')
    return datetime.now(tz=timezone.utc)


def months_ago(months):
    log('library.instants months_ago')
    return datetime.now(tz=timezone.utc) - timedelta(days=30 * months)


def minutes_ago(minutes):
    log('library.instants minutes_ago')
    return datetime.now(tz=timezone.utc) - timedelta(minutes=minutes)


def now():
    log('library.instants now')
    return datetime.now(tz=timezone.utc)
