from django.utils.timezone import now, timedelta


def daily_slicing(key, last_gather, **kwargs):
    """Generate time slices aligned to calendar-day boundaries for hourly collectors.

    Yields ``(start, end)`` pairs that never cross a midnight boundary, each
    spanning at most ``METRICS_UTILITY_GATHER_INTERVAL_HOURS`` hours.

    Args:
        key: Collector key name used to look up the last-gathered entry.
        last_gather: Horizon datetime (now - max_gather_period_days).
        **kwargs: Accepts ``since``, ``until``, and ``last_gathered_entries``.

    Yields:
        Tuple of ``(since, until)`` timezone-aware datetimes.
    """
    since, until = kwargs.get('since', None), kwargs.get('until', now())
    if since is not None:
        last_entry = since
    else:
        last_gathered_entries = kwargs.get('last_gathered_entries', {})
        try:
            last_entry = max(last_gathered_entries.get(key) or last_gather, last_gather)
        except TypeError:
            last_entry = last_gather

    start, end = last_entry, None
    start_beginning_of_next_day = start.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    # If the date range is over one day, we want first interval to contain the rest of the day
    # then we'll cycle by full days
    if until > start_beginning_of_next_day:
        yield (start, start_beginning_of_next_day)
        start = start_beginning_of_next_day

    while start < until:
        end = min(start + timedelta(days=1), until)
        yield (start, end)
        start = end


def until_slicing(_key, _last_gather, **kwargs):
    """Generate a single snapshot slice positioned at ``until - 1 second``.

    Used for snapshot collectors that always perform a full-table scan and
    should be stored in the last-second of the ``until`` partition.

    Args:
        _key: Unused collector key.
        _last_gather: Unused last-gather datetime.
        **kwargs: Accepts optional ``until`` datetime (defaults to now).

    Yields:
        A single ``(last_sec, last_sec)`` tuple.
    """
    # For tables where we always need to do a table full scan, ignoring since & until
    # Always store the inventory snapshot into the last daily partition (until - 1 second)
    until = kwargs.get('until', now())
    last_sec = until - timedelta(seconds=1)
    yield (last_sec, last_sec)
