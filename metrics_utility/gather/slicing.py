from django.utils.timezone import now, timedelta


def daily_slicing(*, key, last_gather, since=None, until=None, last_gathered_entries=None, **kwargs):
    """Generate time slices aligned to calendar-day boundaries for hourly collectors."""
    until = until or now()
    if since is not None:
        last_entry = since
    else:
        last_gathered_entries = last_gathered_entries or {}
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


def until_slicing(*, until=None, **kwargs):
    """Generate a single snapshot slice for full-table-scan collectors."""
    until = until or now()
    last_sec = until - timedelta(seconds=1)
    yield (last_sec, last_sec)
