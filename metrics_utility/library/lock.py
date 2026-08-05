"""PostgreSQL advisory-lock context manager."""

from contextlib import contextmanager


@contextmanager
def lock(
    key,
    wait=False,
    db=None,
):
    """Acquire a PostgreSQL advisory lock by name and yield whether it was acquired.

    Uses ``pg_try_advisory_lock`` (non-blocking) when *wait* is False, or
    ``pg_advisory_lock`` (blocking) when *wait* is True.  The lock is released
    in the ``finally`` block.

    Args:
        key: A unique string identifying the lock (hashed to a bigint internally).
        wait: When True, block until the lock is available.  When False (default),
            yield immediately with ``acquired=False`` if the lock is held elsewhere.
        db: Django database connection used to execute the lock SQL.

    Yields:
        bool: True if the lock was successfully acquired, False otherwise.

    Raises:
        ValueError: If *key* is not a string.
    """

    if not isinstance(key, str):
        raise ValueError(f'Cannot use {key} as a lock id')

    function_name = 'pg_advisory_lock'
    if not wait:
        function_name = 'pg_try_advisory_lock'
    release_function_name = 'pg_advisory_unlock'

    with db.cursor() as cursor:
        cursor.execute('SELECT hashtext(%s)::bigint', (key,))
        pos = cursor.fetchone()[0]
        base = 'SELECT %s(%d)'
        params = (pos % (2**63),)
        acquire_params = (function_name, *params)
        command = base % acquire_params

        cursor.execute(command)

        if not wait:
            acquired = cursor.fetchone()[0]
        else:
            acquired = True

        try:
            yield acquired
        finally:
            if acquired:
                release_params = (release_function_name, *params)

                command = base % release_params
                cursor.execute(command)
