from contextlib import contextmanager


@contextmanager
def lock(
    key,
    wait=False,
    db=None,
):
    # Acquire the lock and yield to caller

    if not isinstance(key, str):
        raise ValueError('Cannot use %s as a lock id' % key)

    function_name = 'pg_advisory_lock'
    if not wait:
        function_name = 'pg_try_advisory_lock'
    release_function_name = 'pg_advisory_unlock'

    with db.cursor() as cursor:
        cursor.execute('SELECT hashtext(%s)::bigint', (key,))
        pos = cursor.fetchone()[0]
        base = 'SELECT %s(%d)'
        params = (pos % (2**63),)
        acquire_params = (function_name,) + params
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
                release_params = (release_function_name,) + params

                command = base % release_params
                cursor.execute(command)
