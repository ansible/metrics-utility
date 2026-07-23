"""Canonical event-type constants shared by collectors and rollups.

Any change here is automatically picked up by both
``library.collectors.controller.main_jobevent_service`` and
``anonymized_rollups.events_modules_anonymized_rollup``.
"""

RUNNER_EVENTS = frozenset(
    [
        'runner_on_ok',
        'runner_on_async_ok',
        'runner_item_on_ok',
        'runner_on_failed',
        'runner_on_async_failed',
        'runner_item_on_failed',
        'runner_on_unreachable',
        'runner_retry',
    ]
)
