"""Collector for analytics query metadata."""

from ..util import DictOutput, collector


@collector
def query_info(*, since=None, until=None, collection_type='metrics-service', output=DictOutput()):
    """Return metadata about this analytics collection run."""
    return output.dict(
        {
            'last_run': str(since),
            'current_time': str(until),
            'collection_type': collection_type,
        }
    )
