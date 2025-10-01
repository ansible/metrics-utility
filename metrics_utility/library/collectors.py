from .debug import log


def collector(func):
    """Decorator that creates a collector class and returns a constructor function."""

    class CollectorClass(BaseCollector):
        collector_fn = staticmethod(func)
        collector_key = func.__name__

    def constructor(**kwargs):
        return CollectorClass(**kwargs)

    return constructor


class BaseCollector:
    def __init__(self, **kwargs):
        log(f'library.collectors {self.__class__.__name__}.__init__')
        self.kwargs = kwargs

    def gather(self):
        log(f'library.collectors {self.__class__.__name__}.gather')
        return self.collector_fn(**self.kwargs)


@collector
def anonymous(db=None, since=None, until=None, custom_params=None):
    log('library.collectors anonymous')
    return {'fake': 'anonymous_data'}


@collector
def config(db=None):
    log('library.collectors config')
    return {'fake': 'config_data'}


@collector
def job_host_summary(db=None, since=None, until=None):
    log('library.collectors job_host_summary')
    return {'fake': 'job_host_summary_data'}


@collector
def main_host(db=None):
    log('library.collectors main_host')
    return {'fake': 'main_host_data'}


@collector
def main_jobevent(db=None, since=None, until=None):
    log('library.collectors main_jobevent')
    return {'fake': 'main_jobevent_data'}


@collector
def main_indirectmanagednodeaudit(db=None, since=None, until=None):
    log('library.collectors main_indirectmanagednodeaudit')
    return {'fake': 'main_indirectmanagednodeaudit_data'}


@collector
def host_metric(db=None, since=None):
    log('library.collectors host_metric')
    return {'fake': 'host_metric_data'}
