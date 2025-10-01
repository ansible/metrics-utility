class BaseCollector:
    def __init__(self, **kwargs):
        print(f"library.collectors {self.__class__.__name__}.__init__")

    def gather(self):
        print(f"library.collectors {self.__class__.__name__}.gather")
        return {"fake": "data"}


class AnonymousCollector(BaseCollector):
    pass


class ConfigCollector(BaseCollector):
    pass


class JobHostSummaryCollector(BaseCollector):
    pass


class MainHostCollector(BaseCollector):
    pass


class MainJobEventCollector(BaseCollector):
    pass


class MainIndirectManagedNodeAuditCollector(BaseCollector):
    pass


class HostMetricCollector(BaseCollector):
    pass


def anonymous(db=None, since=None, until=None, custom_params=None):
    print("library.collectors anonymous")
    return AnonymousCollector()


def config(db=None):
    print("library.collectors config")
    return ConfigCollector()


def job_host_summary(db=None, since=None, until=None):
    print("library.collectors job_host_summary")
    return JobHostSummaryCollector()


def main_host(db=None):
    print("library.collectors main_host")
    return MainHostCollector()


def main_jobevent(db=None, since=None, until=None):
    print("library.collectors main_jobevent")
    return MainJobEventCollector()


def main_indirectmanagednodeaudit(db=None, since=None, until=None):
    print("library.collectors main_indirectmanagednodeaudit")
    return MainIndirectManagedNodeAuditCollector()


def host_metric(db=None, since=None):
    print("library.collectors host_metric")
    return HostMetricCollector()