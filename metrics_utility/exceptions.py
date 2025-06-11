class MetricsException(Exception):
    def __init__(self, message):
        self.name = message


class BadParameter(MetricsException):
    pass


class BadRequiredEnvVar(MetricsException):
    pass


class BadShipTarget(MetricsException):
    pass


class DateFormatError(MetricsException):
    pass


class FailedToUploadPayload(MetricsException):
    pass


class MissingRequiredEnvVar(MetricsException):
    pass


class MissingRequiredParameter(MetricsException):
    pass


class NoAnalyticsCollected(MetricsException):
    pass


class NotSupportedFactory(MetricsException):
    pass


class UnparsableParameter(MetricsException):
    pass
