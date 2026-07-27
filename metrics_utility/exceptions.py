"""Custom exception hierarchy for metrics-utility."""


class MetricsException(Exception):
    """Base class for all metrics-utility exceptions."""

    def __init__(self, message):
        """Initialise with a descriptive error message.

        Args:
            message: Human-readable description of the error.
        """
        self.name = message


class BadParameter(MetricsException):
    """Raised when a CLI parameter value is invalid for the current context."""


class BadRequiredEnvVar(MetricsException):
    """Raised when a required environment variable has an unacceptable value."""


class BadShipTarget(MetricsException):
    """Raised when METRICS_UTILITY_SHIP_TARGET has an unrecognised value."""


class DateFormatError(MetricsException):
    """Raised when a date argument cannot be parsed into the expected format."""


class FailedToUploadPayload(MetricsException):
    """Raised when the HTTP upload of a tarball to the ingress API fails."""


class MissingRequiredEnvVar(MetricsException):
    """Raised when one or more required environment variables are absent or invalid."""


class MissingRequiredParameter(MetricsException):
    """Raised when a required CLI parameter is not provided."""


class NoAnalyticsCollected(MetricsException):
    """Raised when the collector produces no tarballs."""


class NotSupportedFactory(MetricsException):
    """Raised when a factory receives an unrecognised type or target value."""


class UnparsableParameter(MetricsException):
    """Raised when a CLI parameter value cannot be parsed into the expected type."""
