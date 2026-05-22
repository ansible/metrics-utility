"""Custom exception hierarchy for metrics-utility."""


class MetricsException(Exception):
    """Base class for all metrics-utility exceptions."""

    def __init__(self, message):
        """Initialise with a descriptive error message.

        Args:
            message: Human-readable description of the error.
        """
        self.name = message


class FailedToUploadPayload(MetricsException):
    """Raised when the HTTP upload of a tarball to the ingress API fails."""

    pass


class MissingRequiredEnvVar(MetricsException):
    """Raised when one or more required environment variables are absent or invalid."""

    pass


class NoAnalyticsCollected(MetricsException):
    """Raised when the collector produces no tarballs."""

    pass


class CollectorDisabled(MetricsException):
    """Raised when a collector is not enabled in the current configuration."""

    pass


class UnparsableParameter(MetricsException):
    """Raised when a CLI parameter value cannot be parsed into the expected type."""

    pass
