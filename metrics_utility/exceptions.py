"""Custom exception hierarchy for metrics-utility."""


class MetricsError(Exception):
    """Base class for all metrics-utility exceptions."""

    def __init__(self, message):
        """Initialise with a descriptive error message.

        Args:
            message: Human-readable description of the error.
        """
        self.name = message


class FailedToUploadPayloadError(MetricsError):
    """Raised when the HTTP upload of a tarball to the ingress API fails."""


class MissingRequiredEnvVarError(MetricsError):
    """Raised when one or more required environment variables are absent or invalid."""


class NoAnalyticsCollectedError(MetricsError):
    """Raised when the collector produces no tarballs."""


class CollectorDisabledError(MetricsError):
    """Raised when a collector is not enabled in the current configuration."""


class UnparsableParameterError(MetricsError):
    """Raised when a CLI parameter value cannot be parsed into the expected type."""
