"""Shared credential and URL configuration mixin for Package subclasses."""

import os


class CredentialConfig:
    """Mixin that provides shared credential and endpoint getter methods.

    Classes that ship data to Red Hat's CRC ingress API should inherit from
    this mixin to avoid copy-pasting the same ``get_ingress_url()``,
    ``_get_rh_user()``, and ``_get_rh_password()`` implementations across
    multiple package classes.

    All values are read from environment variables so that the same code path
    works in both production deployments and local development overrides.
    """

    def get_ingress_url(self):
        """Return the CRC ingress upload URL.

        Reads ``METRICS_UTILITY_CRC_INGRESS_URL`` from the environment,
        falling back to the canonical Red Hat console endpoint.
        """
        return os.getenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')

    def _get_rh_user(self):
        """Return the Red Hat service-account client ID.

        Reads ``METRICS_UTILITY_SERVICE_ACCOUNT_ID`` from the environment.
        """
        return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID')

    def _get_rh_password(self):
        """Return the Red Hat service-account client secret.

        Reads ``METRICS_UTILITY_SERVICE_ACCOUNT_SECRET`` from the environment.
        """
        return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET')
