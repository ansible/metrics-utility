"""Synthetic canary values and leak-detection helpers for privacy tests.

Every constant is an unmistakable, realistic-format value.  If any of them
reaches an outbound payload, log line, or archive it is a privacy regression.
"""

import base64
import hashlib

from collections.abc import Iterable
from typing import Any
from urllib.parse import quote


CANARY_HOST_NAME = 'canary-host-9f3a.acme.internal'
CANARY_HOST_IP = '198.51.100.77'
CANARY_ORG_NAME = 'CANARY_ORG_AcmeCorp'
CANARY_INVENTORY_NAME = 'CANARY_INV_ProdCluster'
CANARY_PROJECT_NAME = 'CANARY_PROJ_Payroll'
CANARY_JOB_TEMPLATE_NAME = 'CANARY_TMPL_DeployPayroll'
CANARY_PLAYBOOK_NAME = 'canary_payroll_deploy.yml'
CANARY_CUSTOM_COLLECTION = 'canary_acme.private_erp'
CANARY_CUSTOM_MODULE = 'canary_acme.private_erp.sync_hr'
CANARY_CUSTOM_ROLE = 'canary_acme.private_erp.hr_role'
CANARY_CUSTOM_CRED_TYPE = 'CANARY_CRED_AcmeVault'
CANARY_EE_NAME = 'CANARY_EE_AcmeRuntime'
CANARY_EE_IMAGE = 'registry.acme.internal/canary-ee:1.0'
CANARY_USERNAME = 'canary_user_jdoe_7b2c'
CANARY_SERIAL = 'CANARY-SN-884422'
CANARY_MACHINE_ID = 'canary-mid-001122334455'
CANARY_ACCOUNT_NUMBER = 'CANARY_ACCT_90125'
CANARY_INSTALL_UUID = '11111111-2222-4333-8444-555555555555'
CANARY_S3_ACCESS_KEY = 'CANARY_S3_ACCESS_k8M4pQ2z'
CANARY_S3_SECRET_KEY = 'CANARY_S3_SECRET_x9K2mP7qR5vN'
CANARY_CANDLEPIN_CERT_PEM = '-----BEGIN CERTIFICATE-----\nCANARY_CANDLEPIN_CERT_BODY\n-----END CERTIFICATE-----'
CANARY_CANDLEPIN_KEY_PEM = '-----BEGIN PRIVATE KEY-----\nCANARY_CANDLEPIN_KEY_BODY\n-----END PRIVATE KEY-----'

# Every canary that must never appear in a Segment telemetry payload.
SEGMENT_FORBIDDEN_CANARIES = (
    CANARY_HOST_NAME,
    CANARY_HOST_IP,
    CANARY_ORG_NAME,
    CANARY_INVENTORY_NAME,
    CANARY_PROJECT_NAME,
    CANARY_JOB_TEMPLATE_NAME,
    CANARY_PLAYBOOK_NAME,
    CANARY_CUSTOM_COLLECTION,
    CANARY_CUSTOM_MODULE,
    CANARY_CUSTOM_ROLE,
    CANARY_CUSTOM_CRED_TYPE,
    CANARY_EE_NAME,
    CANARY_EE_IMAGE,
    CANARY_USERNAME,
    CANARY_SERIAL,
    CANARY_MACHINE_ID,
    CANARY_ACCOUNT_NUMBER,
    CANARY_INSTALL_UUID,
    CANARY_S3_ACCESS_KEY,
    CANARY_S3_SECRET_KEY,
    CANARY_CANDLEPIN_CERT_PEM,
    CANARY_CANDLEPIN_KEY_PEM,
)


def canary_variants(token: str) -> dict[str, str]:
    """Return the encodings a leaked canary could plausibly take.

    Args:
        token: Raw canary string.

    Returns:
        Mapping of variant name to encoded value.
    """
    encoded = token.encode('utf-8')
    return {
        'raw': token,
        'lower': token.lower(),
        'url_encoded': quote(token, safe=''),
        'base64': base64.b64encode(encoded).decode('ascii'),
        'sha256': hashlib.sha256(encoded).hexdigest(),
    }


def find_leaked_canaries(blob: str, tokens: Iterable[str]) -> list[tuple[str, str]]:
    """Find any representation of the supplied canary tokens in *blob*.

    Args:
        blob: Text to search (JSON dump, log output, HTTP body, ...).
        tokens: Raw canary values to look for.

    Returns:
        List of ``(token, variant_name)`` pairs for every match found.
    """
    leaks: list[tuple[str, str]] = []
    for token in tokens:
        for variant_name, variant in canary_variants(token).items():
            if variant and variant in blob:
                leaks.append((token, variant_name))
    return leaks


def find_forbidden_keys(obj: Any, forbidden_keys: set[str], path: str = '') -> list[str]:
    """Recursively find forbidden key names in nested dicts and lists.

    Args:
        obj: Value to inspect.
        forbidden_keys: Key names that must not appear anywhere in the tree.
        path: Dotted path prefix used while recursing.

    Returns:
        Dotted paths of all forbidden keys found.
    """
    found: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_path = f'{path}.{key}' if path else str(key)
            if key in forbidden_keys:
                found.append(key_path)
            found.extend(find_forbidden_keys(value, forbidden_keys, key_path))
    elif isinstance(obj, (list, tuple)):
        for index, value in enumerate(obj):
            found.extend(find_forbidden_keys(value, forbidden_keys, f'{path}[{index}]'))
    return found
