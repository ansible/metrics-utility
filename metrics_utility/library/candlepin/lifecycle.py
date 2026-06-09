"""Candlepin certificate validation helpers used at CRC ship time."""

from datetime import datetime, timezone

from cryptography import x509

from metrics_utility.logger import logger


def parse_cert(pem_text):
    """Parse a PEM certificate and return a metadata dict.

    Raises ``ValueError`` if the PEM cannot be parsed.
    """
    data = pem_text.encode('utf-8') if isinstance(pem_text, str) else pem_text
    try:
        cert = x509.load_pem_x509_certificate(data)
    except Exception as e:
        raise ValueError(f'Could not parse PEM certificate: {e}') from e

    expiry = cert.not_valid_after_utc
    remaining = expiry - datetime.now(timezone.utc)

    subject = {attr.oid._name: attr.value for attr in cert.subject}
    issuer = {attr.oid._name: attr.value for attr in cert.issuer}

    return {
        'serial': str(cert.serial_number),
        'cn': subject.get('commonName', 'unknown'),
        'issuer_cn': issuer.get('commonName', 'unknown'),
        'issuer_org': issuer.get('organizationName', 'unknown'),
        'not_before': cert.not_valid_before_utc.isoformat(),
        'not_after': expiry.isoformat(),
        'days_remaining': remaining.days,
        'validity_days': (expiry - cert.not_valid_before_utc).days,
    }


def is_cert_valid(cert_pem: str) -> bool:
    """Return True if cert_pem is parseable, already valid, and not yet expired.

    Logs a warning when the cert is not yet valid, expired, or unparseable, then
    returns False so the caller can fall back to service-account authentication.
    """
    try:
        info = parse_cert(cert_pem)
        now = datetime.now(timezone.utc)
        not_before = datetime.fromisoformat(info['not_before'])
        if now < not_before:
            logger.warning(f'Candlepin cert is not yet valid (not_before={info["not_before"]}); falling back to service account auth')
            return False
        if info['days_remaining'] < 0:
            logger.warning(f'Candlepin cert expired at {info["not_after"]}; falling back to service account auth')
            return False
        return True
    except ValueError as e:
        logger.warning(f'Could not parse Candlepin cert: {e}')
        return False
