"""TLS trust settings for librdkafka clients.

librdkafka (bundled in the ``confluent-kafka`` wheel) ignores the standard
``SSL_CERT_FILE`` / ``SSL_CERT_DIR`` environment variables and uses its own CA
probing. Behind a TLS-inspecting proxy (e.g. Zscaler), the broker certificate is
re-signed by a corporate CA that only exists in the bundle those variables point
at, so the handshake fails with ``certificate verify failed``.

The Schema Registry client (Python ``ssl``) already honours ``SSL_CERT_FILE``;
this brings the Kafka clients in line. With neither variable set, the config is
unchanged and librdkafka keeps its default trust store.
"""

from __future__ import annotations

import os


def kafka_ssl_config() -> dict[str, str]:
    """``{"ssl.ca.location": ...}`` from SSL_CERT_FILE/SSL_CERT_DIR, else ``{}``."""
    for var in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
        path = os.environ.get(var)
        if path and os.path.exists(path):
            return {"ssl.ca.location": path}
    return {}
