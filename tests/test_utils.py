import datetime
import os
import sys
import socket
import tempfile
import pytest


def generate_self_signed_tls_certs():
    """Create self-signed key/cert pair for testing.

    This method requires the library ``cryptography`` be installed.
    """
    try:
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        raise ImportError(
            "Using `Security.temporary` requires `cryptography`, please "
            "install it using either pip or conda"
        )
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    key_contents = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    ray_interal = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ray-internal")])
    # This is the same logic used by the GCS server to acquire a
    # private/interal IP address to listen on. If we just use localhost +
    # 127.0.0.1 then we won't be able to connect to the GCS and will get
    # an error like "No match found for server name: 192.168.X.Y"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    private_ip_address = s.getsockname()[0]
    s.close()
    altnames = x509.SubjectAlternativeName(
        [
            x509.DNSName(
                socket.gethostbyname(socket.gethostname())
            ),  # Probably 127.0.0.1
            x509.DNSName("127.0.0.1"),
            x509.DNSName(private_ip_address),  # 192.168.*.*
            x509.DNSName("localhost"),
        ]
    )
    now = datetime.datetime.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(ray_interal)
        .issuer_name(ray_interal)
        .add_extension(altnames, critical=False)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(key, hashes.SHA256(), default_backend())
    )

    cert_contents = cert.public_bytes(serialization.Encoding.PEM).decode()

    return cert_contents, key_contents



def load_certs_from_env():
    tls_env_vars = ["RAY_TLS_SERVER_CERT", "RAY_TLS_SERVER_KEY", "RAY_TLS_CA_CERT"]
    if any(v not in os.environ for v in tls_env_vars):
        raise RuntimeError(
            "If the environment variable RAY_USE_TLS is set to true "
            "then RAY_TLS_SERVER_CERT, RAY_TLS_SERVER_KEY and "
            "RAY_TLS_CA_CERT must also be set."
        )

    with open(os.environ["RAY_TLS_SERVER_CERT"], "rb") as f:
        server_cert_chain = f.read()
    with open(os.environ["RAY_TLS_SERVER_KEY"], "rb") as f:
        private_key = f.read()
    with open(os.environ["RAY_TLS_CA_CERT"], "rb") as f:
        ca_cert = f.read()

    return server_cert_chain, private_key, ca_cert

def setup_tls():
    """Sets up required environment variables for tls"""
    import pytest

    # if sys.platform == "darwin":
        # pytest.skip("Cryptography doesn't install in Mac build pipeline")
    cert, key = generate_self_signed_tls_certs()
    temp_dir = tempfile.mkdtemp("ray-test-certs")
    cert_filepath = os.path.join(temp_dir, "server.crt")
    key_filepath = os.path.join(temp_dir, "server.key")
    with open(cert_filepath, "w") as fh:
        fh.write(cert)
    with open(key_filepath, "w") as fh:
        fh.write(key)

    os.environ["RAY_USE_TLS"] = "1"
    os.environ["RAY_TLS_SERVER_CERT"] = cert_filepath
    os.environ["RAY_TLS_SERVER_KEY"] = key_filepath
    os.environ["RAY_TLS_CA_CERT"] = cert_filepath

    return key_filepath, cert_filepath, temp_dir


def teardown_tls(key_filepath, cert_filepath, temp_dir):
    os.remove(key_filepath)
    os.remove(cert_filepath)
    os.removedirs(temp_dir)
    del os.environ["RAY_USE_TLS"]
    del os.environ["RAY_TLS_SERVER_CERT"]
    del os.environ["RAY_TLS_SERVER_KEY"]
    del os.environ["RAY_TLS_CA_CERT"]


@pytest.fixture
def use_tls(request):
    if request.param:
        key_filepath, cert_filepath, temp_dir = setup_tls()
    yield request.param
    if request.param:
        teardown_tls(key_filepath, cert_filepath, temp_dir)


def build_env():
    env = os.environ.copy()
    if sys.platform == "win32" and "SYSTEMROOT" not in env:
        env["SYSTEMROOT"] = r"C:\Windows"
    return env
