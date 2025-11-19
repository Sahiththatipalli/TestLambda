# gcp_utils.py
import os, json, time, logging, tempfile, hmac, hashlib, base64
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from email.utils import formatdate

try:
    from retry_utils import default_retry
except Exception:
    def default_retry(*_a, **_k):
        def deco(f): return f
        return deco

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------- secret / client helpers ----------

def _read_hmac_secret(secret_name: str):
    sm = boto3.client("secretsmanager")
    c = json.loads(sm.get_secret_value(SecretId=secret_name)["SecretString"])
    access_key = c.get("access_key") or c.get("AccessKey") or c.get("accessKey")
    secret_key = c.get("secret_key") or c.get("Secret") or c.get("secret") or c.get("secretKey")
    if not access_key or not secret_key:
        raise ValueError("GCS HMAC secret is missing required keys (access/secret).")
    return access_key, secret_key


def _build_gcs_client(access_key: str, secret_key: str, region: str, addressing: str, unsigned: bool):
    cfg = BotoConfig(
        signature_version="s3v4",
        s3={
            "addressing_style": addressing,           # "virtual" or "path"
            "payload_signing_enabled": not unsigned,  # unsigned=False => sign body
        },
    )
    session = boto3.session.Session()
    cli = session.client(
        "s3",
        endpoint_url="https://storage.googleapis.com",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=cfg,
    )
    logger.info(f"GCS S3 client initialized (region_name={region}, addressing={addressing}, unsigned-payload={unsigned})")
    return cli


def make_gcs_client_from_secret(secret_name: str):
    """Client for HEAD/GET/LIST (reads). Uploads use the resilient path."""
    ak, sk = _read_hmac_secret(secret_name)
    region = os.getenv("GCS_S3_REGION", "us-east-1")
    return _build_gcs_client(ak, sk, region=region, addressing="path", unsigned=True)


# ---------- v2 PUT signer (for GCS S3-compat) ----------

def _aws_v2_signature(secret_key: str, verb: str, content_md5: str, content_type: str, date_str: str,
                      canonical_amz_headers: str, canonical_resource: str) -> str:
    msg = "\n".join([verb, content_md5, content_type, date_str, canonical_amz_headers + canonical_resource])
    digest = hmac.new(secret_key.encode("utf-8"), msg.encode("utf-8"), hashlib.sha1).digest()
    return base64.b64encode(digest).decode("utf-8")


def _gcs_put_object_v2(access_key: str, secret_key: str, bucket: str, key: str, file_path: str,
                       content_type: str | None = None, endpoint: str = "https://storage.googleapis.com"):
    """
    Minimal AWS Signature v2 PUT to GCS XML API using urllib.
    """
    if content_type is None:
        content_type = "application/octet-stream"

    with open(file_path, "rb") as f:
        body = f.read()

    content_md5 = base64.b64encode(hashlib.md5(body).digest()).decode("utf-8")
    date_str = formatdate(usegmt=True)
    canonical_amz_headers = ""  # no x-amz- headers
    canonical_resource = f"/{bucket}/{key}"

    signature = _aws_v2_signature(secret_key, "PUT", content_md5, content_type, date_str,
                                  canonical_amz_headers, canonical_resource)
    auth = f"AWS {access_key}:{signature}"

    url = f"{endpoint}/{bucket}/{key}"
    req = Request(url=url, data=body, method="PUT")
    req.add_header("Content-MD5", content_md5)
    req.add_header("Content-Type", content_type)
    req.add_header("Date", date_str)
    req.add_header("Authorization", auth)

    try:
        with urlopen(req, timeout=300) as resp:
            resp.read()
    except HTTPError as e:
        raise RuntimeError(f"GCS v2 PUT failed ({e.code}): {e.read().decode('utf-8','ignore')}")
    except URLError as e:
        raise RuntimeError(f"GCS v2 PUT URL error: {e.reason}")


# ---------- copy ops ----------

@default_retry()
def s3_to_gcs(s3_bucket: str, s3_key: str, gcs_bucket: str, gcs_key: str,
              gcs_client, s3_client=None, gcs_secret_name: str | None = None) -> int:
    """
    Copy an object from AWS S3 to GCS (via /tmp). Returns bytes copied.
    Tries SigV4 (boto) first; on SignatureDoesNotMatch falls back to SigV2 PUT.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")
    tmp = os.path.join(tempfile.gettempdir(), os.path.basename(gcs_key) or "tmp_upload")
    s3_client.download_file(s3_bucket, s3_key, tmp)
    size = os.path.getsize(tmp)

    try:
        # SigV4 path
        with open(tmp, "rb") as f:
            gcs_client.put_object(Bucket=gcs_bucket, Key=gcs_key, Body=f)
        logger.info(f"S3 -> GCS (v4): s3://{s3_bucket}/{s3_key} -> gs://{gcs_bucket}/{gcs_key} ({size} bytes)")
        return size
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code != "SignatureDoesNotMatch":
            raise
        logger.warning("GCS SigV4 PUT failed with SignatureDoesNotMatch — falling back to SigV2 PUT.")
    except Exception as e:
        # Some environments raise generic errors; still try v2.
        if "SignatureDoesNotMatch" not in str(e):
            raise
        logger.warning("GCS SigV4 PUT failed — falling back to SigV2 PUT.")

    # Fallback SigV2
    if not gcs_secret_name:
        # Try to find the secret name from env if not passed
        gcs_secret_name = os.environ.get("GCS_HMAC_SECRET_NAME")
    access_key, secret_key = _get_gcs_hmac_from_secret(gcs_secret_name)
    _put_object_v2(access_key, secret_key, gcs_bucket, gcs_key, tmp, content_type="application/octet-stream")
    logger.info(f"S3 -> GCS (v2): s3://{s3_bucket}/{s3_key} -> gs://{gcs_bucket}/{gcs_key} ({size} bytes)")
    return size

@default_retry()
def s3_to_gcs_resilient(secret_name: str, s3_bucket: str, s3_key: str,
                        gcs_bucket: str, gcs_key: str, s3_client=None) -> int:
    """
    Copy S3 -> GCS via /tmp. Try SigV4 first; on SignatureDoesNotMatch, fall back to v2.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    tmp = os.path.join(tempfile.gettempdir(), os.path.basename(gcs_key) or "tmp_upload")
    s3_client.download_file(s3_bucket, s3_key, tmp)
    size = os.path.getsize(tmp)

    ak, sk = _read_hmac_secret(secret_name)

    # Try SigV4 using boto3
    try:
        cli = _build_gcs_client(ak, sk, os.getenv("GCS_S3_REGION", "us-east-1"), addressing="path", unsigned=True)
        with open(tmp, "rb") as f:
            cli.put_object(Bucket=gcs_bucket, Key=gcs_key, Body=f)
        logger.info(f"S3 -> GCS (v4): s3://{s3_bucket}/{s3_key} -> gs://{gcs_bucket}/{gcs_key} ({size} bytes)")
        return size
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code == "SignatureDoesNotMatch":
            logger.warning("GCS SigV4 PUT failed with SignatureDoesNotMatch — falling back to SigV2 PUT.")
        else:
            logger.warning(f"GCS SigV4 PUT failed ({code}); falling back to SigV2.")
    except Exception as e:
        logger.warning(f"GCS SigV4 PUT unexpected error: {e}; falling back to SigV2.")

    # Fall back to v2
    _gcs_put_object_v2(ak, sk, gcs_bucket, gcs_key, tmp)
    logger.info(f"S3 -> GCS (v2): s3://{s3_bucket}/{s3_key} -> gs://{gcs_bucket}/{gcs_key} ({size} bytes)")
    return size


def gcs_to_s3(gcs_bucket: str, gcs_key: str, s3_bucket: str, s3_key: str,
              gcs_client, s3_client=None) -> int:
    if s3_client is None:
        s3_client = boto3.client("s3")
    tmp = os.path.join(tempfile.gettempdir(), os.path.basename(gcs_key) or "tmp_download")
    gcs_client.download_file(gcs_bucket, gcs_key, tmp)
    s3_client.upload_file(tmp, s3_bucket, s3_key)
    size = os.path.getsize(tmp)
    logger.info(f"GCS -> S3: gs://{gcs_bucket}/{gcs_key} -> s3://{s3_bucket}/{s3_key} ({size} bytes)")
    return size


def gcs_object_exists(gcs_client, bucket: str, key: str) -> bool:
    try:
        gcs_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def wait_for_gcs_objects(gcs_client, bucket: str, keys, timeout_sec: int = 900, poll_interval_sec: int = 10):
    keys = list(keys)
    found = {k: False for k in keys}
    deadline = time.time() + timeout_sec
    while time.time() < deadline and not all(found.values()):
        for k in keys:
            if not found[k] and gcs_object_exists(gcs_client, bucket, k):
                found[k] = True
                logger.info(f"GCS object present: gs://{bucket}/{k}")
        if not all(found.values()):
            time.sleep(poll_interval_sec)
    for k, ok in found.items():
        if not ok:
            logger.warning(f"Timed out waiting for gs://{bucket}/{k}")
    return found


def gcs_put_smoke_test(secret_name: str, bucket: str, key_prefix: str = "_lambda_smoke", content: bytes = b"ok"):
    """Write a tiny object using the resilient uploader path."""
    tmp = os.path.join(tempfile.gettempdir(), "gcs_smoke.txt")
    with open(tmp, "wb") as f:
        f.write(content)

    ak, sk = _read_hmac_secret(secret_name)
    # attempt v4 first
    try:
        cli = _build_gcs_client(ak, sk, os.getenv("GCS_S3_REGION", "us-east-1"), addressing="path", unsigned=True)
        key = f"{key_prefix.rstrip('/')}/{int(time.time())}.txt"
        with open(tmp, "rb") as f:
            cli.put_object(Bucket=bucket, Key=key, Body=f)
        cli.head_object(Bucket=bucket, Key=key)
        logger.info(f"GCS smoke (v4) OK: gs://{bucket}/{key}")
        return {"status": "ok", "gcs": f"gs://{bucket}/{key}"}
    except Exception:
        logger.warning("GCS smoke: SigV4 failed — using SigV2 fallback.")

    # fallback v2
    key = f"{key_prefix.rstrip('/')}/{int(time.time())}.txt"
    _gcs_put_object_v2(ak, sk, bucket, key, tmp)
    logger.info(f"GCS smoke (v2) OK: gs://{bucket}/{key}")
    return {"status": "ok", "gcs": f"gs://{bucket}/{key}"}

# ---------------- Cloud Functions (optional HTTP) ----------------
def call_http_cloud_function(url: str, payload: dict | None = None,
                             bearer_token: str | None = None, timeout: int = 600):
    """
    Invoke a Google Cloud Function via HTTPS URL. Returns response text.
    """
    if not url:
        return None
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    data = json.dumps(payload or {}).encode("utf-8")
    req = Request(url=url, data=data, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", "ignore")
            logger.info(f"Called GCF @ {url} ({len(text)} bytes)")
            return text
    except HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        logger.error(f"GCF HTTPError {e.code}: {body}")
        raise
    except URLError as e:
        logger.error(f"GCF URLError: {e.reason}")
        raise
