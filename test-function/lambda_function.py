import os
import json
import boto3
import paramiko
import tempfile
import shutil
import logging

from logging_utils import (
    log_job_start, log_job_end, log_sftp_connection, log_matched_files,
    log_checksum_ok, log_checksum_fail, log_file_transferred, log_archive,
    log_tmp_usage, log_warning, log_error
)
from dry_run_utils import is_dry_run_enabled, log_dry_run_action
from checksum_utils import log_checksum
from trace_utils import get_or_create_trace_id
from file_match_utils import match_files
from retry_utils import default_retry
from storage_utils import get_date_subpath
from performance_utils import time_operation
from metrics_utils import publish_file_transfer_metric, publish_error_metric
from alert_utils import send_file_transfer_sns_alert

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


def _get_secret_dict(secret_name: str) -> dict:
    """
    Fetch connection details from Secrets Manager.
    Secret is expected to contain JSON with at least:
      {
        "Host": "sftp.ninepoint.com",
        "Username": "appservice",
        "Password": "********"
      }
    """
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = resp.get("SecretString") or ""
    try:
        return json.loads(secret_str)
    except json.JSONDecodeError:
        # fall back to single-value secrets if needed
        return {"value": secret_str}


def _get_file_patterns():
    """
    Read FILE_PATTERN env var and turn into a list.

    Examples:
       FILE_PATTERN="CRMEXTHLD.*"
       FILE_PATTERN="CRMEXTHLD.*,CRMEXTNAV.*"
    """
    raw = os.getenv("FILE_PATTERN")
    if not raw:
        return ["*"]
    return [p.strip() for p in raw.split(",") if p.strip()]


@default_retry()
def _create_sftp_client(host: str, port: int, username: str, password: str) -> paramiko.SFTPClient:
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)


@default_retry()
def _process_files_on_sftp(
    sftp_client: paramiko.SFTPClient,
    src_dir: str,
    dest_dir: str,
    bucket: str,
    prefix: str,
    tmp_dir: str,
    trace_id: str,
    file_patterns,
    metrics: dict,
    transfer_status: dict,
    checksum_status: dict,
    errors: list,
    warnings: list,
):
    """
    Main worker:

    1. List files in src_dir on the SFTP server.
    2. Filter using FILE_PATTERN.
    3. For each file:
       - download from src_dir to /tmp
       - upload to dest_dir on the SAME SFTP server
       - upload to S3 under S3_BUCKET/S3_PREFIX/YYYY/MM/DD/
       - optionally delete from src_dir once both uploads succeed.
    """
    src_dir = src_dir.rstrip("/") or "."
    dest_dir = dest_dir.rstrip("/") or "/"

    all_files = sftp_client.listdir(src_dir)
    files = match_files(all_files, include_patterns=file_patterns)
    unmatched = sorted(set(all_files) - set(files))
    log_matched_files(trace_id, files, unmatched)

    if not files:
        msg = f"No files in {src_dir} matched patterns {file_patterns}; nothing to do."
        log_warning(trace_id, msg)
        transfer_status["sftp_move"] = "NO FILES"
        transfer_status["s3"] = "NO FILES"
        warnings.append(msg)
        return

    date_subpath = get_date_subpath()  # "YYYY/MM/DD"

    total_bytes_download = 0
    total_bytes_upload_sftp = 0
    total_bytes_upload_s3 = 0

    total_time_download = 0.0
    total_time_upload_sftp = 0.0
    total_time_upload_s3 = 0.0

    delete_src = str(os.getenv("DELETE_SOURCE", "true")).lower() == "true"

    for filename in files:
        src_path = f"{src_dir}/{filename}"
        dest_path = f"{dest_dir}/{filename}"
        local_path = os.path.join(tmp_dir, filename)

        if is_dry_run_enabled():
            log_dry_run_action(
                f"Would download {src_path} to {local_path}, "
                f"then upload to {dest_path} and S3 bucket={bucket} prefix={prefix}/{date_subpath}"
            )
            continue

        # --- Download from source folder ---
        try:
            logger.info("[%s] Downloading %s from %s to %s",
                        trace_id, filename, src_path, local_path)
            _, dl_time = time_operation(sftp_client.get, src_path, local_path)
        except Exception as e:
            msg = f"Failed to download {src_path}"
            log_error(trace_id, msg, exc=e)
            errors.append(f"{msg}: {e}")
            continue

        size_bytes = os.path.getsize(local_path)
        total_bytes_download += size_bytes
        total_time_download += dl_time

        # checksums around S3 upload (SFTP source and S3 will share the same file)
        downloaded_checksum = log_checksum(
            local_path, trace_id, algo="sha256",
            note="after download from /lti/prod"
        )
        s3_upload_checksum = log_checksum(
            local_path, trace_id, algo="sha256",
            note="before S3 upload"
        )

        if downloaded_checksum == s3_upload_checksum:
            log_checksum_ok(trace_id, filename, downloaded_checksum)
            checksum_status[filename] = f"OK (sha256: {downloaded_checksum})"
        else:
            log_checksum_fail(trace_id, filename,
                              downloaded_checksum, s3_upload_checksum)
            checksum_status[filename] = (
                f"FAIL (downloaded: {downloaded_checksum}, "
                f"s3: {s3_upload_checksum})"
            )

        # --- Upload to destination folder on same SFTP server ---
        try:
            logger.info("[%s] Uploading %s to dest %s",
                        trace_id, filename, dest_path)
            _, up_time_sftp = time_operation(sftp_client.put, local_path, dest_path)
            total_bytes_upload_sftp += size_bytes
            total_time_upload_sftp += up_time_sftp

            mb = size_bytes / 1024 / 1024 if size_bytes else 0.0
            mbps = (mb / up_time_sftp) if up_time_sftp else None
            log_file_transferred(trace_id, filename, "SFTP_MOVE", up_time_sftp, mbps)
        except Exception as e:
            msg = f"Failed to upload {local_path} to dest {dest_path}"
            log_error(trace_id, msg, exc=e)
            errors.append(f"{msg}: {e}")
            # don't delete source if move failed for this file
            continue

        # --- Upload to S3 archive ---
        s3_key = f"{prefix}/{date_subpath}/{filename}" if prefix else f"{date_subpath}/{filename}"
        try:
            logger.info("[%s] Uploading %s to s3://%s/%s",
                        trace_id, filename, bucket, s3_key)
            _, up_time_s3 = time_operation(s3_client.upload_file, local_path, bucket, s3_key)
            total_bytes_upload_s3 += size_bytes
            total_time_upload_s3 += up_time_s3

            mb = size_bytes / 1024 / 1024 if size_bytes else 0.0
            mbps = (mb / up_time_s3) if up_time_s3 else None
            log_file_transferred(trace_id, filename, "S3", up_time_s3, mbps)
            log_archive(trace_id, filename, s3_key)
        except Exception as e:
            msg = f"Failed to upload {local_path} to S3 at {s3_key}"
            log_error(trace_id, msg, exc=e)
            errors.append(f"{msg}: {e}")
            # We still keep the file on dest_dir; treat S3 as independent archive.
            # Do NOT delete source in this case.
            continue

        # --- Delete from source directory after successful move + S3 (if configured) ---
        if delete_src:
            try:
                logger.info("[%s] Deleting source file %s", trace_id, src_path)
                sftp_client.remove(src_path)
            except Exception as e:
                msg = f"Failed to delete source file {src_path}"
                log_error(trace_id, msg, exc=e)
                warnings.append(f"{msg}: {e}")

    # Aggregate metrics
    def _calc_speed(total_bytes, total_time):
        if not total_bytes or not total_time:
            return 0.0, 0.0
        mb = total_bytes / 1024 / 1024
        mbps = mb / total_time if total_time else 0.0
        return mb, mbps

    mb_dl, mbps_dl = _calc_speed(total_bytes_download, total_time_download)
    mb_up_sftp, mbps_up_sftp = _calc_speed(total_bytes_upload_sftp, total_time_upload_sftp)
    mb_up_s3, mbps_up_s3 = _calc_speed(total_bytes_upload_s3, total_time_upload_s3)

    metrics["SFTP download total mb"] = f"{mb_dl:.2f}"
    metrics["SFTP download speed mb/s"] = f"{mbps_dl:.2f}"
    metrics["SFTP move total mb"] = f"{mb_up_sftp:.2f}"
    metrics["SFTP move speed mb/s"] = f"{mbps_up_sftp:.2f}"
    metrics["S3 upload total mb"] = f"{mb_up_s3:.2f}"
    metrics["S3 upload speed mb/s"] = f"{mbps_up_s3:.2f}"

    if not is_dry_run_enabled():
        # CloudWatch metrics for both legs
        try:
            if total_bytes_download:
                publish_file_transfer_metric(
                    namespace="LambdaFileTransfer",
                    direction="SFTP_TO_TMP",
                    file_count=len(files),
                    total_bytes=total_bytes_download,
                    duration_sec=round(total_time_download, 2),
                    trace_id=trace_id,
                )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for SFTP download", exc=e)
            publish_error_metric("LambdaFileTransfer", "SFTPDownloadMetricError", trace_id)

        try:
            if total_bytes_upload_sftp:
                publish_file_transfer_metric(
                    namespace="LambdaFileTransfer",
                    direction="TMP_TO_SFTP",
                    file_count=len(files),
                    total_bytes=total_bytes_upload_sftp,
                    duration_sec=round(total_time_upload_sftp, 2),
                    trace_id=trace_id,
                )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for SFTP move", exc=e)
            publish_error_metric("LambdaFileTransfer", "SFTPMoveMetricError", trace_id)

        try:
            if total_bytes_upload_s3:
                publish_file_transfer_metric(
                    namespace="LambdaFileTransfer",
                    direction="TMP_TO_S3",
                    file_count=len(files),
                    total_bytes=total_bytes_upload_s3,
                    duration_sec=round(total_time_upload_s3, 2),
                    trace_id=trace_id,
                )
        except Exception as e:
            log_error(trace_id, "CloudWatch metric error for S3 upload", exc=e)
            publish_error_metric("LambdaFileTransfer", "S3UploadMetricError", trace_id)

    if is_dry_run_enabled():
        transfer_status["sftp_move"] = f"DRY_RUN ({', '.join(files)})"
        transfer_status["s3"] = f"DRY_RUN ({', '.join(files)})"
    else:
        transfer_status["sftp_move"] = f"SUCCESS ({', '.join(files)})"
        transfer_status["s3"] = f"SUCCESS ({', '.join(files)})"


def lambda_handler(event, context):
    """
    Lambda entry point for Salesforce LTI -> Unitrax move.

    Behaviour:
      - Connects once to sftp.ninepoint.com (or configured host).
      - Reads all files in SRC_REMOTE_DIR that match FILE_PATTERN.
      - Downloads them into /tmp.
      - Uploads the same files into DEST_REMOTE_DIR on the SAME SFTP server.
      - Uploads the files into S3 under S3_BUCKET/S3_PREFIX/YYYY/MM/DD/.
      - Optionally deletes the files from SRC_REMOTE_DIR (DELETE_SOURCE=true|false).
      - Sends an SNS summary using send_file_transfer_sns_alert.
    """
    trace_id = get_or_create_trace_id(context)
    job_id = getattr(context, "aws_request_id", "N/A")
    file_patterns = _get_file_patterns()

    log_job_start(trace_id, job_id, file_patterns)

    # Environment configuration
    secret_name = os.getenv("SFTP_SECRET_NAME")
    src_remote_dir = os.getenv("SRC_REMOTE_DIR", "/lti/prod")
    dest_remote_dir = os.getenv("DEST_REMOTE_DIR", "/npunitrax")
    s3_bucket = os.getenv("S3_BUCKET")
    s3_prefix = os.getenv("S3_PREFIX", "salesforce")
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    if not secret_name:
        raise RuntimeError("SFTP_SECRET_NAME environment variable is required")
    if not s3_bucket:
        raise RuntimeError("S3_BUCKET environment variable is required")

    # Fetch SFTP credentials
    secret = _get_secret_dict(secret_name)
    host = secret.get("Host") or secret.get("host")
    username = secret.get("Username") or secret.get("username")
    password = secret.get("Password") or secret.get("password")

    if not all([host, username, password]):
        raise RuntimeError("Secret for SFTP must contain Host, Username and Password fields.")

    metrics: dict = {}
    transfer_status: dict = {}
    checksum_status: dict = {}
    errors: list = []
    warnings: list = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

        # Connect to SFTP server once
        sftp_client = _create_sftp_client(host, 22, username, password)
        log_sftp_connection(trace_id, host, "OPENED")

        try:
            _process_files_on_sftp(
                sftp_client,
                src_remote_dir,
                dest_remote_dir,
                s3_bucket,
                s3_prefix,
                tmp_dir,
                trace_id,
                file_patterns,
                metrics,
                transfer_status,
                checksum_status,
                errors,
                warnings,
            )
        finally:
            sftp_client.close()
            log_sftp_connection(trace_id, host, "CLOSED")

        free_mb = shutil.disk_usage(tmp_dir).free // (1024 * 1024)
        log_tmp_usage(trace_id, len(os.listdir(tmp_dir)), free_mb)

    # Send SNS summary (if configured)
    if sns_topic_arn:
        try:
            send_file_transfer_sns_alert(
                sns_topic_arn,
                trace_id,
                transfer_status=transfer_status,
                checksum_status=checksum_status,
                errors=errors,
                warnings=warnings,
                function_name=(context.function_name if context else "lambda_handler"),
            )
        except Exception as e:
            log_error(trace_id, "Failed to send SNS alert", exc=e)
    else:
        log_warning(trace_id, "SNS_TOPIC_ARN is not set; skipping SNS alert.")

    log_job_end(trace_id, job_id)

    body = {
        "message": "Files processed from src to dest SFTP and S3.",
        "trace_id": trace_id,
        "transfer_status": transfer_status,
        "checksum_status": checksum_status,
        "errors": errors,
        "warnings": warnings,
        "metrics": metrics,
    }

    return {
        "statusCode": 200,
        "body": json.dumps(body),
    }
