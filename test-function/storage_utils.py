import os
import datetime as dt
import logging
from boxsdk.exception import BoxAPIException

# existing imports/utilities
from trace_utils import get_or_create_trace_id
from retry_utils import default_retry

try:
    # Python 3.9+ stdlib
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

logger = logging.getLogger(__name__)


def _now_local():
    tzname = os.getenv("TIMEZONE", "America/Toronto")
    if ZoneInfo:
        return dt.datetime.now(ZoneInfo(tzname))
    # Fallback to naive now if zoneinfo not available
    return dt.datetime.now()


def get_date_subpath():
    """Current date in YYYY/MM/DD — legacy helper (unused for Sprott)."""
    now = _now_local()
    return f"{now.year}/{str(now.month).zfill(2)}/{str(now.day).zfill(2)}"


def get_prev_business_day_str():
    """Return previous business day (Mon–Fri) as YYYYMMDD, using TIMEZONE."""
    today = _now_local().date()
    d = today - dt.timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= dt.timedelta(days=1)
    return f"{d.year}{d.month:02d}{d.day:02d}"


def get_today_business_day_str():
    """
    Return today's date (local TZ) as YYYYMMDD.
    If it's weekend, still returns today's date (no rollback).
    """
    d = _now_local().date()
    if d.weekday() >= 5:
        logger.warning("Today is a weekend; returning today's date anyway: %s", d)
    return f"{d.year}{d.month:02d}{d.day:02d}"


def get_business_day_str(mode: str = "today") -> str:
    """
    mode='today' (default) -> today's YYYYMMDD (no weekend rollback)
    mode='prev'            -> previous business day YYYYMMDD (weekend-safe)
    """
    m = (mode or "today").lower()
    if m == "prev":
        return get_prev_business_day_str()
    return get_today_business_day_str()


# ---------------- Box helpers for originals ---------------- #

def _get_or_create_folder(client, parent_folder, folder_name):
    """Get or create a subfolder with the given name under parent_folder."""
    for item in parent_folder.get_items():
        if item.type == 'folder' and item.name == folder_name:
            return item
    return parent_folder.create_subfolder(folder_name)


@default_retry()
def upload_files_to_box_prev_bday(client, root_folder_id, local_dir, context=None):
    """
    Legacy: Upload files into Box under previous business day folder (YYYYMMDD).
    """
    trace_id = get_or_create_trace_id(context)
    files = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
    prev_key = get_prev_business_day_str()
    logger.info(f"[{trace_id}] Uploading {len(files)} files to Box folder {root_folder_id}/{prev_key}")

    root = client.folder(root_folder_id)
    day_folder = _get_or_create_folder(client, root, prev_key)

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        logger.info(f"[{trace_id}] Uploading {filename} to Box {prev_key}")
        try:
            with open(local_path, 'rb') as file_stream:
                day_folder.upload_stream(file_stream, filename)
                logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully")
        except BoxAPIException as e:
            if e.status == 409 and e.code == 'item_name_in_use':
                logger.info(f"[{trace_id}] {filename} exists, uploading as new version (Box).")
                items = {item.name: item for item in day_folder.get_items()}
                if filename in items:
                    items[filename].update_contents(local_path)
                    logger.info(f"[{trace_id}] Uploaded {filename} as a new version in Box.")
                else:
                    logger.error(f"[{trace_id}] Conflict reported but file not listed in folder: {filename}")
            else:
                logger.error(f"[{trace_id}] Failed to upload {filename} to Box: {e}")


@default_retry()
def upload_files_to_box_for_day(client, root_folder_id, local_dir, day_key: str, context=None):
    """
    Upload files into Box under a folder named with 'day_key' (YYYYMMDD).
    If a file name already exists, upload as a new version.
    """
    trace_id = get_or_create_trace_id(context)
    files = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
    logger.info(f"[{trace_id}] Uploading {len(files)} files to Box folder {root_folder_id}/{day_key}")

    root = client.folder(root_folder_id)
    day_folder = _get_or_create_folder(client, root, day_key)

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        logger.info(f"[{trace_id}] Uploading {filename} to Box {day_key}")
        try:
            with open(local_path, 'rb') as file_stream:
                day_folder.upload_stream(file_stream, filename)
                logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully")
        except BoxAPIException as e:
            if e.status == 409 and e.code == 'item_name_in_use':
                logger.info(f"[{trace_id}] {filename} exists, uploading as new version (Box).")
                items = {item.name: item for item in day_folder.get_items()}
                if filename in items:
                    items[filename].update_contents(local_path)
                    logger.info(f"[{trace_id}] Uploaded {filename} as a new version in Box.")
                else:
                    logger.error(f"[{trace_id}] Conflict reported but file not listed in folder: {filename}")
            else:
                logger.error(f"[{trace_id}] Failed to upload {filename} to Box: {e}")


# ---------------- NEW: Box helpers for SAM/PE output hierarchies ---------------- #

def _ensure_box_path(client, root_folder_id: str, path_parts: list[str]):
    """
    Ensure nested folders exist under root_folder_id following path_parts order.
    Returns the final Box folder object.
    """
    root = client.folder(root_folder_id)
    current = root
    for part in path_parts:
        current = _get_or_create_folder(client, current, part)
    return current


def _upload_with_versioning(target_folder, local_path: str, filename: str, trace_id: str | None = None):
    """
    Uploads a single file to target_folder; if the name already exists, uploads as a new version.
    """
    try:
        with open(local_path, 'rb') as fs:
            target_folder.upload_stream(fs, filename)
            logger.info(f"[{trace_id}] Uploaded {filename} to Box successfully")
    except BoxAPIException as e:
        if e.status == 409 and e.code == 'item_name_in_use':
            logger.info(f"[{trace_id}] {filename} exists, uploading as new version (Box).")
            items = {item.name: item for item in target_folder.get_items()}
            if filename in items:
                items[filename].update_contents(local_path)
                logger.info(f"[{trace_id}] Uploaded {filename} as a new version in Box.")
            else:
                logger.error(f"[{trace_id}] Conflict reported but file not listed in folder: {filename}")
        else:
            logger.error(f"[{trace_id}] Failed to upload {filename} to Box: {e}")
            raise


def _split_day_key(day_key: str):
    """Return (yyyy, mm, dd) strings from YYYYMMDD."""
    yyyy = day_key[:4]
    mm   = day_key[4:6]
    dd   = day_key[6:8]
    return yyyy, mm, dd


def upload_sam_output_to_box(client, root_folder_id: str, local_path: str, day_key: str, context=None):
    """
    SAM path format: yyyy/MM/yyyyMMdd
    """
    trace_id = get_or_create_trace_id(context)
    yyyy, mm, _ = _split_day_key(day_key)
    path_parts = [yyyy, mm, day_key]
    filename = os.path.basename(local_path)

    logger.info(f"[{trace_id}] SAM -> Box path {root_folder_id}/{'/'.join(path_parts)} (file={filename})")
    target_folder = _ensure_box_path(client, root_folder_id, path_parts)
    _upload_with_versioning(target_folder, local_path, filename, trace_id)


def upload_pe_output_to_box(client, root_folder_id: str, local_path: str, day_key: str, context=None):
    """
    PE path format: yyyy/yyyyMMdd
    """
    trace_id = get_or_create_trace_id(context)
    yyyy, _, _ = _split_day_key(day_key)
    path_parts = [yyyy, day_key]
    filename = os.path.basename(local_path)

    logger.info(f"[{trace_id}] PE  -> Box path {root_folder_id}/{'/'.join(path_parts)} (file={filename})")
    target_folder = _ensure_box_path(client, root_folder_id, path_parts)
    _upload_with_versioning(target_folder, local_path, filename, trace_id)
