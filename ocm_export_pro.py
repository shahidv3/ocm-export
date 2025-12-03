#!/usr/bin/env python3
"""
ocm_export_pro.py

Production-style OCM export:

- Paginates through assets from an OCM repository
- Builds local folder tree from OCM folder metadata
- Downloads binaries in parallel (thread pool)
- Streams large files (1MB chunks)
- Checkpointed via state.json and file existence
- Per-asset metadata written to assets.jsonl

Output structure:

ocm_export/
  files/
    <folder-path>/
      <assetId>_<sanitized_name>.<ext>
  meta/
    assets.jsonl       # one JSON per line (file assets)
    folders.json       # raw OCM folder metadata
    rbac.json          # (optional, RBAC export)
    state.json         # checkpoint
"""

import os
import json
import time
import logging
import threading
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import yaml

with open("config.yaml") as f:
    CONFIG = yaml.safe_load(f)

OCM_BASE_URL = CONFIG["ocm"]["base_url"]
OCM_TOKEN = CONFIG["ocm"]["token"]
REPOSITORY_ID = CONFIG["ocm"]["repository_id"]
PAGE_LIMIT = CONFIG["ocm"]["page_limit"]
MAX_RETRIES = CONFIG["ocm"]["max_retries"]
CHUNK_SIZE = CONFIG["ocm"]["chunk_size_mb"] * 1024 * 1024
MAX_WORKERS = CONFIG["ocm"]["max_workers"]

EXPORT_ROOT = CONFIG["output"]["root_dir"]
FILES_DIR = CONFIG["output"]["files_dir"]
META_DIR = CONFIG["output"]["meta_dir"]


STATE_FILE = os.path.join(META_DIR, "state.json")
ASSETS_JSONL = os.path.join(META_DIR, "assets.jsonl")
FOLDERS_JSON = os.path.join(META_DIR, "folders.json")
RBAC_JSON = os.path.join(META_DIR, "rbac.json")

LOG_LEVEL = logging.INFO

# ---------------------------------------- #

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {OCM_TOKEN}"})

# thread-safe metadata append
meta_lock = threading.Lock()
folder_tree_lock = threading.Lock()


def ensure_dirs():
    os.makedirs(FILES_DIR, exist_ok=True)
    os.makedirs(META_DIR, exist_ok=True)


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"last_offset": 0}
    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def get_json(path, params=None, max_retries=MAX_RETRIES):
    url = urljoin(OCM_BASE_URL, path)
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                return resp.json()
            log.warning("GET %s failed (%s): %s", url, resp.status_code, resp.text)
        except Exception as e:
            log.warning("GET %s exception on attempt %d: %s", url, attempt, e)
        time.sleep(2 ** attempt)
    raise RuntimeError(f"GET {url} failed after {max_retries} attempts")


def sanitize_filename(name: str) -> str:
    bad_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for c in bad_chars:
        name = name.replace(c, "_")
    return name.strip() or "unnamed"


def guess_ext(mime_type: str) -> str:
    if not mime_type:
        return ""
    if "/" in mime_type:
        _, subtype = mime_type.split("/", 1)
        subtype = subtype.split(";")[0]
        # crude mapping
        if subtype in ("jpeg", "pjpeg"):
            return ".jpg"
        if subtype in ("plain",):
            return ".txt"
        return "." + subtype
    return ""


# ---------- Folder tree logic ---------- #

def export_folders():
    """
    Fetch all folder assets from OCM and dump to folders.json.

    NOTE: You may need to adjust the filter query / 'type' depending on OCM schema.
    """
    log.info("Exporting folder metadata...")
    all_folders = []
    offset = 0
    while True:
        params = {
            "repositoryId": REPOSITORY_ID,
            "offset": offset,
            "limit": PAGE_LIMIT,
            # Filter for folders, if your OCM supports it; else filter later.
            # "q": "(type:folder)"
        }
        data = get_json("management/api/v1.1/assets", params=params)
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            # Heuristic: treat anything with type=="folder" as folder
            if item.get("type") == "folder":
                all_folders.append(item)

        offset += PAGE_LIMIT

    with open(FOLDERS_JSON, "w") as f:
        json.dump(all_folders, f, indent=2)

    log.info("Exported %d folders to %s", len(all_folders), FOLDERS_JSON)
    return all_folders


def build_folder_paths(folders):
    """
    Build a mapping from OCM folder id -> relative path like "Compliance/Subfolder".

    You MUST adapt the key names below based on real OCM folder JSON:
    - folder_id_key: typically 'id'
    - parent_id_key: might be 'parentID', 'parentId', or something under 'parent'
    """
    folder_id_key = "id"
    parent_id_key_candidates = ["parentID", "parentId", "parentFolderId"]

    # Index by id
    by_id = {f[folder_id_key]: f for f in folders if folder_id_key in f}

    # Determine which parent key exists
    def get_parent_id(folder):
        for k in parent_id_key_candidates:
            if k in folder:
                return folder.get(k)
        # Some OCM schemas have parent as nested object
        parent = folder.get("parent")
        if isinstance(parent, dict):
            return parent.get("id")
        return None

    cache = {}

    def resolve_path(fid):
        if fid in cache:
            return cache[fid]
        folder = by_id.get(fid)
        if not folder:
            return ""
        name = sanitize_filename(folder.get("name", fid))
        parent_id = get_parent_id(folder)
        if parent_id and parent_id in by_id:
            parent_path = resolve_path(parent_id)
            path = os.path.join(parent_path, name) if parent_path else name
        else:
            path = name
        cache[fid] = path
        return path

    folder_paths = {}
    for fid in by_id:
        folder_paths[fid] = resolve_path(fid)

    log.info("Constructed folder paths for %d folders", len(folder_paths))
    return folder_paths


# ---------- Asset export & downloads ---------- #

def append_asset_metadata(asset):
    """
    Write asset JSON as a single line to assets.jsonl (thread-safe).
    """
    with meta_lock:
        with open(ASSETS_JSONL, "a") as f:
            f.write(json.dumps(asset) + "\n")


def download_asset_binary(asset, folder_paths):
    """
    Download one asset's binary with retries, streaming, and resume-by-skip.
    """
    asset_id = asset.get("id")
    name = asset.get("name") or asset_id
    mime_type = asset.get("mimeType")

    # Determine OCM folder ID field (adapt based on your JSON)
    folder_id = (
        asset.get("folderId")
        or asset.get("parentID")
        or asset.get("parentId")
        or None
    )

    rel_dir = ""
    if folder_id and folder_id in folder_paths:
        rel_dir = folder_paths[folder_id]

    local_dir = os.path.join(FILES_DIR, rel_dir) if rel_dir else FILES_DIR
    os.makedirs(local_dir, exist_ok=True)

    safe_name = sanitize_filename(name)
    ext = guess_ext(mime_type)
    filename = f"{asset_id}_{safe_name}{ext}"
    local_path = os.path.join(local_dir, filename)

    # If file already exists and non-zero size, skip
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        log.debug("Skipping existing file %s", local_path)
        append_asset_metadata(asset)
        return

    url = urljoin(OCM_BASE_URL, f"published/api/v1.1/assets/{asset_id}/native")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Downloading %s -> %s (attempt %d)", asset_id, local_path, attempt)
            with session.get(url, stream=True, timeout=120) as r:
                if r.status_code != 200:
                    log.warning("Download %s failed (%s): %s", asset_id, r.status_code, r.text)
                    raise RuntimeError(f"status {r.status_code}")
                tmp_path = local_path + ".part"
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                os.replace(tmp_path, local_path)
            append_asset_metadata(asset)
            return
        except Exception as e:
            log.warning("Error downloading %s: %s", asset_id, e)
            time.sleep(2 ** attempt)

    log.error("Giving up on asset %s after %d attempts", asset_id, MAX_RETRIES)


def export_assets(folder_paths):
    """
    Paginate through OCM assets, schedule downloads for non-folder items.

    Uses state.json['last_offset'] as a checkpoint.
    """
    state = load_state()
    offset = state.get("last_offset", 0)

    log.info("Starting asset export from offset=%d", offset)

    total_assets = 0
    file_assets = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []

        while True:
            params = {
                "repositoryId": REPOSITORY_ID,
                "offset": offset,
                "limit": PAGE_LIMIT,
            }
            data = get_json("management/api/v1.1/assets", params=params)
            items = data.get("items", [])
            if not items:
                log.info("No more items from OCM, stopping.")
                break

            log.info("Fetched %d assets at offset=%d", len(items), offset)
            total_assets += len(items)

            for item in items:
                # Skip folders here; we only download file-like assets
                if item.get("type") == "folder":
                    continue
                file_assets += 1
                futures.append(
                    executor.submit(download_asset_binary, item, folder_paths)
                )

            offset += PAGE_LIMIT
            # Update checkpoint after each page
            state["last_offset"] = offset
            save_state(state)

        # Wait for all downloads to finish
        for i, f in enumerate(as_completed(futures), 1):
            try:
                f.result()
            except Exception as e:
                log.error("Download task failed: %s", e)
            if i % 50 == 0:
                log.info("Completed %d downloads", i)

    log.info("Asset export completed. total=%d, files=%d", total_assets, file_assets)


def export_rbac():
    """
    Export repository members (RBAC). Optional but recommended.
    """
    log.info("Exporting RBAC for repository: %s", REPOSITORY_ID)
    path = f"management/api/v1.1/repositories/{REPOSITORY_ID}/members"
    data = get_json(path)

    members = data.get("items", data) if isinstance(data, dict) else data
    with open(RBAC_JSON, "w") as f:
        json.dump(members, f, indent=2)

    log.info("RBAC exported to %s (%d members)", RBAC_JSON, len(members))


def main():
    ensure_dirs()

    # 1) Export folders once and build folder paths
    if os.path.exists(FOLDERS_JSON):
        log.info("folders.json exists, reusing it.")
        with open(FOLDERS_JSON) as f:
            folders = json.load(f)
    else:
        folders = export_folders()

    folder_paths = build_folder_paths(folders)

    # 2) Export assets (files) with checkpoint
    export_assets(folder_paths)

    # 3) Export RBAC (for later Drive permissions sync)
    export_rbac()

    log.info("OCM export complete. Root dir: %s", EXPORT_ROOT)


if __name__ == "__main__":
    main()
