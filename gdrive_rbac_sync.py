#!/usr/bin/env python3
"""
gdrive_rbac_sync.py

Applies RBAC on a Google Shared Drive based on OCM RBAC export.

Input: ocm_export/metadata/rbac.json

Two modes:
- Direct user assignment
- Group-based assignment (recommended)
"""

import os
import json
import logging

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ------------- CONFIG ------------- #

META_DIR = "./ocm_export/metadata"
RBAC_JSON = os.path.join(META_DIR, "rbac.json")

SHARED_DRIVE_ID = "<YOUR_SHARED_DRIVE_ID>"

SCOPES = ["https://www.googleapis.com/auth/drive"]

# Map OCM role -> Google Drive role
OCM_ROLE_TO_GDRIVE_ROLE = {
    "manager": "organizer",
    "contributor": "writer",
    "reader": "reader",
    "viewer": "reader",
}

# If you want to assign to GROUPS instead of individual users:
USE_GROUPS = False

# When USE_GROUPS=True, map OCM role -> Google Group email
ROLE_TO_GROUP = {
    "manager": "ocm-compliance-managers@your-domain.com",
    "contributor": "ocm-compliance-editors@your-domain.com",
    "reader": "ocm-compliance-viewers@your-domain.com",
    "viewer": "ocm-compliance-viewers@your-domain.com",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------------------------- #


def get_drive_service():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build("drive", "v3", credentials=creds)
    return service


def load_rbac():
    if not os.path.exists(RBAC_JSON):
        logging.error("rbac.json not found at %s", RBAC_JSON)
        return []
    with open(RBAC_JSON) as f:
        data = json.load(f)
    # Adjust this if OCM structure differs
    return data


def add_permission(drive, email, role, type_="user"):
    """
    Adds permission on the Shared Drive.
    For Shared Drives, we use 'drive' permission.
    """
    body = {
        "role": role,
        "type": type_,
        "emailAddress": email,
    }
    logging.info("Adding %s permission for %s on shared drive %s", role, email, SHARED_DRIVE_ID)
    try:
        drive.permissions().create(
            fileId=SHARED_DRIVE_ID,
            body=body,
            supportsAllDrives=True,
            sendNotificationEmail=False,
        ).execute()
    except Exception as e:
        logging.error("Failed to add permission for %s: %s", email, e)


def sync_rbac_direct_users(drive, members):
    """
    Assign permissions directly to each user.
    """
    for m in members:
        # You may need to adapt keys based on actual OCM RBAC schema
        role = (m.get("role") or "").lower()
        email = m.get("email") or m.get("loginName") or m.get("name")

        if not email:
            logging.warning("Skipping member without email: %s", m)
            continue

        gdrive_role = OCM_ROLE_TO_GDRIVE_ROLE.get(role)
        if not gdrive_role:
            logging.warning("Unknown OCM role '%s' for %s", role, email)
            continue

        add_permission(drive, email, gdrive_role, type_="user")


def sync_rbac_groups(drive, members):
    """
    Map OCM roles -> Google Groups and assign group permissions only once.
    """
    roles_present = set((m.get("role") or "").lower() for m in members)
    for role in roles_present:
        if not role:
            continue
        gdrive_role = OCM_ROLE_TO_GDRIVE_ROLE.get(role)
        group_email = ROLE_TO_GROUP.get(role)

        if not gdrive_role or not group_email:
            logging.warning(
                "No mapping for role '%s' (gdrive_role=%s, group=%s)",
                role,
                gdrive_role,
                group_email,
            )
            continue

        add_permission(drive, group_email, gdrive_role, type_="group")


def main():
    drive = get_drive_service()
    members = load_rbac()
    if not members:
        logging.error("No RBAC data to sync.")
        return

    if USE_GROUPS:
        logging.info("Syncing RBAC in GROUP mode...")
        sync_rbac_groups(drive, members)
    else:
        logging.info("Syncing RBAC in DIRECT USER mode...")
        sync_rbac_direct_users(drive, members)

    logging.info("RBAC sync complete.")


if __name__ == "__main__":
    main()
