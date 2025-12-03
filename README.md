# OCM → Google Workspace Migration Toolkit

A robust, production-ready migration workflow for exporting large datasets (up to 100GB+) from **Oracle Content Management (OCM)** and migrating them into **Google Workspace Shared Drives**, while **preserving folder structure and user RBAC**.

---

## 🚀 Migration Flow

**OCM API → Python Export → Local Disk (checkpointed) → rclone → Google Workspace (Shared Drive)**

This toolkit includes:

- `ocm_export_pro.py`  
  Parallel, resumable OCM exporter (files + metadata + folders + RBAC)

- `gdrive_rbac_sync.py`  
  Syncs OCM roles → Google Workspace Shared Drive permissions

- `config.yaml`  
  Central config file for URLs, IDs, credentials, tuning parameters

---

## 📦 Project Structure

├── config.yaml
├── ocm_export_pro.py
├── gdrive_rbac_sync.py
├── README.md
└── ocm_export/
├── files/
│ └── (folder tree replicated from OCM)
└── meta/
├── assets.jsonl
├── folders.json
├── rbac.json
└── state.json


---

## 🛠 1. Install Dependencies

```bash
pip install requests google-api-python-client google-auth-httplib2 \
    google-auth-oauthlib pyyaml


