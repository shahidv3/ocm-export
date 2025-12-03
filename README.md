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

<img width="448" height="350" alt="image" src="https://github.com/user-attachments/assets/9cf1bf65-2e5d-438b-8535-cde876414670" />




---

## 🛠 1. Install Dependencies

```bash
pip install requests google-api-python-client google-auth-httplib2 \
    google-auth-oauthlib pyyaml


