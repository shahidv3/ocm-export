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

<img width="783" height="454" alt="image" src="https://github.com/user-attachments/assets/1209aaae-080f-45e9-b39b-2973ed33e1a8" />



---

## 🛠 1. Install Dependencies

```bash
pip install requests google-api-python-client google-auth-httplib2 \
    google-auth-oauthlib pyyaml


