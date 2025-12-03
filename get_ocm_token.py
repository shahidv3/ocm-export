import requests
import yaml
import sys
import base64
from pathlib import Path


def load_config():
    """Load configuration from config.yaml"""
    config_path = Path("config.yaml")

    if not config_path.exists():
        print("❌ ERROR: config.yaml not found.")
        sys.exit(1)

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_ocm_token(client_id, client_secret, token_url, scope):
    """Fetch OAuth2 Client Credentials token from IDCS"""
    print("🔐 Requesting OCM API token...")

    try:
        response = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "scope": scope
            },
            auth=(client_id, client_secret),
            timeout=15
        )

        if response.status_code != 200:
            print(f"❌ ERROR: Failed to get token. Status: {response.status_code}")
            print("Response:", response.text)
            sys.exit(1)

        token_json = response.json()
        access_token = token_json.get("access_token")

        if not access_token:
            print("❌ ERROR: No access_token returned from IDCS.")
            print(token_json)
            sys.exit(1)

        print("✅ Token successfully retrieved.")
        return access_token

    except Exception as e:
        print("❌ Exception while getting token:", str(e))
        sys.exit(1)


def save_token(token):
    """Save token to local file for re-use"""
    with open("ocm_token.txt", "w") as f:
        f.write(token)
    print("💾 Token saved to ocm_token.txt")


if __name__ == "__main__":
    config = load_config()

    CLIENT_ID = config["ocm"]["client_id"]
    CLIENT_SECRET = config["ocm"]["client_secret"]
    TOKEN_URL = config["ocm"]["token_url"]
    SCOPE = config["ocm"]["scope"]

    token = get_ocm_token(CLIENT_ID, CLIENT_SECRET, TOKEN_URL, SCOPE)
    save_token(token)

    print("\n🔑 Your OCM API token:")
    print(token)
