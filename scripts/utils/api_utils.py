import requests
import json
from datetime import datetime, timezone
from config.config import TOKEN_FILE

def load_tokens():
    with open(TOKEN_FILE) as f:
        return json.load(f)

def save_tokens(tokens):
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f, indent=4)

def is_token_expired(expires_at_str):
    expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    buffer = 60
    return (expires_at.timestamp() - now.timestamp()) < buffer

def refresh_access_token(refresh_token, client_id, client_secret):
    response = requests.post(
        "https://api.ouraring.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
    )
    response.raise_for_status()
    return response.json()

def get_valid_token(client_id, client_secret):
    tokens = load_tokens()
    
    if is_token_expired(tokens["expires_at"]):
        new_tokens = refresh_access_token(tokens["refresh_token"], client_id, client_secret)
        expires_at = datetime.now(timezone.utc).timestamp() + new_tokens["expires_in"]
        new_tokens["expires_at"] = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
        save_tokens(new_tokens)
        return new_tokens["access_token"]
    
    return tokens["access_token"]

def get_oura_data(endpoint, client_id, client_secret, params=None):
    token = get_valid_token(client_id, client_secret)
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(endpoint, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data["data"]