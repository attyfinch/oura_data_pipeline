import os
import requests
from datetime import datetime, timezone
import duckdb
from dotenv import load_dotenv

load_dotenv()

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
DATABASE = "oura"

def get_db_connection():
    return duckdb.connect(f"md:{DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")

def load_tokens():
    conn = get_db_connection()
    result = conn.execute("""
        SELECT access_token, refresh_token, expires_at 
        FROM auth_tokens 
        WHERE service = 'oura'
    """).fetchone()
    conn.close()
    
    if not result:
        raise ValueError("No Oura tokens found in database. Run initial token seed first.")
    
    return {
        "access_token": result[0],
        "refresh_token": result[1],
        "expires_at": result[2]
    }

def save_tokens(tokens):
    conn = get_db_connection()
    conn.execute("""
        UPDATE auth_tokens 
        SET access_token = ?,
            refresh_token = ?,
            expires_at = ?,
            updated_at = ?
        WHERE service = 'oura'
    """, [
        tokens["access_token"],
        tokens["refresh_token"],
        tokens["expires_at"],
        datetime.now(timezone.utc).isoformat()
    ])
    conn.close()

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