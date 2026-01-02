import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from datetime import datetime, timezone
import duckdb
from config.config import MOTHERDUCK_TOKEN, DATABASE


class OuraAPIError(Exception):
    """Custom exception for Oura API errors"""
    def __init__(self, message, status_code=None, endpoint=None):
        self.message = message
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(self.message)


class TokenError(Exception):
    """Custom exception for token-related errors"""
    pass


class DatabaseError(Exception):
    """Custom exception for database operations"""
    pass


def get_db_connection():
    try:
        return duckdb.connect(f"md:{DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")
    except Exception as e:
        raise DatabaseError(f"Failed to connect to MotherDuck: {e}")


def load_tokens():
    try:
        conn = get_db_connection()
        result = conn.execute("""
            SELECT access_token, refresh_token, expires_at 
            FROM auth_tokens 
            WHERE service = 'oura'
        """).fetchone()
        conn.close()
    except DatabaseError:
        raise
    except Exception as e:
        raise DatabaseError(f"Failed to load tokens from database: {e}")
    
    if not result:
        raise TokenError("No Oura tokens found in database. Run initial token seed first.")
    
    return {
        "access_token": result[0],
        "refresh_token": result[1],
        "expires_at": result[2]
    }


def save_tokens(tokens):
    try:
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
    except DatabaseError:
        raise
    except Exception as e:
        raise DatabaseError(f"Failed to save tokens to database: {e}")


def is_token_expired(expires_at_str):
    try:
        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        buffer = 60
        return (expires_at.timestamp() - now.timestamp()) < buffer
    except (ValueError, AttributeError) as e:
        raise TokenError(f"Invalid token expiration format: {expires_at_str}. Error: {e}")


def refresh_access_token(refresh_token, client_id, client_secret):
    try:
        response = requests.post(
            "https://api.ouraring.com/oauth/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    
    except Timeout:
        raise OuraAPIError("Token refresh request timed out", endpoint="oauth/token")
    except ConnectionError:
        raise OuraAPIError("Could not connect to Oura API for token refresh", endpoint="oauth/token")
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 401:
            raise TokenError("Refresh token is invalid or expired. Re-authorization required.")
        elif status_code == 400:
            raise TokenError(f"Bad token refresh request: {e.response.text}")
        else:
            raise OuraAPIError(f"Token refresh failed: {e.response.text}", status_code=status_code, endpoint="oauth/token")
    except RequestException as e:
        raise OuraAPIError(f"Token refresh request failed: {e}", endpoint="oauth/token")


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
    try:
        token = get_valid_token(client_id, client_secret)
    except (TokenError, DatabaseError, OuraAPIError):
        raise
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("data", data)
    
    except Timeout:
        raise OuraAPIError(f"Request timed out", endpoint=endpoint)
    except ConnectionError:
        raise OuraAPIError(f"Could not connect to Oura API", endpoint=endpoint)
    except HTTPError as e:
        status_code = e.response.status_code
        if status_code == 401:
            raise TokenError("Access token rejected. Token may have been revoked.")
        elif status_code == 403:
            raise OuraAPIError("Access forbidden - check API scopes", status_code=403, endpoint=endpoint)
        elif status_code == 404:
            raise OuraAPIError("Endpoint not found", status_code=404, endpoint=endpoint)
        elif status_code == 429:
            raise OuraAPIError("Rate limited by Oura API", status_code=429, endpoint=endpoint)
        else:
            raise OuraAPIError(f"API request failed: {e.response.text}", status_code=status_code, endpoint=endpoint)
    except RequestException as e:
        raise OuraAPIError(f"Request failed: {e}", endpoint=endpoint)