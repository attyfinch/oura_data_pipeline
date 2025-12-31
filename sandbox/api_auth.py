import os
import json
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import requests
from datetime import datetime, timezone
from config.config import OURA_CLIENT_ID, OURA_CLIENT_SECRET

CLIENT_ID = OURA_CLIENT_ID
CLIENT_SECRET = OURA_CLIENT_SECRET
REDIRECT_URI = "http://localhost:8000/callback"
TOKEN_FILE = "secrets/oura_tokens.json"

AUTHORIZE_URL = "https://cloud.ouraring.com/oauth/authorize"
TOKEN_URL = "https://api.ouraring.com/oauth/token"

# Scopes - adjust based on what you need
SCOPES = "personal daily heartrate workout tag session spo2 heart_health stress"


class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/callback":
            params = parse_qs(parsed.query)
            if "code" in params:
                code = params["code"][0]
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h1>Authorization successful!</h1><p>You can close this window.</p>")
                self.server.auth_code = code
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"No code received")
                self.server.auth_code = None

    def log_message(self, format, *args):
        pass  # Suppress logging


def get_tokens(auth_code):
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )
    response.raise_for_status()
    tokens = response.json()
    
    # Add expires_at timestamp
    expires_at = datetime.now(timezone.utc).timestamp() + tokens["expires_in"]
    tokens["expires_at"] = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
    
    return tokens


def main():
    print(f"Client ID loaded: {bool(CLIENT_ID)}")
    print(f"Client Secret loaded: {bool(CLIENT_SECRET)}")
    
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Error: Missing OURA_CLIENT_ID or OURA_CLIENT_SECRET in .env")
        return
    
    # Build authorization URL
    auth_url = f"{AUTHORIZE_URL}?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&scope={SCOPES}"
    
    print(f"\nOpening browser for authorization...")
    print(f"If browser doesn't open, go to:\n{auth_url}\n")
    webbrowser.open(auth_url)
    
    # Start local server to receive callback
    server = HTTPServer(("localhost", 8000), OAuthHandler)
    server.auth_code = None
    
    print("Waiting for authorization callback...")
    while server.auth_code is None:
        server.handle_request()
    
    print(f"Received authorization code, exchanging for tokens...")
    
    tokens = get_tokens(server.auth_code)
    
    # Save tokens
    os.makedirs("secrets", exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f, indent=4)
    
    print(f"\nTokens saved to {TOKEN_FILE}")
    print("Setup complete!")


if __name__ == "__main__":
    main()