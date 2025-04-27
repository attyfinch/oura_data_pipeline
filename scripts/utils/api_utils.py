import requests

def get_oura_data(endpoint, token, params=None):
    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(endpoint, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    return data["data"]
