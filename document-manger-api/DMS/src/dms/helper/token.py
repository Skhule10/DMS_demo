from ..appconfig import get_config_instance
from requests.auth import HTTPBasicAuth

config = get_config_instance()

def get_access_token():    
    token_url = config.TOKEN_URL
    client_id = config.CLIENT_ID
    client_secret = config.CLIENT_SECRET    
    auth = HTTPBasicAuth(client_id, client_secret)
    payload = {
        'grant_type': 'client_credentials'
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(token_url, auth=auth, data=payload, headers=headers)
    response_data = response.json()   
    return response_data['access_token']