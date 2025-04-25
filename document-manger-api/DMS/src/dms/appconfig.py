import os
import json
import base64
# from .auth.auth import get_current_user
import requests
import datetime
from os.path import join, dirname, exists
from dotenv import load_dotenv


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


config_instance = None

class AppConfig:
    def __init__(self):
        # Load environment variables from .env file if it exists
        dotenv_path = join(dirname(__file__),  '.env')
        if exists(dotenv_path):
            print('Loading the local env file found at :', dotenv_path)
            load_dotenv(dotenv_path=dotenv_path)
        else:
            print(f"Warning: .env file not found at {dotenv_path}")

        self.LOCAL_ENV = os.getenv("ENV", "PROD").upper() == "LOCAL"
        if not self.LOCAL_ENV:
            from .auth.oauth2 import oauth2_scheme
            from .auth.auth import XSUAAMiddleware
            self.oauth2_scheme = oauth2_scheme
            self.auth_handler = XSUAAMiddleware()
        else:
            self.oauth2_scheme = None
            self.auth_handler = None
        
        self.destination_token_cache = {"token": None, "expires_at": None}
        self.connectivity_token_cache = {"token": None, "expires_at": None}

        if self.LOCAL_ENV:
            self._load_local_env()
        else:
            self._load_production_env()
        self.app = self._create_fastapi_app()
    
    def get_auth_dependencies(self):
        """Return authentication dependencies based on environment"""
        if self.LOCAL_ENV:
            return []
        from fastapi import Depends
        return [Depends(self.oauth2_scheme)]

    def get_user_dependency(self):
        """Return the appropriate user dependency based on environment"""
        if self.LOCAL_ENV:
            return None
        from fastapi import Security
        return Security(get_current_user)

    def _create_fastapi_app(self) -> FastAPI:
        app = FastAPI(
            title="Data Stories API",
            description="Enterprise-grade API for generating data stories",
            version="1.0.0"
        )
        
        # Configure CORS
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Configure OAuth UI
        app.swagger_ui_init_oauth = {
            "usePkceWithAuthorizationCodeGrant": False,
        }      
         
        return app

    def _load_local_env(self):
        self._load_common_env()
        self.SAP_PROVIDER_URL = self._get_env_var("SAP_PROVIDER_URL")
        self.SAP_CLIENT_ID = self._get_env_var("SAP_CLIENT_ID")
        self.SAP_CLIENT_SECRET = self._get_env_var("SAP_CLIENT_SECRET")
        self.SAP_ENDPOINT_URL_GPT4O = self._get_env_var("SAP_ENDPOINT_URL_GPT4O")
        self.SAP_EMBEDDING_ENDPOINT_URL = self._get_env_var("SAP_EMBEDDING_ENDPOINT_URL")
        self.ODATA_USERNAME = self._get_env_var("ODATA_USERNAME")
        self.ODATA_PASSWORD = self._get_env_var("ODATA_PASSWORD")
        self.ODATA_ENDPOINT = self._get_env_var("ODATA_ENDPOINT")       
        self.PROXIES = None
        # XSUAA Details for local environment
        self.XSUAA_URL = self._get_env_var("XSUAA_URL")
        self.XSUAA_CLIENT_ID = self._get_env_var("XSUAA_CLIENT_ID")
        self.XSUAA_CLIENT_SECRET = self._get_env_var("XSUAA_CLIENT_SECRET")
    def _load_production_env(self):
        self.LOCAL_ENV = os.getenv("ENV", "PROD").upper() == "LOCAL"
        if not self.LOCAL_ENV:
            from cfenv import AppEnv
        cenv = AppEnv()
        self._load_common_env()
        genai = cenv.get_service(name=os.getenv("AICORE_SERVICE_NAME", "aicore"))

        if genai:
            self.SAP_PROVIDER_URL = f"{genai.credentials['url']}/oauth/token"
            self.SAP_CLIENT_ID = genai.credentials["clientid"]
            self.SAP_CLIENT_SECRET = genai.credentials["clientsecret"]
            self.SAP_ENDPOINT_URL_GPT4O = f"{genai.credentials['serviceurls']['AI_API_URL']}/v2/inference/deployments/{self._get_env_var('AZURE_DEPLOYMENT_ID_4O')}/chat/completions?api-version={self.SAP_API_VERSION}"
            self.SAP_EMBEDDING_ENDPOINT_URL = f"{genai.credentials['serviceurls']['AI_API_URL']}/v2/inference/deployments/{self._get_env_var('AZURE_EMBEDDING_DEPLOYMENT_ID')}/embeddings?api-version={self.SAP_API_VERSION}"
            self._set_destination_service(cenv)
        else:
            raise ValueError("AI Core service not found. Please check your environment configuration.")
        
        xsuaa = cenv.get_service(name=os.getenv("XSUAA_SERVICE_NAME", "xsuaa"))
        if xsuaa:
            self.XSUAA_URL = xsuaa.credentials["url"]
            self.XSUAA_CLIENT_ID = xsuaa.credentials["clientid"]
            self.XSUAA_CLIENT_SECRET = xsuaa.credentials["clientsecret"]
        else:
            raise ValueError("XSUAA service not found. Please check your environment configuration.")

    def _load_common_env(self):
        self.SAP_GPT4O_MODEL = self._get_env_var("SAP_GPT4O_MODEL")
        self.SAP_API_VERSION = self._get_env_var("API_VERSION", "2023-05-15")
        self.LEEWAY = self._get_env_var("LEEWAY")
        self.STORY_DATA_PERSISTENT_ENDPOINT_URL= self._get_env_var("STORY_DATA_PERSISTENT_ENDPOINT_URL")
        self.STORY_SOURCE_PERSISTENT_ENDPOINT_URL= self._get_env_var("STORY_SOURCE_PERSISTENT_ENDPOINT_URL")
        self.STORY_UPDATE_STATUS =self._get_env_var("STORY_UPDATE_STATUS")
        self.CLIENT_SECRET = self._get_env_var("CLIENT_SECRET")
        self.CLIENT_ID = self._get_env_var("CLIENT_ID")
        self.TOKEN_URL = self._get_env_var("TOKEN_URL")        
        self.STORY_UPDATE_STATUS= self._get_env_var("STORY_UPDATE_STATUS")
    def _set_destination_service(self, cenv):
        self.destination_service = cenv.get_service(name="odata-service")
        self.uaa_service = cenv.get_service(name="xsuaa")
        self.connectivity_service = cenv.get_service(name="connectivity-service")
        self.destination_name = "DOUS4HANA"

        if self.destination_service and self.uaa_service and self.connectivity_service:            
            token = self.get_destination_token()
            conn_token = self.get_connectivity_token()
            
            headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
            destination_url = f"{self.destination_service.credentials['uri']}/destination-configuration/v1/destinations/{self.destination_name}"
            destination_details = requests.get(destination_url, headers=headers)

            if destination_details.status_code != 200:
                raise ValueError(f"Failed to retrieve destination: Status {destination_details.status_code} - {destination_details.text}")
            if destination_details.status_code != 200:
                raise ValueError(f"Failed to retrieve destination: Status {destination_details.status_code} - {destination_details.text}")
          
            destination_response = destination_details.json()
            
            self.ODATA_USERNAME = destination_response['destinationConfiguration'].get('User')
            self.ODATA_PASSWORD = destination_response['destinationConfiguration'].get('Password')
           
            self.ODATA_ENDPOINT = f"{destination_response['destinationConfiguration']['URL']}"          
            
            
            conn_proxy_host = self.connectivity_service.credentials["onpremise_proxy_host"]
            conn_proxy_port = int(self.connectivity_service.credentials["onpremise_proxy_http_port"])
            self.PROXIES = {
                "http": f"http://{conn_proxy_host}:{conn_proxy_port}",
                "https": f"https://{conn_proxy_host}:{conn_proxy_port}"
            }
            self.ODATA_HEADERS = {  
                "Content-Type": "application/xml",
                "Proxy-Authorization": f"Bearer {conn_token}",
                "SAP-Connectivity-SCC-Location_ID": "DOU"
            }

    def get_destination_token(self):
        if not self.destination_token_cache["token"] or self._is_token_expired(self.destination_token_cache):
            self._refresh_destination_token()
        return self.destination_token_cache["token"]

    def get_connectivity_token(self):
        if not self.connectivity_token_cache["token"] or self._is_token_expired(self.connectivity_token_cache):
            self._refresh_connectivity_token()
        return self.connectivity_token_cache["token"]

    def _is_token_expired(self, token_cache):
        return token_cache["expires_at"] is None or datetime.datetime.now().timestamp() >= token_cache["expires_at"]

    def _refresh_destination_token(self):
        auth_header = self._get_basic_auth_header(self.destination_service.credentials)
        form_data = self._get_token_form_data(self.destination_service.credentials)
        response = requests.post(f"{self.destination_service.credentials['url']}/oauth/token", data=form_data, headers=auth_header)

        if response.status_code != 200:
            raise ValueError(f"Failed to retrieve destination token: Status {response.status_code} - {response.text}")

        self.destination_token_cache["token"] = response.json().get('access_token')
        self.destination_token_cache["expires_at"] = datetime.datetime.now().timestamp() + 2 * 3600  # 2 hours

    def _refresh_connectivity_token(self):
        auth_header = self._get_basic_auth_header(self.connectivity_service.credentials)
        form_data = self._get_token_form_data(self.connectivity_service.credentials)
        response = requests.post(f"{self.connectivity_service.credentials['url']}/oauth/token", data=form_data, headers=auth_header)

        if response.status_code != 200:
            raise ValueError(f"Failed to retrieve connectivity token: Status {response.status_code} - {response.text}")

        self.connectivity_token_cache["token"] = response.json().get('access_token')
        self.connectivity_token_cache["expires_at"] = datetime.datetime.now().timestamp() + 2 * 3600  # 2 hours

    def _get_basic_auth_header(self, credentials):
        auth = f"{credentials['clientid']}:{credentials['clientsecret']}"
        return {'Authorization': 'Basic ' + base64.b64encode(auth.encode()).decode(), 'Content-Type': 'application/x-www-form-urlencoded'}

    def _get_token_form_data(self, credentials):
        return {
            'client_id': credentials['clientid'],
            'client_secret': credentials['clientsecret'],
            'grant_type': 'client_credentials'
        }

    def _get_env_var(self, key, default=None):
        value = os.getenv(key, default)
        if value is None:
            raise ValueError(f"Missing required environment variable: {key}")
        return value

    def _print_env(self):
        for key, value in os.environ.items():
            print(f"{key}={value}")

    def to_json(self):
        return json.dumps(self.__dict__, indent=4)

def get_config_instance():
    global config_instance
    if config_instance is None:
        config_instance = AppConfig()
    return config_instance

if __name__ == "__main__":
    app = get_config_instance()
    print(f"If LOCAL? : {app.LOCAL_ENV}")