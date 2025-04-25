from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class IntegrationCreate(BaseModel):
    integration_name: str
    integration_type: str
    api_url: Optional[str] = None
    access_token: Optional[str] = None
    created_by: str

class ContainerCreate(BaseModel):
    integration_id: int
    container_name: str
    root_path: str
    created_by: str 