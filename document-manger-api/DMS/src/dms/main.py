from fastapi import FastAPI, HTTPException, status , Query 
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import pyhdb
from typing import Dict, Any
import os
from dotenv import load_dotenv
import requests
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor
import logging

from src.dms.helper.token import get_access_token

from .models.integration_models import IntegrationCreate, ContainerCreate
from .integrations.base_integration import BaseIntegration
from .integrations.github_integration import GitHubIntegration

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="DMS API",
    description="Document Management System API",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
HANA_CONNECTION = {
    "host": os.getenv("HANA_HOST"),
    "port": int(os.getenv("HANA_PORT", "30015")),
    "user": os.getenv("HANA_USER"),
    "password": os.getenv("HANA_PASSWORD")
}

# Integration type mapping
INTEGRATION_CLASSES = {
    "github": GitHubIntegration,
    # Add other integration classes here
}


#Fetching the structure from Github \ integrations

class GitHubRequest(BaseModel):
    connection_config: Dict[str, Any]     #can be added 
    integration_config: Dict[str, Any]  #  repo_owner, repo_name  example 

from fastapi.concurrency import run_in_threadpool

@app.get("/github/load", tags=["GitHub"])
async def read_github_repo(dry_run: bool = Query(False, description="Set to true to skip DB insert")):
    """
    Compatible endpoint to load GitHub repository structure into HANA tables.
    Params:
    - dry_run: If True, fetches structure without writing to DB.
    """
    github = GitHubIntegration()

    try:
        result = await run_in_threadpool(github.setup_container, dry_run)  # Runs in a separate thread to avoid blocking

        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["message"])

        return JSONResponse(status_code=status.HTTP_200_OK, content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#app = FastAPI()

#@app.get("/github/load", tags=["GitHub"])
#async def read_github_repo(dry_run: bool = Query(False, description="Set to true to skip DB insert")):
#    github = GitHubIntegration()
#   result = github.setup_container(dry_run=dry_run)
#   return result

@app.get("/")
async def root():
    """Root endpoint to check if API is running"""
    return {"message": "DMS API is running"}

@app.post("/api/v1/integrations")
async def create_integration(integration: IntegrationCreate):
    """
    Create a new integration entry in the Integrations table
    """
    try:
        connection = pyhdb.connect(**HANA_CONNECTION)
        cursor = connection.cursor()

        try:
            # Insert into Integrations table
            query = """
                INSERT INTO Integrations (
                    IntegrationName, IntegrationType, ApiUrl, AccessToken, CreatedBy
                ) VALUES (?, ?, ?, ?, ?)
            """
            payload = {
                "INTEGRATIONNAME":integration.integration_name,
                "INTEGRATIONTYPE":integration.integration_type,
                "APIURL":integration.api_url,
                "ACCESSTOKEN":integration.access_token,
                "CREATED_BY":integration.created_by
            }
            
            url = "https://maas.cfapps.eu10-004.hana.ondemand.com/models/dms_integrations"
            
            # Get the generated integration ID
            access_token = get_access_token() 
            headers = {
                "Content-Type": "application/json",
                "accept": "application/json",
                "Authorization": f"Bearer {access_token}"
            }
            response = requests.post(
                url=url,
                headers=headers,
                json=payload,
                verify=False  
            )          

            if response.status_code == 200:            
                if response.headers.get("Content-Type") == "application/json":
                    response_data = response.json()                
                    integration_id = response_data.get("result", {}).get("id")                
                else:
                    response_data = response.text
                    print("Response Text:", response_data)
                    return None                         
            
                return JSONResponse(
                status_code=status.HTTP_201_CREATED,
                content={
                    "message": "Integration created successfully",
                    "integration_id": integration_id
                }
            )

        except Exception as e:
            connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create integration: {str(e)}"
            )
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}"
        )

@app.post("/api/v1/containers")
async def create_container(container: ContainerCreate):
    """
    Create a new container and scan its contents
    """
    try:
        connection = pyhdb.connect(**HANA_CONNECTION)
        cursor = connection.cursor()

        try:
            # First, verify the integration exists
            cursor.execute(
                "SELECT IntegrationType, ApiUrl, AccessToken FROM Integrations WHERE IntegrationId = ?",
                (container.integration_id,)
            )
            integration_row = cursor.fetchone()
            
            if not integration_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Integration with ID {container.integration_id} not found"
                )

            integration_type, api_url, access_token = integration_row

            # Insert container
            query = """
                INSERT INTO Containers (
                    ContainerName, IntegrationId, RootPath, CreatedBy
                ) VALUES (?, ?, ?, ?)
            """
            params = (
                container.container_name,
                container.integration_id,
                container.root_path,
                container.created_by
            )
            
            cursor.execute(query, params)
            
            # Get the generated container ID
            cursor.execute("SELECT CURRENT_IDENTITY_VALUE() FROM Containers")
            container_id = cursor.fetchone()[0]

            # Initialize the appropriate integration class based on integration type
            integration_config = {
                "api_url": api_url,
                "access_token": access_token,
                "root_path": container.root_path
            }

            integration_class = INTEGRATION_CLASSES.get(integration_type.lower())
            if not integration_class:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported integration type: {integration_type}"
                )

            integration = integration_class(
                connection_config=HANA_CONNECTION,
                integration_config=integration_config
            )

            # Scan and store folder/file structure
            integration.process_contents(cursor, container_id, None, container.root_path)
            
            connection.commit()
            
            return JSONResponse(
                status_code=status.HTTP_201_CREATED,
                content={
                    "message": "Container created and contents scanned successfully",
                    "container_id": container_id
                }
            )

        except Exception as e:
            if connection:
                connection.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create container: {str(e)}"
            )
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}"
        )

@app.get("/api/v1/integrations")
async def list_integrations():
    """
    List all integrations
    """
    try:
        connection = pyhdb.connect(**HANA_CONNECTION)
        cursor = connection.cursor()

        try:
            cursor.execute("""
                SELECT IntegrationId, IntegrationName, IntegrationType, ApiUrl, CreatedAt, CreatedBy 
                FROM Integrations
            """)
            
            columns = [desc[0] for desc in cursor.description]
            integrations = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"integrations": integrations}
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch integrations: {str(e)}"
            )
        finally:
            cursor.close()
            connection.close()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}"
        )

@app.get("/api/v1/containers")
async def list_containers():
    """
    List all containers
    """
    try:
        connection = pyhdb.connect(**HANA_CONNECTION)
        cursor = connection.cursor()

        try:
            cursor.execute("""
                SELECT c.ContainerId, c.ContainerName, c.RootPath, 
                       c.CreatedAt, c.CreatedBy, i.IntegrationName
                FROM Containers c
                JOIN Integrations i ON c.IntegrationId = i.IntegrationId
            """)
            
            columns = [desc[0] for desc in cursor.description]
            containers = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={"containers": containers}
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch containers: {str(e)}"
            )
        finally:
            cursor.close()
            connection.close()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.DEBUG)
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True, log_level="debug")