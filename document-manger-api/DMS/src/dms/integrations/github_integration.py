import datetime
from importlib.resources import contents
import requests
import pyhdb
from typing import Dict, Any, Optional, List
from fastapi import HTTPException, status
import os
import requests
import pyhdb
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from hdbcli import dbapi
import aiohttp
import asyncio
import os

## hardcoded credentials as of now 

class GitHubIntegration:
    def __init__(self):
        self.token = os.getenv("GITHUB_TOKEN")            # from deloitte sap 
        self.repo_owner = os.getenv("GITHUB_REPO_OWNER")     
        self.repo_name = os.getenv("GITHUB_REPO_NAME")
        self.maas_url = os.getenv("MAAS_IMPORT_URL")  # Example ki tarah : http://localhost:8080/maas/api/container/import
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }

    async def fetch_repo_structure(self, session, path="") -> List[Dict[str, Any]]:
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/contents/{path}"
        async with session.get(url, headers=self.headers) as response:
            if response.status != 200:
                text = await response.text()
                raise Exception(f"Failed to fetch contents for {path}: {text}")
            return await response.json()

    async def build_tree(self, session, path="") -> Dict[str, Any]:
        contents = await self.fetch_repo_structure(session, path)
        tree = {
            "id": str(uuid.uuid4()),
            "name": path.split("/")[-1] if path else self.repo_name,
            "type": "folder",
            "children": []
        }

        for item in contents:
            if item["type"] == "dir":
                folder = await self.build_tree(session, item["path"])
                tree["children"].append(folder)
            elif item["type"] == "file":
                tree["children"].append({
                    "id": str(uuid.uuid4()),
                    "name": item["name"],
                    "type": "file",
                    "path": item["path"],
                    "size": item.get("size", 0)
                })

        return tree

    async def sync_repo_to_maas(self, dry_run=False) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                tree = await self.build_tree(session)

                if dry_run:
                    return {
                        "status": "success",
                        "message": "Dry run completed.",
                        "tree": tree
                    }

                async with session.post(self.maas_url, json=tree) as maas_response:
                    if maas_response.status != 200:
                        error = await maas_response.text()
                        raise Exception(f"Failed to post to MAAS: {error}")

                    return {
                        "status": "success",
                        "message": "GitHub repo structure uploaded to MAAS container."
                    }
        except Exception as e:
            return {"status": "error", "message": str(e)}
        
    def process_contents(self, cursor, container_id, parent_folder_id, path: str, integration_id: int, dry_run=False):
        contents = self.get_contents(path)
        for item in contents:
            item_path = item["path"]
            item_name = item["name"]
            item_type = item["type"]

            if item_type == "dir":
                folder_id = str(uuid.uuid4()) if dry_run else self.insert_folder(cursor, container_id, item_name, parent_folder_id, item_path, integration_id, "system")
                self.process_contents(cursor, container_id, folder_id if not dry_run else parent_folder_id, item_path, integration_id, dry_run)

            elif item_type == "file":
                if not dry_run:
                    file_size = item.get("size", 0)
                    file_type = item_name.split(".")[-1] if "." in item_name else "unknown"
                    self.insert_file(cursor, parent_folder_id, container_id, item_name, item_path, file_size, file_type, integration_id, "system")

    def setup_container(self, dry_run: bool = False):
        try:
            connection = pyhdb.connect(**self.connection_config)
            cursor = connection.cursor()

            cursor.execute("SELECT IntegrationId FROM DMS_Integrations WHERE IntegrationName = 'GitHub'")
            integration_row = cursor.fetchone()
            if not integration_row:
                raise Exception("GitHub integration not found in DMS_Integrations")

            integration_id = integration_row[0]
            container_id = str(uuid.uuid4()) if dry_run else self.insert_container(cursor, integration_id, self.repo_name, f"{self.repo_owner}/{self.repo_name}", "system")

            self.process_contents(cursor, container_id, None, "", integration_id, dry_run)

            if not dry_run:
                self.log_sync(cursor, integration_id, "SUCCESS", "GitHub repository data successfully stored in DMS database")
                connection.commit()

            return {
                "status": "success",
                "message": "GitHub repository structure fetched",
                "container_id": container_id,
                "dry_run": dry_run
            }

        except Exception as e:
            if not dry_run:
                connection.rollback()
            return {
                "status": "error",
                "message": str(e)
            }
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

