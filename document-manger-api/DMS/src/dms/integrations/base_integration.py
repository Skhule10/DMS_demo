from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class BaseIntegration(ABC):
    def __init__(self, connection_config: Dict[str, Any], integration_config: Dict[str, Any]):
        self.connection_config = connection_config
        self.integration_config = integration_config

    @abstractmethod
    def get_contents(self, path: str = "") -> Any:
        """Get contents from the integration source"""
        pass

    @abstractmethod
    def setup_container(self) -> Dict[str, Any]:
        """Setup container and sync data"""
        pass

    def insert_container(self, cursor, integration_id: int, container_name: str, root_path: str, created_by: str) -> int:
        cursor.execute("""
            INSERT INTO DMS_Containers (ContainerId, ContainerName, IntegrationId, RootPath, CreatedBy, CreatedAt)
            VALUES (NEXT VALUE FOR DMS_Containers_Seq, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (container_name, integration_id, root_path, created_by))
        
        cursor.execute("SELECT CURRENT_IDENTITY_VALUE() FROM DMS_Containers")
        return cursor.fetchone()[0]

    def insert_folder(self, cursor, container_id: int, folder_name: str, 
                     parent_folder_id: Optional[int], folder_path: str, created_by: str) -> int:
        cursor.execute("""
            INSERT INTO DMS_Folders (FolderId, FolderName, ContainerId, ParentFolderId, FolderPath, CreatedBy, CreatedAt)
            VALUES (NEXT VALUE FOR DMS_Folders_Seq, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (folder_name, container_id, parent_folder_id, folder_path, created_by))
        
        cursor.execute("SELECT CURRENT_IDENTITY_VALUE() FROM DMS_Folders")
        return cursor.fetchone()[0]

    def insert_file(self, cursor, folder_id: int, container_id: int, file_name: str, 
                   file_path: str, file_size: int, file_type: str, created_by: str) -> None:
        cursor.execute("""
            INSERT INTO DMS_Files (FileId, FileName, FolderId, ContainerId, FilePath, FileSize, FileType, CreatedBy, CreatedAt)
            VALUES (NEXT VALUE FOR DMS_Files_Seq, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (file_name, folder_id, container_id, file_path, file_size, file_type, created_by)) 