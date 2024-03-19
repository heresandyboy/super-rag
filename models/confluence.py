from pathlib import Path
from pydantic import BaseModel
from unstructured.ingest.connector.confluence import ConfluenceAccessConfig, SimpleConfluenceConfig
from unstructured.ingest.interfaces import PartitionConfig, ProcessorConfig, ReadConfig


class Confluence(BaseModel):
    connector_config: SimpleConfluenceConfig


class ConfluenceFile:
    def __init__(self, local_path: Path, base_url: str):
        self.local_path = local_path
        self.base_url = base_url

    @property
    def original_url(self):
        # Extract the space name and page ID from the file path
        space_name = self.local_path.parent.name
        file_name_without_extension = self.local_path.stem
        return f"{self.base_url}wiki/spaces/{space_name}/pages/{file_name_without_extension}"

    def __repr__(self):
        return f"ConfluenceFile(local_path={self.local_path}, original_url={self.original_url})"
