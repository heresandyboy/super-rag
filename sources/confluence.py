import json
import os
import uuid
import copy
import tiktoken
from pathlib import Path
from typing import List

from unstructured.ingest.interfaces import PartitionConfig, ProcessorConfig, ReadConfig, ChunkingConfig
from unstructured.ingest.runner import ConfluenceRunner

from models.confluence import Confluence, ConfluenceFile
from models.document import BaseDocument, BaseDocumentChunk
from utils.logger import logger


class ConfluenceHandler:
    def __init__(self, confluence_config: Confluence):
        self.confluence_config = confluence_config

    def run(self):
        """
        Initializes and runs the ConfluenceRunner, then collects and returns the paths to the chunked data files.
        """
        try:
            output_dir = "confluence-ingest-output"
            logger.info("Handling Confluence")
            runner = ConfluenceRunner(
                processor_config=ProcessorConfig(
                    reprocess=False,
                    verbose=True,
                    num_processes=2,
                    raise_on_error=True,
                    output_dir=output_dir,
                ),
                read_config=ReadConfig(
                    re_download=False,
                    preserve_downloads=False,
                    download_only=False,
                    # max_docs=100
                ),
                partition_config=PartitionConfig(
                    pdf_infer_table_structure=False,
                    strategy="auto",
                    # ocr_languages=["eng"],
                    ocr_languages=None,
                    encoding=None,
                    additional_partition_args=None,
                    skip_infer_table_types=None,
                    flatten_metadata=False,
                    metadata_exclude=["emphasized_text_contents", "emphasized_text_tags",
                                      "link_texts", "link_urls", "languages"],
                    partition_endpoint=os.getenv("UNSTRUCTURED_IO_SERVER_URL"),
                    partition_by_api=True,
                    api_key=os.getenv("UNSTRUCTURED_API_KEY"),
                    hi_res_model_name=None,
                ),
                connector_config=self.confluence_config.connector_config,
                chunking_config=ChunkingConfig(
                    chunk_elements=True),
            )
            base_url = self.confluence_config.connector_config.url
            logger.info("Running Confluence Runner")
            runner.run()

            chunked_file_objects = []
            for space_dir in Path(output_dir).iterdir():
                if space_dir.is_dir():
                    for file_path in space_dir.glob('*'):
                        logger.info(
                            f"Chunks saved: `{file_path}`, NAME: `{file_path.name}`")

                        confluence_file = ConfluenceFile(
                            local_path=file_path, base_url=base_url)
                        chunked_file_objects.append(confluence_file)

            logger.info(f"Collected {len(chunked_file_objects)} chunked data files.")
            return chunked_file_objects

        except Exception as e:
            logger.error(f"An error occurred while handling Confluence: {str(e)}")
            raise e

    async def generate_chunks_from_file_paths(self, chunked_files: List[ConfluenceFile]) -> List[BaseDocumentChunk]:
        try:
            doc_chunks = []
            for files in chunked_files:
                logger.info(f"Processing chunked data from: {files}")

                with open(files.local_path, "r", encoding="utf-8") as file:
                    chunks = json.load(file)
                    logger.info(f"Chunks: {chunks}")
                    document_id = f"doc_{uuid.uuid4()}"
                    for chunk in chunks:

                        metadata = chunk.get("metadata", {})
                        logger.info(f"Metadata from chunk: {metadata}")
                        logger.info(
                            f"Creating BaseDocumentChunk with source_type: {metadata['filetype']}, title: {metadata['filename']}, content: {chunk['text']}")

                        chunk_id = str(uuid.uuid4())
                        source_type = metadata.get("filetype", "unknown")
                        title = metadata.get("filename", "Untitled")
                        document_content = chunk.get("text", "")
                        path = files.local_path.as_posix()

                        doc_chunk = BaseDocumentChunk(
                            id=chunk_id,
                            doc_url=files.original_url,  # TODO: Can I construct the original url from data we have
                            document_id=document_id,
                            content=document_content,
                            source=path,
                            source_type=source_type,
                            # chunk_index=chunk["element_id"],# TODO: not sure how to get an index here
                            chunk_index=None,
                            title=title,
                            token_count=self._tiktoken_length(document_content),
                            metadata=self._sanitize_metadata(metadata),
                        )
                        logger.info(f"Created BaseDocumentChunk: {doc_chunk}")
                        doc_chunks.append(doc_chunk)

                    BaseDocument(
                        id=document_id,
                        content=document_content,
                        doc_url=files.original_url,
                        metadata={
                            "source": path,
                            "source_type": source_type,
                            "document_type": files.local_path.suffix.lstrip('.'),
                        },
                    )

            return doc_chunks
        except Exception as e:
            logger.error(f"Error loading Confluence chunks: {e}")
            raise

    def _tiktoken_length(self, text: str):
        tokenizer = tiktoken.get_encoding("cl100k_base")
        tokens = tokenizer.encode(text, disallowed_special=())
        return len(tokens)

    def _sanitize_metadata(self, metadata: dict) -> dict:
        def sanitize_value(value):
            if isinstance(value, (str, int, float, bool)):
                return value
            elif isinstance(value, list):
                # Ensure all elements in the list are of type str, int, float, or bool
                # Convert non-compliant elements to str
                sanitized_list = []
                for v in value:
                    if isinstance(v, (str, int, float, bool)):
                        sanitized_list.append(v)
                    elif isinstance(v, (dict, list)):
                        # For nested structures, convert to a string representation
                        sanitized_list.append(str(v))
                    else:
                        sanitized_list.append(str(v))
                return sanitized_list
            elif isinstance(value, dict):
                return {k: sanitize_value(v) for k, v in value.items()}
            else:
                return str(value)

        return {key: sanitize_value(value) for key, value in metadata.items()}
