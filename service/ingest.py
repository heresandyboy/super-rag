from typing import List

from models.confluence import Confluence
from models.file import File
from models.google_drive import GoogleDrive
from models.ingest import DocumentProcessorConfig
from service.embedding import EmbeddingService
from sources.confluence import ConfluenceHandler

from utils.logger import logger


async def handle_urls(
    *,
    embedding_service: EmbeddingService,
    files: List[File],
    config: DocumentProcessorConfig
):
    embedding_service.files = files
    chunks = await embedding_service.generate_chunks(config=config)
    summary_documents = await embedding_service.generate_summary_documents(
        documents=chunks
    )
    return chunks, summary_documents


async def handle_google_drive(
    _embedding_service: EmbeddingService, _google_drive: GoogleDrive
):
    pass


async def handle_confluence(
    embedding_service: EmbeddingService, confluence_config: Confluence
):
    try:
        confluence_handler = ConfluenceHandler(confluence_config)

        chunked_file_paths = confluence_handler.run()

        chunks = await confluence_handler.generate_chunks_from_file_paths(
            chunked_files=chunked_file_paths,
        )
        summary_documents = await embedding_service.generate_summary_documents(
            documents=chunks)

        return chunks, summary_documents
    except Exception as e:
        logger.error(f"An error occurred while handling Confluence: {str(e)}")
        raise e
