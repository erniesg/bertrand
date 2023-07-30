import os
import uuid
import json
import argparse
import asyncio
import sys

from loguru import logger
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Read the PARENT_DIR environment variable
parent_dir = os.environ.get("PARENT_DIR")

# Check if the environment variable is set and not empty
if parent_dir is None or parent_dir.strip() == "":
    raise ValueError("PARENT_DIR environment variable is not set or is empty.")

# Add the parent directory to the Python path
sys.path.append(parent_dir)

from models.models import Document, DocumentMetadata, Source
from datastore.datastore import DataStore
from datastore.factory import get_datastore
from services.extract_metadata import extract_metadata_from_document
from services.file import extract_text_from_filepath
from services.pii_detection import screen_text_for_pii

# More imports and initial setup here...
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import openai

# Get the Key Vault URL from an environment variable
KEY_VAULT_URL = os.getenv('KEY_VAULT_URL')

# Create a credential object using the DefaultAzureCredential class
credential = DefaultAzureCredential()

# Create a SecretClient object
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

try:
    # Retrieve the secrets
    OPENAI_API_KEY = secret_client.get_secret("openai-api").value
    # Set the API key
    openai.api_key = OPENAI_API_KEY
    # Print only the last 3 characters of each value
    print("OPENAI_API_KEY:", OPENAI_API_KEY[-3:])

except Exception as e:
    # If an exception occurs, print the error message
    print("Error fetching credentials:", e)


DOCUMENT_UPSERT_BATCH_SIZE = 50

async def process_directory(
    dirpath: str,
    datastore: DataStore,
    custom_metadata: dict,
    screen_for_pii: bool,
    extract_metadata: bool,
):
    documents = []
    skipped_files = []

    # Use os.walk to traverse the directory and its subdirectories
    for root, dirs, files in os.walk(dirpath):
        for filename in files:
            if len(documents) % 20 == 0:
                logger.info(f"Processed {len(documents)} documents")

            filepath = os.path.join(root, filename)

            # Rest of the file processing logic goes here...
            try:
                extracted_text = extract_text_from_filepath(filepath)
                logger.info(f"Extracted text from {filepath}")

                # Create a metadata object with the source and source_id fields
                metadata = DocumentMetadata(
                    source=Source.file,
                    source_id=filename,
                )

                # Update metadata with custom values
                for key, value in custom_metadata.items():
                    if key == "author":
                        # Convert the author value to a single string (if it's a list)
                        if isinstance(value, list):
                            value = ", ".join(value)
                    # Ensure 'author' is a string, even if it contains multiple authors as a list
                    if key == "author" and not isinstance(value, str):
                        value = str(value)

                    if hasattr(metadata, key):
                        setattr(metadata, key, value)

                # Screen for pii if requested
                if screen_for_pii:
                    pii_detected = screen_text_for_pii(extracted_text)
                    # If pii detected, print a warning and skip the document
                    if pii_detected:
                        logger.info("PII detected in document, skipping")
                        skipped_files.append(filepath)  # Add the skipped file to the list
                        continue

                # Extract metadata if requested
                if extract_metadata:
                    # Extract metadata from the document text
                    extracted_metadata = extract_metadata_from_document(
                        f"Text: {extracted_text}; Metadata: {str(metadata)}"
                    )
                    # Get a Metadata object from the extracted metadata
                    metadata = DocumentMetadata(**extracted_metadata)

                # Create a document object with a random id, text, and metadata
                document = Document(
                    id=str(uuid.uuid4()),
                    text=extracted_text,
                    metadata=metadata,
                )
                documents.append(document)
            except Exception as e:
                # Log the error and continue with the next file
                logger.error(f"Error processing {filepath}: {e}")
                skipped_files.append(filepath)  # Add the skipped file to the list


    # Batch upserting and logging here...
    # Do this in batches, the upsert method already batches documents but this allows
    # us to add more descriptive logging
    for i in range(0, len(documents), DOCUMENT_UPSERT_BATCH_SIZE):
        # Get the text of the chunks in the current batch
        batch_documents = documents[i : i + DOCUMENT_UPSERT_BATCH_SIZE]
        logger.info(f"Upserting batch of {len(batch_documents)} documents, batch {i}")
        await datastore.upsert(batch_documents)

    # Print the skipped files
    logger.info(f"Skipped {len(skipped_files)} files due to errors or PII detection")
    for file in skipped_files:
        logger.info(file)


async def main():
    # Command-line argument parsing here...
    # Parse the command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dirpath", required=True, help="The path to the directory")
    parser.add_argument(
        "--custom_metadata",
        default="{}",
        help="A JSON string of key-value pairs to update the metadata of the documents",
    )
    parser.add_argument(
        "--screen_for_pii",
        default=False,
        type=bool,
        help="A boolean flag to indicate whether to try the PII detection function (using a language model)",
    )
    parser.add_argument(
        "--extract_metadata",
        default=False,
        type=bool,
        help="A boolean flag to indicate whether to try to extract metadata from the document (using a language model)",
    )
    args = parser.parse_args()

    # Get the arguments
    dirpath = args.dirpath
    custom_metadata = json.loads(args.custom_metadata)
    screen_for_pii = args.screen_for_pii
    extract_metadata = args.extract_metadata

    # Initialize the db instance once as a global variable
    datastore = await get_datastore()

    # Process the directory
    await process_directory(
        dirpath, datastore, custom_metadata, screen_for_pii, extract_metadata
    )


if __name__ == "__main__":
    asyncio.run(main())
