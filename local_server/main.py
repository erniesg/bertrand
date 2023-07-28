# This is a version of the main.py file found in ../../../server/main.py for testing the plugin locally.
# Use the command `poetry run dev` to run this.
from typing import Optional
import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Body, UploadFile
from loguru import logger
import uuid

from models.api import (
    DeleteRequest,
    DeleteResponse,
    QueryRequest,
    QueryResponse,
    UpsertRequest,
    UpsertResponse,
)
from datastore.factory import get_datastore
from services.file import get_document_from_file

from starlette.responses import FileResponse

from models.models import DocumentMetadata, Source
from fastapi.middleware.cors import CORSMiddleware

from dotenv import load_dotenv
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging
import traceback
import openai
# Create a logger for the upsert endpoint
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Load the .env file
load_dotenv()

# Get the Key Vault URL from an environment variable
KEY_VAULT_URL = os.getenv('KEY_VAULT_URL')

# Create a credential object using the DefaultAzureCredential class
credential = DefaultAzureCredential()

# Create a SecretClient object
secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

try:
    # Retrieve the secrets
    BEARER_TOKEN = secret_client.get_secret("bearer-token").value
    OPENAI_API_KEY = secret_client.get_secret("openai-api").value
    DATASTORE = os.getenv('DATASTORE')
    # Set the API key
    openai.api_key = OPENAI_API_KEY
    # Print only the last 3 characters of each value
    print("BEARER_TOKEN:", BEARER_TOKEN[-3:])
    print("OPENAI_API_KEY:", OPENAI_API_KEY[-3:])
    print("DATASTORE:", DATASTORE[-3:])

except Exception as e:
    # If an exception occurs, print the error message
    print("Error fetching credentials:", e)

app = FastAPI()

PORT = 3333

origins = [
    f"http://localhost:{PORT}",
    "https://chat.openai.com",
    "https://gestum.serveo.net",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.route("/.well-known/ai-plugin.json")
async def get_manifest(request):
    file_path = "./local_server/ai-plugin.json"
    simple_headers = {}
    simple_headers["Access-Control-Allow-Private-Network"] = "true"
    return FileResponse(file_path, media_type="text/json", headers=simple_headers)


@app.route("/.well-known/logo.png")
async def get_logo(request):
    file_path = "./local_server/logo.png"
    return FileResponse(file_path, media_type="text/json")


@app.route("/.well-known/openapi.yaml")
async def get_openapi(request):
    file_path = "./local_server/openapi.yaml"
    return FileResponse(file_path, media_type="text/json")


@app.post(
    "/upsert-file",
    response_model=UpsertResponse,
)
async def upsert_file(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
):
    try:
        metadata_obj = (
            DocumentMetadata.parse_raw(metadata)
            if metadata
            else DocumentMetadata(source=Source.file)
        )
    except:
        metadata_obj = DocumentMetadata(source=Source.file)

    document = await get_document_from_file(file, metadata_obj)

    try:
        ids = await datastore.upsert([document])
        return UpsertResponse(ids=ids)
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail=f"str({e})")


@app.post("/upsert", response_model=UpsertResponse)
async def upsert(request: UpsertRequest = Body(...)):
    try:
        logger.info("Received upsert request.")
        logger.debug("Request Payload: %s", request.json())

        # Generate unique IDs for each document in the request
        for document in request.documents:
            if document.id is None:
                document.id = str(uuid.uuid4())  # Generate a new UUID

        # Log details about the document being upserted
        logger.debug("Request Payload (after UUID generation): %s", request.json())

        # Upsert the validated documents to the datastore
        ids = await datastore.upsert(request.documents)

        # Log the IDs of the upserted documents
        logger.info("Upsert successful. IDs: %s", ids)

        return UpsertResponse(ids=ids)
    except RetryError as retry_error:
        # Log the RetryError and traceback
        logger.error("RetryError occurred during upsert: %s", retry_error)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Retry Error occurred during upsert")
    except AuthenticationError as auth_error:
        # Log the AuthenticationError and traceback
        logger.error("AuthenticationError occurred during upsert: %s", auth_error)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Authentication Error occurred during upsert")
    except Exception as e:
        # Log the general exception and traceback
        exc_traceback = traceback.format_exc()
        logger.error("Error occurred during upsert: %s", e)
        logger.error("Traceback:\n%s", exc_traceback)
        raise HTTPException(status_code=500, detail="Internal Service Error\n" + exc_traceback)
@app.post("/query", response_model=QueryResponse)
async def query_main(request: QueryRequest = Body(...)):
    try:
        results = await datastore.query(
            request.queries,
        )
        return QueryResponse(results=results)
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail="Internal Service Error")


@app.delete(
    "/delete",
    response_model=DeleteResponse,
)
async def delete(
    request: DeleteRequest = Body(...),
):
    if not (request.ids or request.filter or request.delete_all):
        raise HTTPException(
            status_code=400,
            detail="One of ids, filter, or delete_all is required",
        )
    try:
        success = await datastore.delete(
            ids=request.ids,
            filter=request.filter,
            delete_all=request.delete_all,
        )
        return DeleteResponse(success=success)
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail="Internal Service Error")


@app.on_event("startup")
async def startup():
    global datastore
    try:
        datastore = await get_datastore()
        logger.info("Datastore initialized successfully")
    except Exception as e:
        logger.error(f"Error occurred while initializing datastore: {e}")
        raise


def start():
    uvicorn.run("local_server.main:app", host="localhost", port=PORT, reload=True)
