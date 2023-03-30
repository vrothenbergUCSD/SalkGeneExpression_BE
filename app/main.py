# import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException


# Authentication between FastAPI and Firebase
import uvicorn
import firebase_admin
import pyrebase
import json

from firebase_admin import credentials, firestore

# from google.cloud import secretmanager

# # Create a client to access the Secrets Manager
# client = secretmanager.SecretManagerServiceClient()

# # Define the name of the secret containing the JSON file
# firebase_service_account = "firebase-service_account_keys"

# firebase_config = "firebase_config"

# project_id = "rbio-p-datasharing"

# # Access the secret
# response = client.access_secret_version(request={"name": f"projects/{project_id}/secrets/{firebase_service_account}/versions/latest"})

# # Get the payload (the secret's content)
# firebase_service_account_payload = response.payload.data.decode("UTF-8")

# # Access the secret
# response = client.access_secret_version(request={"name": f"projects/{project_id}/secrets/{firebase_config}/versions/latest"})

# # Get the payload (the secret's content)
# firebase_config_payload = response.payload.data.decode("UTF-8")


cred = credentials.Certificate("/firebase-service_account_keys.json")
firebase = firebase_admin.initialize_app(cred)
pb = pyrebase.initialize_app(json.load("/firebase_config.json"))
fs = firestore.client()

from app.routers import (
    gene_expression_routes,
    gene_metadata_routes,
    sample_metadata_routes,
    database_metadata_routes,
    dataset_upload_routes,
)

app = FastAPI()

origins = [
    "*",
    #     "rbio-p-datasharing.web.app",
    #     "*.rbio-p-datasharing.web.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# add_timing_middleware(app, record=logger.info, prefix="app", exclude="untimed")

app.include_router(gene_expression_routes.router)
app.include_router(gene_metadata_routes.router)
app.include_router(sample_metadata_routes.router)
app.include_router(database_metadata_routes.router)
app.include_router(dataset_upload_routes.router)
