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
import os 

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
try:
    print("Trying to open mounted volume /firebase-service_account_keys.json")
    with open('/firebase-service_account_keys.json') as f:
        firebase_service_keys = json.load(f)
        cred = credentials.Certificate(firebase_service_keys)
except Exception as e:
    print(e)
    print("Exception.  Now trying environment variable FIREBASE_SERVICE_KEYS")
    firebase_service_keys_str = os.environ.get('FIREBASE_SERVICE_KEYS')
    firebase_service_keys = json.loads(firebase_service_keys_str)
    print(firebase_service_keys)
    cred = credentials.Certificate(firebase_service_keys)

# cred = credentials.Certificate("/firebase-service_account_keys.json")
firebase = firebase_admin.initialize_app(cred)

try:
    print("Trying to open mounted volume /firebase_config.json")
    with open('/firebase_config.json') as f:
        firebase_config = json.load(f)
except Exception as e:
    print(e)
    print("Exception. Now trying environment variable FIREBASE_CONFIG")
    firebase_config_str = os.environ.get('FIREBASE_CONFIG')
    firebase_config = json.loads(firebase_config_str)
    print(firebase_config)

pb = pyrebase.initialize_app(firebase_config)
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
