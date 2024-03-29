# import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
# from starlette.requests import Request
import time
from datetime import datetime


# Authentication between FastAPI and Firebase
import uvicorn
import firebase_admin
import pyrebase
import json
import os 

from firebase_admin import credentials, firestore

# Check if running in development mode
development = False

if development:
    print('Development mode')
    cred = credentials.Certificate("rbio-p-datasharing-firebase-service_account_keys.json")
    firebase = firebase_admin.initialize_app(cred)
    pb = pyrebase.initialize_app(json.load(open("firebase_config.json")))
    fs = firestore.client()

else:
    print('Production mode')
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

class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        print(f"{timestamp} - Processing time: {process_time} secs")
        return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    # allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(TimingMiddleware)

# add_timing_middleware(app, record=logger.info, prefix="app", exclude="untimed")

app.include_router(gene_expression_routes.router)
app.include_router(gene_metadata_routes.router)
app.include_router(sample_metadata_routes.router)
app.include_router(database_metadata_routes.router)
app.include_router(dataset_upload_routes.router)
