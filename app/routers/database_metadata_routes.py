from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException

from firebase_admin import auth

from app.main import pb, fs

import app.fetch.database_metadata

router = APIRouter()

        
@router.post("/datasets_metadata/data")
async def get_datasets_metadata(
    authorization: str = Form()
):
    """Returns complete list of datasets metadata describing all tables, depending upon user permissions.

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    print("/datasets_metadata/data")
    return await app.fetch.database_metadata.get_datasets_metadata_fs(authorization)



@router.get("/datasets_metadata/genes")
async def get_datasets_metadata_genes(
    # authorization: str = Form(),
):
    """Returns complete list of datasets metadata describing all tables.

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    print("/datasets_metadata/data")
    return app.fetch.database_metadata.get_datasets_genes()