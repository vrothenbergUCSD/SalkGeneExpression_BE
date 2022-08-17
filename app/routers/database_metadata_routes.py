from fastapi import APIRouter

import app.fetch.database_metadata

router = APIRouter()


@router.get("/database_metadata/data")
async def get_database_metadata():
    """Returns complete list of database metadata describing all tables.

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.database_metadata.get_database_metadata()


@router.get("/datasets_metadata/data")
async def get_datasets_metadata():
    """Returns complete list of datasets metadata describing all tables.

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.database_metadata.get_datasets_metadata()
