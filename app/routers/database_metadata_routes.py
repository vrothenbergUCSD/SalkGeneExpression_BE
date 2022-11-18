from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException

from firebase_admin import auth

import app.fetch.database_metadata
import app.auth.verify_user


router = APIRouter()


# @router.get("/database_metadata/data")
# async def get_database_metadata():
#     """Returns complete list of database metadata describing all tables.

#     Returns:
#         list: List of sample metadata JSON row objects from database
#     """
#     return app.fetch.database_metadata.get_database_metadata()

# TODO: Check if user has permission first
@router.get("/datasets_metadata/data")
async def get_datasets_metadata(
    authorization: str = Form(),
):
    """Returns complete list of datasets metadata describing all tables.

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    print("/datasets_metadata/data")
    try:
        user, user_level = app.auth.verify_user.get_user_permission(authorization)

        return app.fetch.database_metadata.get_datasets_metadata(user_level)

    except Exception as e:
        print("Error", e)
        return HTTPException(
            detail={
                "message": "get_datasets_metadata Error: " + str(e),
            },
            status_code=400,
        )
