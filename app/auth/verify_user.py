#  Authentication libraries
from firebase_admin import auth

from app.main import fs
from fastapi.exceptions import HTTPException


async def get_user_permission(authorization):
    print("get_user_permission")
    try:
        user = auth.verify_id_token(authorization)
        user_level = None
        if not user:
            user_level = "public"
        doc = fs.collection("admins").document(user["uid"]).get()
        if doc.exists:
            user_level = "admin"
        else:
            doc = fs.collection("uploaders").document(user["uid"]).get()
            if doc.exists:
                user_level = "uploader"
            else:
                user_level = "user"
        return user, user_level

    except Exception as e:
        print("Exception", e)
        return HTTPException(
            detail={
                "message": "auth.verify_user.get_user_permission: There was an error validating the token. "
                + str(e),
            },
            status_code=400,
        )
