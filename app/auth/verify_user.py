#  Authentication libraries
from firebase_admin import auth

from app.main import fs
from fastapi.exceptions import HTTPException

from fastapi import Form


def get_user_read_permission(user_id: str, 
                                dataset: dict,
                                groups: dict):
    print('get_user_read_permission', user_id)
    # permission_groups = fs.collection("permission_groups").stream()
    # groups = {group.id : group.to_dict() for group in permission_groups}
    if user_id is None:
        for group_id in dataset['reader_groups']:
            # Get group 
            group_dict = groups[group_id]
            if group_dict['name'] == 'Public':
                return True
        return False
    
    if 'permittedUsers' in dataset and user_id in dataset['permittedUsers']:
        return True
    
    if 'admin_groups' in dataset:
        for group_id in dataset['admin_groups']:
            group_dict = groups[group_id]
            if group_dict['name'] == 'Public':
                return True
            if user_id in group_dict['admin_users']:
                return True 
                
    if 'editor_groups' in dataset:
        for group_id in dataset['editor_groups']:
            group_dict = groups[group_id]
            if user_id in group_dict['admin_users']:
                return True 
            if user_id in group_dict['editor_users']:
                return True 

    if 'reader_groups' in dataset:
        for group_id in dataset['reader_groups']:
            # Get group 
            group_dict = groups[group_id]
            if group_dict['name'] == 'Public':
                return True
            if user_id in group_dict['admin_users']:
                return True 
            if user_id in group_dict['editor_users']:
                return True 
            if user_id in group_dict['reader_users']:
                return True 
            
    return False
    




# TODO: Granular permission levels
# Currently only checks if a user is an admin, 
async def get_user_level(
        authorization: str = Form(),
                   ):
    """Returns tuple of

    Args:
        authorization (FormData): _description_

    Returns:
        tuple: Tuple of (user level, user id)
    """
    try:
        user = auth.verify_id_token(authorization)
        if not user:
            print("No User")
            return ("anon", None)

        doc = fs.collection("admins").document(user["uid"]).get()
        if doc.exists:
            print('Admin')
            return ("admin", user['uid'])
        else:
            doc = fs.collection("uploaders").document(user["uid"]).get()
            if doc.exists:
                print('Uploader')
                return ("uploader", user['uid'])
            else:
                print('User')
                return ("user", user['uid'])

    except Exception as e:
        print("Exception", e)
        return ("anon", None)

async def get_user_permission(authorization):
    print("verify_user.get_user_permission")
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

# async def get_user_permission(authorization):
#     print("verify_user.get_user_permission")
#     try:
#         user = auth.verify_id_token(authorization)
#         user_level = None
#         if not user:
#             user_level = "public"
#         doc = fs.collection("admins").document(user["uid"]).get()
#         if doc.exists:
#             user_level = "admin"
#         else:
#             doc = fs.collection("uploaders").document(user["uid"]).get()
#             if doc.exists:
#                 user_level = "uploader"
#             else:
#                 user_level = "user"
#         return user, user_level

#     except Exception as e:
#         print("Exception", e)
#         return HTTPException(
#             detail={
#                 "message": "auth.verify_user.get_user_permission: There was an error validating the token. "
#                 + str(e),
#             },
#             status_code=400,
#         )
