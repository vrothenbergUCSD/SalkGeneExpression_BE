# # Authentication libraries
# import firebase_admin
# import pyrebase as pb
# import json
from firebase_admin import credentials, auth, firestore
from app.main import pb, fs


# import os

from fastapi import APIRouter, File, UploadFile, Form, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException

# import JSON

from typing import List

import app.post.upload
from app.models.dataset_models import DatasetMetadata


# cred = firebase_admin.credentials.Certificate(os.getenv("FIREBASE_AUTH_KEY"))
# firebase = firebase_admin.initialize_app(cred)
# pb = pyrebase.initialize_app(json.load(open(os.getenv("FIREBASE_CONFIG"))))

router = APIRouter()


@router.post("/upload/dataset")
async def post_dataset(
    metadata: DatasetMetadata = Depends(DatasetMetadata.as_form),
    gene_metadata_filename: str = Form(),
    sample_metadata_filename: str = Form(),
    gene_expression_data_filename: str = Form(),
    files: List[UploadFile] = File(None),
):
    """Uploads entire dataset and its associated metadata to BigQuery.
    If dataset already exists, overwrites it.

    Args:
        metadata (DatasetMetadata): _description_. Defaults to Depends(DatasetMetadata.as_form).
        gene_metadata_filename (str): _description_. Defaults to Form().
        sample_metadata_filename (str): _description_. Defaults to Form().
        gene_expression_data_filename (str): _description_. Defaults to Form().
        files (List[UploadFile]): List of CSV file objects. Defaults to File(None).

    Returns:
        _type_: _description_
    """
    print("/upload/dataset: post_dataset")

    gene_metadata_file = [
        file for file in files if file.filename == gene_metadata_filename
    ][0]
    sample_metadata_file = [
        file for file in files if file.filename == sample_metadata_filename
    ][0]
    gene_expression_data_file = [
        file for file in files if file.filename == gene_expression_data_filename
    ][0]

    return await app.post.upload.post_dataset(
        metadata=metadata,
        gene_metadata_file=gene_metadata_file,
        sample_metadata_file=sample_metadata_file,
        gene_expression_data_file=gene_expression_data_file,
    )


@router.post("/login_test", include_in_schema=True)
async def login_test(request: Request):
    # print("login_test")
    req_json = await request.json()
    # print(req_json)
    email = req_json["email"]
    password = req_json["password"]
    try:
        user = pb.auth().sign_in_with_email_and_password(email, password)
        jwt = user["idToken"]
        return JSONResponse(content={"token": jwt}, status_code=200)
    except:
        return HTTPException(
            detail={"message": "There was an error logging in"}, status_code=400
        )


@router.post("/test")
async def test(request: Request):
    req_json = await request.json()
    return JSONResponse(content={"token": req_json}, status_code=200)


# ping endpoint
@router.post("/ping", include_in_schema=True)
async def validate(
    authorization: str = Form(),
    # metadata: DatasetMetadata = Depends(DatasetMetadata.as_form),
    # gene_metadata_filename: str = Form(),
    # sample_metadata_filename: str = Form(),
    # gene_expression_data_filename: str = Form(),
    # files: List[UploadFile] = File(None),
):
    # headers = request.headers
    # jwt = headers.get("authorization")
    # jwt = authorization
    print(f"Auth Token: {authorization}")
    try:
        user = auth.verify_id_token(authorization)
        if not user:
            print("No User")
            return HTTPException(
                detail={
                    "message": "Failed token check. " + str(e),
                },
                status_code=400,
            )

        print("user")
        print(user)

        # print(fs.collection("admins"))

        # all_user_ids = fs.collection("admins").shallow().get()
        # print(all_user_ids)
        # print()
        # # doc = doc_ref.get()
        doc = fs.document("admin", user["uid"])
        if doc:
            # Is admin
            print("Admin")
        else:
            print("Not admin")

            # Fail

        # gene_metadata_file = [
        #     file for file in files if file.filename == gene_metadata_filename
        # ][0]
        # sample_metadata_file = [
        #     file for file in files if file.filename == sample_metadata_filename
        # ][0]
        # gene_expression_data_file = [
        #     file for file in files if file.filename == gene_expression_data_filename
        # ][0]

        resp_json = {
            "message": "Successful",
            "uid": user["uid"],
            # "gene_metadata_filename": gene_metadata_filename,
            # "sample_metadata_filename": sample_metadata_filename,
            # "gene_expression_data_filename": gene_expression_data_filename,
            # "metadata": metadata,
        }

        return JSONResponse(content=resp_json, status_code=200)

    except Exception as e:
        print("Exception", e)
        return HTTPException(
            detail={
                "message": "There was an error validating the token. " + str(e),
            },
            status_code=400,
        )


# signup endpoint
@router.post("/signup", include_in_schema=True)
async def signup(request: Request):
    req = await request.json()
    email = req["email"]
    password = req["password"]
    if email is None or password is None:
        return HTTPException(
            detail={"message": "Error! Missing Email or Password"}, status_code=400
        )
    try:
        user = auth.create_user(email=email, password=password)
        return JSONResponse(
            content={"message": f"Successfully created user {user.uid}"},
            status_code=200,
        )
    except:
        return HTTPException(detail={"message": "Error Creating User"}, status_code=400)
