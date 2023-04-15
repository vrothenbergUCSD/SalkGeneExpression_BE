# from google.cloud import bigquery

# client = bigquery.Client()

# db = "rbio-p-datasharing.gene_expression_database"

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from firebase_admin import auth

from app.main import pb, fs

import app.auth.verify_user 

# TODO: Only return datasets user has permission to view.
async def get_datasets_metadata(
        authorization: str = Form()
        ):
    """Returns list of datasets metadata describing each table dependent upon user level permissions.

    Returns:
        list: List of database metadata JSON row objects from database
    """
    print('get_datasets_metadata')
    docs = fs.collection(u'datasets').stream()
    result = []
    user_level, uid = await app.auth.verify_user.get_user_level(authorization)
    print('user_level', user_level)
    print('uid', uid)

    permission_groups = fs.collection("permission_groups").stream()
    groups = {group.id : group.to_dict() for group in permission_groups}

    # Check user permissions
    # TODO: More granular permissions 
    if user_level in ["admin"]:
        # Access to all datasets
        for doc in docs:
            doc_dict = doc.to_dict()
            if doc_dict['valid']:
                result.append(doc_dict)
        
    else: 
        # Access to only permitted datasets
        for doc in docs:
            doc_dict = doc.to_dict()
            permitted = app.auth.verify_user.get_user_read_permission(uid, doc_dict, groups)
            if permitted and doc_dict['valid']:
                result.append(doc_dict)

    return result


# TODO: Only return datasets user has permission to view.
def get_datasets_metadata_by_table_name(table_name):
    """Returns datasets metadata entry that contains table_name.

    Returns:
        list: A database metadata JSON row object from database
    """
    print("get_datasets_metadata_by_table_name", table_name)

    doc_ref = fs.collection(u'datasets').document(table_name)

    doc = doc_ref.get()
    result = []
    if doc.exists:
        print(f'Document data: {doc.to_dict()}')
        result.append(doc.to_dict())
    else:
        print(u'No such document!')
    return result

    # docs = fs.collection(u'datasets').stream()
    # result = []
    # for doc in docs:
    #     if 
    #     result.append(doc.to_dict())
    #     # print(f'{doc.id} => {doc.to_dict()}')
    # return result

def get_datasets_genes(
        # user_level: str
        ):
    """Returns complete list of genes.

    Returns:
        list: List of gene_id strings.
    """
    print('get_datasets_genes')
    # Check user permissions

    # Only show datasets 

    docs = fs.collection(u'gene_list').stream()
    gene_list = [doc.id for doc in docs]
    return gene_list