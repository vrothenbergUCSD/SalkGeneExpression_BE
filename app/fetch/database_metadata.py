# from google.cloud import bigquery

# client = bigquery.Client()

# db = "rbio-p-datasharing.gene_expression_database"

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from firebase_admin import auth

import json
import asyncio

from app.main import pb, fs

import app.auth.verify_user 

# # TODO: Only return datasets user has permission to view.
# async def get_datasets_metadata(
#         authorization: str = Form()
#         ):
#     """Returns list of datasets metadata describing each table dependent upon user level permissions.

#     Returns:
#         list: List of database metadata JSON row objects from database
#     """
#     print('get_datasets_metadata')
#     docs = fs.collection('datasets').stream()
#     result = []
#     user_level, uid = await app.auth.verify_user.get_user_level(authorization)
#     print('user_level', user_level)
#     print('uid', uid)

#     permission_groups = fs.collection("permission_groups").stream()
#     groups = {group.id : group.to_dict() for group in permission_groups}

#     # Check user permissions
#     # TODO: More granular permissions 
#     if user_level in ["admin"]:
#         # Access to all datasets
#         for doc in docs:
#             doc_dict = doc.to_dict()
#             if doc_dict['valid']:
#                 result.append(doc_dict)
        
#     else: 
#         # Access to only permitted datasets
#         for doc in docs:
#             doc_dict = doc.to_dict()
#             permitted = app.auth.verify_user.get_user_read_permission(uid, doc_dict, groups)
#             if permitted and doc_dict['valid']:
#                 result.append(doc_dict)

#     return result

async def process_experiment(experiment, user_level, uid, groups):
    
    experiment_name = experiment.id
    experiment_dict = experiment.to_dict()
    # print('process_experiment started', experiment.id)

    if user_level not in ["admin"] and not app.auth.verify_user.get_user_read_permission(uid, experiment_dict, groups):
        return []

    tissues_metadata = json.loads(experiment_dict['metadata'])

    for tissue_metadata in tissues_metadata:
        tissue_metadata['condition'] = json.dumps(tissue_metadata['condition'])
        tissue_metadata['gender'] = json.dumps(tissue_metadata['gender'])
        tissue_metadata['reader_groups'] = experiment_dict['reader_groups']
        tissue_metadata['editor_groups'] = experiment_dict['editor_groups']
        tissue_metadata['admin_groups'] = experiment_dict['admin_groups']
        tissue_metadata['owner'] = experiment_dict['owner']

    # print('process_experiment finished', experiment_name)

    return tissues_metadata

async def get_datasets_metadata_fs(
        authorization: str = Form()
        ):
    """Returns list of experiments metadata describing each table dependent upon user level permissions.

    Returns:
        list: List of database metadata JSON row objects from Firestore
    """
    print('get_datasets_metadata')
    experiments = fs.collection('experiments').stream()
    user_level, uid = await app.auth.verify_user.get_user_level(authorization)
    print('user_level', user_level)
    print('uid', uid)

    permission_groups = fs.collection("permission_groups").stream()
    groups = {group.id : group.to_dict() for group in permission_groups}

    # Create a list of tasks to process experiments in parallel
    tasks = [process_experiment(experiment, user_level, uid, groups) for experiment in experiments]
    
    # Run all tasks in parallel and collect the results
    results = await asyncio.gather(*tasks)

    # Flatten the results into a single list
    result = [item for sublist in results for item in sublist]

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