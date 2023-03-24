# from google.cloud import bigquery

# client = bigquery.Client()

# db = "rbio-p-datasharing.gene_expression_database"

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
from firebase_admin import auth

from app.main import pb, fs


# def get_database_metadata():
#     """Returns complete list of database metadata describing each table.

#     Returns:
#         list: List of database metadata JSON row objects from database
#     """
#     table = "database_metadata"

#     QUERY = f"SELECT * FROM `{db}.{table}`"
#     query_job = client.query(QUERY)  # API request
#     rows = query_job.result()  # Waits for query to finish
#     return list(rows)


# TODO: Only return datasets user has permission to view.
def get_datasets_metadata(
        # user_level: str
        ):
    """Returns complete list of datasets metadata describing each table.

    Returns:
        list: List of database metadata JSON row objects from database
    """
    print('get_datasets_metadata')
    # table = "datasets_metadata"

    # QUERY = f"""SELECT * FROM `{db}.{table}`"""
    # query_job = client.query(QUERY)  # API request
    # rows = list(query_job.result())  # Waits for query to finish
    # return rows

    docs = fs.collection(u'datasets').stream()
    result = []
    for doc in docs:
        result.append(doc.to_dict())
        # print(f'{doc.id} => {doc.to_dict()}')
    return result


# TODO: Only return datasets user has permission to view.
def get_datasets_metadata_by_table_name(table_name):
    """Returns datasets metadata entry that contains table_name.

    Returns:
        list: A database metadata JSON row object from database
    """
    print("get_datasets_metadata_by_table_name", table_name)

    # QUERY = f"""SELECT * FROM `{db}.datasets_metadata`
    # WHERE enabled = TRUE
    # AND
    # (gene_metadata_table_name = '{table_name}'
    # OR sample_metadata_table_name = '{table_name}'
    # OR gene_expression_data_table_name = '{table_name}')
    # LIMIT 1"""
    # query_job = client.query(QUERY)  # API request
    # rows = query_job.result()  # Waits for query to finish
    # return list(rows)

    # raise NotImplementedError

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
