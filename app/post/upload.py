"""
Upload functions
"""

import csv
import codecs
import time
import asyncio

from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.exceptions import HTTPException

from google.cloud import bigquery as bq

from firebase_admin import auth
from app.main import pb, fs

from app.models.dataset_models import GeneMetadataTable
from app.models.dataset_models import SampleMetadataTable
from app.models.dataset_models import GeneExpressionDataTable

import app.fetch.database_metadata

client = bq.Client()

# client.__http.mount("https://", adapter)
# client._http._auth_request.session.mount("https://", adapter)

DB = "rbio-p-datasharing.gene_expression_database"


def split(list_a, chunk_size):

    for i in range(0, len(list_a), chunk_size):
        yield list_a[i : i + chunk_size]


def post_gene_metadata(file):
    """_summary_

    Args:
        file (_type_): _description_

    Returns:
        _type_: _description_
    """
    csvReader = csv.DictReader(codecs.iterdecode(file.file, "utf-8"))
    return list(csvReader)


def post_sample_metadata(file):
    """_summary_

    Args:
        file (_type_): _description_

    Returns:
        _type_: _description_
    """
    csvReader = csv.DictReader(codecs.iterdecode(file.file, "utf-8"))
    return list(csvReader)


def post_gene_expression_data(file):
    """_summary_

    Args:
        file (_type_): _description_

    Returns:
        _type_: _description_
    """
    csvReader = csv.DictReader(codecs.iterdecode(file.file, "utf-8"))
    return list(csvReader)


async def table_exists(table_id):
    try:
        client.get_table(table_id)  # Make an API request.
        print("TABLE EXISTS!", table_id)
        # print(table_id)
        return True
    except Exception as error:
        print("ERROR! Does not exist", table_id)
        print(error)
        return False


def gene_metadata_pydantic(data: GeneMetadataTable):
    return data


async def create_gene_metadata_table(table_id: str):
    # table_id = f"{DB}.{table_name}"
    # print("table_id: ", table_id)
    print("create_gene_metadata_table")
    print(table_id)

    schema = [
        bq.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bq.SchemaField("gene_name", "STRING", mode="REQUIRED"),
        bq.SchemaField("refseq", "STRING", mode="REQUIRED"),
        bq.SchemaField("chr", "STRING", mode="REQUIRED"),
        bq.SchemaField("start", "INT64", mode="REQUIRED"),
        bq.SchemaField("end", "INT64", mode="REQUIRED"),
        bq.SchemaField("strand", "STRING", mode="REQUIRED"),
        bq.SchemaField("length", "FLOAT64", mode="REQUIRED"),
        bq.SchemaField("description", "STRING", mode="REQUIRED"),
        bq.SchemaField("ensembl_gene_id", "STRING", mode="REQUIRED"),
        bq.SchemaField("gene_biotype", "STRING", mode="REQUIRED"),
        bq.SchemaField("copies", "INT64", mode="NULLABLE"),
        bq.SchemaField("annotation_divergence", "STRING", mode="NULLABLE"),
        bq.SchemaField("external_gene_name", "STRING", mode="NULLABLE"),
        bq.SchemaField("ensembl_peptide_id", "STRING", mode="NULLABLE"),
    ]

    try:
        table = bq.Table(table_id, schema=schema)
        table.clustering_fields = ["gene_id"]
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
        return True
    except Exception as error:
        print("Error on create_gene_metadata_table")
        print(error)
        # print(Conflict)
        return False

    # errors = client.insert_rows_json(table_id, data)
    # return errors


async def upload_gene_metadata_table(data: GeneMetadataTable, table_id: str):
    print("upload_gene_metadata_table")
    chunked = list(split(data, 10000))
    # while not await table_exists(table_id):
    #     print("Waiting...")
    #     time.sleep(1)
    executor = ThreadPoolExecutor(10)
    threads = []
    # errors = []
    for c in chunked:
        threads.append(executor.submit(client.insert_rows_json, table_id, c))

    for future in as_completed(threads):
        if len(future.result()) > 0:
            return False

    return True


def sample_metadata_pydantic(data: SampleMetadataTable):
    return data


async def create_sample_metadata_table(table_id: str):
    # table_id = f"{DB}.{table_name}"
    print("create_sample_metadata_table")
    print(table_id)

    schema = [
        bq.SchemaField("sample_name", "STRING", mode="REQUIRED"),
        bq.SchemaField("species", "STRING", mode="REQUIRED"),
        bq.SchemaField("time_point", "STRING", mode="REQUIRED"),
        bq.SchemaField("group_name", "STRING", mode="REQUIRED"),
        bq.SchemaField("age_months", "INT64", mode="REQUIRED"),
        bq.SchemaField("gender", "STRING", mode="REQUIRED"),
        bq.SchemaField("tissue", "STRING", mode="REQUIRED"),
        bq.SchemaField("number_of_replicates", "INT64", mode="REQUIRED"),
        bq.SchemaField("data_type", "STRING", mode="REQUIRED"),
    ]

    try:
        table = bq.Table(table_id, schema=schema)
        table.clustering_fields = ["sample_name"]

        table = client.create_table(table)  # Make an API request.
        print("SUCCESS!")
        print(
            "Created table {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
        return True
    except Exception as error:
        print("Error on create_sample_metadata_table")
        print(error)
        return False
    # errors = client.insert_rows_json(table_id, data)
    # return errors


async def upload_sample_metadata_table(data: SampleMetadataTable, table_id: str):
    print("upload_sample_metadata_table")
    print(table_id)
    chunked = list(split(data, 1000))
    # while not table_exists(table_id):
    #     print("Waiting...")
    #     time.sleep(1)
    executor = ThreadPoolExecutor(5)
    threads = []
    for c in chunked:
        # print(c)
        threads.append(executor.submit(client.insert_rows_json, table_id, c))

    for future in as_completed(threads):
        if len(future.result()) > 0:
            return False

    return True


def gene_expression_data_pydantic(data: GeneExpressionDataTable):
    """Returns data in validated pydantic format.

    Args:
        data (GeneExpressionDataTable): List of GeneExpressionData to be converted to validated pydantic model.

    Returns:
        GeneExpressionDataTable: Validated rows of GeneExpressionData models.
    """
    return data


async def create_gene_expression_data_table(table_id: str):
    print("create_gene_expression_data_table")
    print(table_id)

    schema = [
        bq.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bq.SchemaField("sample_name", "STRING", mode="REQUIRED"),
        bq.SchemaField("gene_expression", "FLOAT", mode="REQUIRED"),
    ]

    try:
        table = bq.Table(table_id, schema=schema)
        table.clustering_fields = ["gene_id", "sample_name"]
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table: {}.{}.{}".format(
                table.project, table.dataset_id, table.table_id
            )
        )
        return True
    except Exception as error:
        print("Error on create_gene_expression_data_table")
        print(error)
        return False


async def upload_gene_expression_data_table(
    data: GeneExpressionDataTable, table_id: str
):
    print("upload_gene_expression_data_table")
    chunked = list(split(data, 50000))
    print("chunked", len(chunked))
    # while not table_exists(table_id):
    #     print("Waiting...")
    #     time.sleep(1)
    # errors = []
    # for c in chunked:
    #     errors += client.insert_rows_json(table_id, c)

    executor = ThreadPoolExecutor(10)
    threads = []
    for c in chunked:
        threads.append(executor.submit(client.insert_rows_json, table_id, c))

    for future in as_completed(threads):
        if len(future.result()) > 0:
            return False
    return True


async def replace_data_table_rows(table_id: str):
    """Delete all the rows from a specified table in the database.
    Used to clear out a table when overwriting with new data.

    Args:
        table_id (str): Table name to be cleared out from BigQuery database.
        e.g. 'TRF_2018_Mouse_Lung_gene_expression_data_UCb0eBc2ewPjv9ipwLaEUYSwdhh1'

    Returns:
        list[str]: List of errors, if any.
    """

    print("replace_data_table_rows")

    QUERY = f"""DELETE FROM `{DB}.{table_id}` WHERE true"""
    query_job = client.query(QUERY)  # API request
    errors = query_job.result()  # Waits for query to finish

    return errors


# TODO: Check if updating existing metadata
# Obsolete, using Firestore with update_dataset_metadata
async def post_dataset_metadata(metadata):
    """Posts dataset metadata information as a single row to datasets_metadata table.

    Args:
        metadata (DatasetMetadata): DatasetMetadata pydantic object.
          Must be converted to dict.

    Returns:
        list[str]: List of errors, if any.
    """
    print("upload.post_dataset_metadata")
    table_id = f"{DB}.datasets_metadata"
    errors = client.insert_rows_json(table_id, [metadata.dict()])
    return errors

async def create_dataset_metadata(table_name, metadata):
    """Creates dataset metadata document in datasets collection on Firestore.

    Args:
        metadata (DatasetMetadata): DatasetMetadata pydantic object.
            Must be converted to dict

    Returns:
        list[str]: List of errors, if any.
    """
    try:
        fs.collection(u'datasets').document(table_name).set(metadata.dict())
    except Exception as e:
        return HTTPException(
                detail={
                    "message": "Error on creating dataset metadata in Firestore. " + str(e),
                    "errorLog" : ["Error on creating dataset metadata in Firestore. " + str(e)]
                },
                status_code=400,
            )




async def update_dataset_metadata(
    doc_ref,
    gene_metadata_table_name,
    sample_metadata_table_name,
    gene_expression_data_table_name,
    valid=True
):
    """Update dataset metadata entry from datasets collection in Firestore.

    Args:
        old_gene_metadata_table_name (str)
        old_sample_metadata_table_name (str)
        old_gene_expression_data_table_name (str)

    Returns:
        list[str]: List of errors, if any.
    """
    print("upload.update_dataset_metadata")
    # old_gene_metadata_table_name, new_gene_metadata_table_name = gene_metadata_table_tuple
    # old_sample_metadata_table_name, new_sample_metadata_table_name = sample_metadata_table_tuple
    # old_gene_expression_data_table_name, new_gene_expression_data_table_name = gene_expression_data_table_tuple

   
    # QUERY = f"""DELETE `{DB}.datasets_metadata`  
    # WHERE gene_metadata_table_name = '{old_gene_metadata_table_name}'
    # AND sample_metadata_table_name = '{old_sample_metadata_table_name}'
    # AND gene_expression_data_table_name = '{old_gene_expression_data_table_name}'
    # """

    # query_job = client.query(QUERY)  # API request
    # errors = query_job.result()  # Waits for query to finish

    # return errors
    try:
        doc_ref.update({
            u'gene_metadata_table_name': gene_metadata_table_name,
            u'sample_metadata_table_name': sample_metadata_table_name,
            u'gene_expression_data_table_name': gene_expression_data_table_name,
            u'valid': valid
        })
    except Exception as e:
        return HTTPException(
                detail={
                    "message": "Error on updating dataset metadata in Firestore. " + str(e),
                    "errorLog" : ["Error on updating dataset metadata in Firestore. " + str(e)]
                },
                status_code=400,
            )





async def post_dataset(
    metadata,
    authorization,
    gene_metadata_file,
    sample_metadata_file,
    gene_expression_data_file,
    replace,
):
    """Validates gene metadata, sample metadata and gene expression data files.
    Creates tables in BigQuery for each CSV file.
    Table IDs are of the format 'project.dataset.table_name'
    Table names are of format 'Experiment_Species_Tissue_Year_firebaseUserID'
    User IDs are appended to table name to ensure uniqueness.

    Args:
        metadata (DatasetMetadata): Pydantic model of dataset metadata
        authorization (): 
        gene_metadata_file (File): Gene metadata CSV file from FormData API request body
        sample_metadata_file (File): Sample metadata CSV file from FormData API request body
        gene_expression_data_file (File): Gene expression data CSV file from FormData API request body

    Returns:
        dict: Dictionary of BigQuery table ids and error log.  Converted to JSON.
    """
    print("upload.post_dataset")
    errorLog = []
    owner = metadata.owner

    # Replace should be 0 or 1, 
    if type(replace) != bool:
        replace = bool(replace)

    # Authenticate ID token
    try:
        user = auth.verify_id_token(authorization)
        if not user:
            print("No User")
            return HTTPException(
                detail={
                    "message": "Failed token check. Invalid user. " + str(e),
                },
                status_code=401,
            )

        user_level = "user"
        user_doc = fs.collection("admins").document(user["uid"]).get()
        if user_doc.exists:
            user_level = "admin"
        else:
            user_doc = fs.collection("uploaders").document(user["uid"]).get()
            if user_doc.exists:
                user_level = "uploader"
            else:
                return HTTPException(
                    detail={
                        "message": "Invalid token permissions. Must be an admin or uploader.",
                    },
                    status_code=403,
                )
                
    except Exception as e:
        print("Exception", e)
        return HTTPException(
            detail={
                "message": "There was an error validating the token. " + str(e),
            },
            status_code=400,
        )

    print("Checking if data table exists")
    # Check if entry in metadata table exists in Firestore 
    # experiment_year_species_tissue 
    table_name = f"""{metadata.experiment}_{metadata.year}_{metadata.species}_{metadata.tissue}"""
    # print(table_name)
    doc_ref = fs.collection(u'datasets').document(table_name)
    # print('doc_ref', doc_ref)
    doc = doc_ref.get()
    # print('doc', doc)

    # already_exists = len(result) > 0
    # print("Already exists: ", already_exists)
    if doc.exists:
        print("Already exists")
        if replace:
            print("Replacing")
            # Replace table name suffixes
            doc_dict = doc.to_dict()
            old_gene_metadata_table_name = doc_dict['gene_metadata_table_name']
            metadata.gene_metadata_table_name = replace_suffix(
                old_gene_metadata_table_name
            )

            old_sample_metadata_table_name = doc_dict['sample_metadata_table_name']
            metadata.sample_metadata_table_name = replace_suffix(
                old_sample_metadata_table_name
            )

            old_gene_expression_data_table_name = doc_dict['gene_expression_data_table_name']
            metadata.gene_expression_data_table_name = replace_suffix(
                old_gene_expression_data_table_name
            )

            # Check all valid names
            if not (
                metadata.gene_metadata_table_name
                and metadata.sample_metadata_table_name
                and metadata.gene_expression_data_table_name
            ):
                return HTTPException(
                    detail={
                        "message": "Error on table names.  If replacing, existing table must have __n suffix.",
                        "errorLog" : ["Error on table names.  If replacing, existing table must have __n suffix."]
                    },
                    status_code=400,
                )
            

            # Not necessary, using Firestore to track metadata
            # # Delete old metadata table row
            # errorLog += await delete_dataset_metadata(
            #     old_gene_metadata_table_name,
            #     old_sample_metadata_table_name,
            #     old_gene_expression_data_table_name,
            # )
            # gene_metadata_table_tuple = (old_gene_metadata_table_name, 
            #                              metadata.gene_metadata_table_name)
            # sample_metadata_table_tuple = (old_sample_metadata_table_name, 
            #                                metadata.sample_metadata_table_name)
            # gene_expression_data_table_tuple = (old_gene_expression_data_table_name, 
            #                                     metadata.gene_expression_data_table_name)

            await update_dataset_metadata(
                doc_ref,
                metadata.gene_metadata_table_name,
                metadata.sample_metadata_table_name,
                metadata.gene_expression_data_table_name
            )


        else:
            print("Table already exists.  Set replace to 1.")
            return HTTPException(
                detail={
                    "message": "Table already exists. Set replace to 1.",
                    "errorLog" : ["Table already exists. Set replace to 1."]
                },
                status_code=400,
            )
        
    else:
        print('Table does not exist.  Creating metadata.')
        await create_dataset_metadata(table_name, metadata)

    print("Setting table IDs")
    gene_metadata_table_id = f"{DB}.{metadata.gene_metadata_table_name}"
    sample_metadata_table_id = f"{DB}.{metadata.sample_metadata_table_name}"
    gene_expression_data_table_id = f"{DB}.{metadata.gene_expression_data_table_name}"

    # await asyncio.gather(
    #         create_gene_metadata_table(gene_metadata_table_id),
    #         create_sample_metadata_table(sample_metadata_table_id),
    #         create_gene_expression_data_table(gene_expression_data_table_id)
    # )
    
    gene_metadata_exists = await table_exists(gene_metadata_table_id)
    if not gene_metadata_exists:
        print('not exists')
        await create_gene_metadata_table(gene_metadata_table_id)

    sample_metadata_exists = await table_exists(sample_metadata_table_id)
    if not sample_metadata_exists:
        await create_sample_metadata_table(sample_metadata_table_id)

    gene_expression_data_exists = await table_exists(gene_expression_data_table_id)
    if not gene_expression_data_exists:
        await create_gene_expression_data_table(gene_expression_data_table_id)

    print("After create tables")
    print("Before gene_metadata_list")
    gene_metadata_list = list(
        csv.DictReader(codecs.iterdecode(gene_metadata_file.file, "utf-8-sig"))
    )
    print("After gene_metadata_list")
    gene_metadata = gene_metadata_pydantic(gene_metadata_list)
    print("gene_metadata", len(gene_metadata), type(gene_metadata))

    sample_metadata_list = list(
        csv.DictReader(codecs.iterdecode(sample_metadata_file.file, "utf-8-sig"))
    )
    sample_metadata = sample_metadata_pydantic(sample_metadata_list)
    print("sample_metadata", len(sample_metadata), type(sample_metadata))

    gene_expression_data_list = list(
        csv.DictReader(codecs.iterdecode(gene_expression_data_file.file, "utf-8-sig"))
    )
    gene_expression_data = gene_expression_data_pydantic(gene_expression_data_list)
    print("gene_expression_data", len(gene_expression_data), type(gene_expression_data))

    print("Before final upload")
    start = time.time()


    uploadResults = []
    uploadResults += await asyncio.gather(
        upload_gene_metadata_table(gene_metadata, gene_metadata_table_id),
        upload_sample_metadata_table(sample_metadata, sample_metadata_table_id),
        upload_gene_expression_data_table(
            gene_expression_data, gene_expression_data_table_id
        ),
    )
    end = time.time()
    print(f'\nTime taken to upload %d (s)\n', end-start)
    print('errorLog')
    print(errorLog)
    print('uploadResults')
    print(uploadResults)
    for r in uploadResults:
        if r is False:
            print('Error on uploading.  Undo dataset metadata.')
            await update_dataset_metadata(
                doc_ref,
                metadata.gene_metadata_table_name,
                metadata.sample_metadata_table_name,
                metadata.gene_expression_data_table_name,
                valid=False
            )
            return HTTPException(
                detail={
                    "message": "Error on uploading datasets to BigQuery.",
                    "errorLog" : ["Error on uploading datasets to BigQuery."]
                },
                status_code=400,
            )

    return {
        "gene_metadata_table_id": gene_metadata_table_id,
        "sample_metadata_table_id": sample_metadata_table_id,
        "gene_expression_data_table_id": gene_expression_data_table_id,
        "errorLog": [],
    }


def replace_suffix(table_name):
    # Replace suffix, should be an integer after __
    s = table_name.split("__")
    # Default integer suffix
    num = 1
    if len(s) == 2:
        # If old table name already has an integer, increment it
        num = int(s[1]) + 1
    s = s[0] + "__" + str(num)
    return s
