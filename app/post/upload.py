"""
Upload functions
"""

import csv
import codecs
import time

from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery as bq

from app.models.dataset_models import GeneMetadataTable
from app.models.dataset_models import SampleMetadataTable
from app.models.dataset_models import GeneExpressionDataTable

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


def table_exists(table_id):
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


def create_gene_metadata_table(table_id: str):
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
    while not table_exists(table_id):
        print("Waiting...")
        time.sleep(1)
    executor = ThreadPoolExecutor(10)
    threads = []
    errors = []
    for c in chunked:
        threads.append(executor.submit(client.insert_rows_json, table_id, c))

    for future in as_completed(threads):
        errors.append(future.result())

    return errors


def sample_metadata_pydantic(data: SampleMetadataTable):
    return data


def create_sample_metadata_table(table_id: str):
    # table_id = f"{DB}.{table_name}"
    print("create_sample_metadata_table")
    print(table_id)

    schema = [
        bq.SchemaField("sample_name", "STRING", mode="REQUIRED"),
        bq.SchemaField("species", "STRING", mode="REQUIRED"),
        bq.SchemaField("time_point", "STRING", mode="REQUIRED"),
        bq.SchemaField("group_name", "STRING", mode="REQUIRED"),
        bq.SchemaField("age_years", "FLOAT64", mode="REQUIRED"),
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
        # print(Conflict)
        return False
    # errors = client.insert_rows_json(table_id, data)
    # return errors


async def upload_sample_metadata_table(data: SampleMetadataTable, table_id: str):
    print("upload_sample_metadata_table")
    print(table_id)
    chunked = list(split(data, 1000))
    while not table_exists(table_id):
        print("Waiting...")
        time.sleep(1)
    executor = ThreadPoolExecutor(5)
    threads = []
    errors = []
    for c in chunked:
        # print(c)
        threads.append(executor.submit(client.insert_rows_json, table_id, c))

    for future in as_completed(threads):
        errors.append(future.result())

    return errors


def gene_expression_data_pydantic(data: GeneExpressionDataTable):
    """Returns data in validated pydantic format.

    Args:
        data (GeneExpressionDataTable): List of GeneExpressionData to be converted to validated pydantic model.

    Returns:
        GeneExpressionDataTable: Validated rows of GeneExpressionData models.
    """
    return data


def create_gene_expression_data_table(table_id: str):
    print("create_gene_expression_data_table")
    print(table_id)

    schema = [
        bq.SchemaField("gene_id", "STRING", mode="REQUIRED"),
        bq.SchemaField("sample_name", "STRING", mode="REQUIRED"),
        bq.SchemaField("gene_expression", "FLOAT64", mode="REQUIRED"),
    ]

    try:

        table = bq.Table(table_id, schema=schema)
        table.clustering_fields = ["gene_id", "sample_name"]
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(
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
    while not table_exists(table_id):
        print("Waiting...")
        time.sleep(1)
    # errors = []
    # for c in chunked:
    #     errors += client.insert_rows_json(table_id, c)

    executor = ThreadPoolExecutor(10)
    threads = []
    errors = []
    for c in chunked:
        threads.append(executor.submit(client.insert_rows_json, table_id, c))

    for future in as_completed(threads):
        errors.append(future.result())

    return errors


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


async def post_dataset(
    metadata,
    gene_metadata_file,
    sample_metadata_file,
    gene_expression_data_file,
):
    """Validates gene metadata, sample metadata and gene expression data files.
    Creates tables in BigQuery for each CSV file.
    Table IDs are of the format 'project.dataset.table_name'
    Table names are of format 'Experiment_Species_Tissue_Year_firebaseUserID'
    User IDs are appended to table name to ensure uniqueness.

    Args:
        metadata (DatasetMetadata): Pydantic model of dataset metadata
        gene_metadata_file (File): Gene metadata CSV file from FormData API request body
        sample_metadata_file (File): Sample metadata CSV file from FormData API request body
        gene_expression_data_file (File): Gene expression data CSV file from FormData API request body

    Returns:
        dict: Dictionary of BigQuery table ids and error log.  Converted to JSON.
    """
    print("upload.post_dataset")
    errorLog = []
    owner = metadata.owner
    gene_metadata_table_id = f"{DB}.{metadata.gene_metadata_table_name}"
    sample_metadata_table_id = f"{DB}.{metadata.sample_metadata_table_name}"
    gene_expression_data_table_id = f"{DB}.{metadata.gene_expression_data_table_name}"
    # print("owner", owner)
    # print(gene_metadata_table_id)
    # print(sample_metadata_table_id)
    # print(gene_expression_data_table_id)
    create_gene_metadata_table(gene_metadata_table_id)
    create_sample_metadata_table(sample_metadata_table_id)
    create_gene_expression_data_table(gene_expression_data_table_id)
    gene_metadata_list = list(
        csv.DictReader(codecs.iterdecode(gene_metadata_file.file, "utf-8-sig"))
    )
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

    errorLog += await upload_gene_metadata_table(gene_metadata, gene_metadata_table_id)
    errorLog += await upload_sample_metadata_table(
        sample_metadata, sample_metadata_table_id
    )
    errorLog += await upload_gene_expression_data_table(
        gene_expression_data, gene_expression_data_table_id
    )
    errorLog += await post_dataset_metadata(metadata)

    return {
        "gene_metadata_table_id": gene_metadata_table_id,
        "sample_metadata_table_id": sample_metadata_table_id,
        "gene_expression_data_table_id": gene_expression_data_table_id,
        "errorLog": errorLog,
    }
