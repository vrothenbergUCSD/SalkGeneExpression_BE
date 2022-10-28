from google.cloud import bigquery

client = bigquery.Client()

db = "rbio-p-datasharing.gene_expression_database"


def get_database_metadata():
    """Returns complete list of database metadata describing each table.

    Returns:
        list: List of database metadata JSON row objects from database
    """
    table = "database_metadata"

    QUERY = f"SELECT * FROM `{db}.{table}`"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_datasets_metadata():
    """Returns complete list of datasets metadata describing each table.

    Returns:
        list: List of database metadata JSON row objects from database
    """
    table = "datasets_metadata"

    QUERY = f"SELECT * FROM `{db}.{table}`"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_datasets_metadata_by_table_name(table_name):
    """Returns datasets metadata entry that contains table_name.

    Returns:
        list: A database metadata JSON row object from database
    """
    table = "datasets_metadata"

    QUERY = f"""SELECT * FROM `{db}.{table}`
    WHERE gene_metadata_table_name = `{table_name}`
    OR sample_metadata_table_name = `{table_name}`
    OR gene_expression_data_table_name = `{table_name}`
    LIMIT 1"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)
