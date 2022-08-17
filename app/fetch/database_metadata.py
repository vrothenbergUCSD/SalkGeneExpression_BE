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
