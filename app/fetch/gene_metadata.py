# from sqlalchemy.orm import Session
from google.cloud import bigquery

client = bigquery.Client()

db = "rbio-p-datasharing.gene_expression_database"


def get_gene_metadata(limit: str, table: str):
    """Returns unfiltered list of gene metadata.

    Args:
        limit (str): Maximum number of rows to return
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    QUERY = f"""SELECT * FROM `{db}.{table}` LIMIT {limit}"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_metadata_all_names(limit: int, table: str):
    """Returns unfiltered list of gene names only.

    Args:
        limit (str): Maximum number of rows to return
        table (str): Name of table in database

    Returns:
        list: List of gene metadata JSON row objects from database
    """

    QUERY = f"""SELECT gene_name FROM `{db}.{table}` 
                LIMIT {limit}"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_metadata_by_gene_name(gene_names: str, table: str):
    """Returns filtered list of gene metadata, if gene_name in gene_names.

    Args:
        gene_names (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    gene_name_strs = ",".join([f"'{gene_name}'" for gene_name in gene_names.split(",")])
    QUERY = f"""SELECT * FROM `{db}.{table}` 
                WHERE gene_name IN ({gene_name_strs})"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_metadata_by_gene_id(gene_ids: str, table: str):
    """Returns filtered list of gene metadata, if gene_id in gene_ids.

    Args:
        gene_ids (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    gene_id_strs = ",".join([f"'{gene_id}'" for gene_id in gene_ids.split(",")])
    QUERY = f"""SELECT * FROM `{db}.{table}` 
                WHERE gene_id IN ({gene_id_strs})"""
    query_job = client.query(QUERY)
    rows = query_job.result()
    return list(rows)


def get_gene_metadata_by_chr(chrs: str, limit: int, table: str):
    """Returns filtered list of gene metadata, if chr in chrs.

    Args:
        chrs (str): List of chromosomes in string format e.g. chr7,chr8
        limit (int): Maximum number of rows to return
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    chr_strs = ",".join([f"'{chr}'" for chr in chrs.split(",")])
    QUERY = f"""SELECT * FROM `{db}.{table}` 
                WHERE chr IN ({chr_strs}) LIMIT {limit}"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)
