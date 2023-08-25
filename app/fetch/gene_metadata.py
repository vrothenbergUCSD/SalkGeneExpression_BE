# from sqlalchemy.orm import Session
from google.cloud import bigquery
from app.main import fs

client = bigquery.Client()

db = "rbio-p-datasharing.gene_expression_database"


def get_gene_metadata(table: str):
    """Returns unfiltered list of gene metadata.

    Args:
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    QUERY = f"""SELECT * FROM `{db}.{table}`"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_metadata_all_names(table: str):
    """Returns unfiltered list of gene names only.

    Args:
        table (str): Name of table in database

    Returns:
        list: List of gene metadata JSON row objects from database
    """

    QUERY = f"""SELECT gene_name FROM `{db}.{table}` """
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)

def get_gene_metadata_all_names_fs(table: str):
    """Returns unfiltered list of gene names only.

    Args:
        table (str): Table in format 'ExperimentID_TissueID_DataType'

    Returns:
        list: List of gene IDs from Firestore
    """
    experiment_id, tissue_id, data_type = table.split('_')
    tissue_ref = (
        fs.collection(u'experiments').document(experiment_id)
        .collection(u'tissues').document(tissue_id)
        )
    
    gene_ids = tissue_ref.get().to_dict()['gene_names']
    
    return gene_ids


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


def get_gene_metadata_by_gene_name_fs(gene_names: str, table: str):
    """Returns filtered list of gene metadata, if gene_name in gene_names.

    Args:
        gene_names (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str): Table in format 'ExperimentID_TissueID_DataType'

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    gene_names_list = gene_names.split(',')
    experiment_id, tissue_id, data_type = table.split('_')
    gene_metadata_ref = (
        fs.collection(u'experiments').document(experiment_id)
        .collection(u'tissues').document(tissue_id)
        .collection(u'gene_expression')
        )
    
    # Get the gene metadata documents that match the provided gene names
    gene_metadata_list = []
    for gene_name in gene_names_list:
        doc_ref = gene_metadata_ref.document(gene_name)
        doc = doc_ref.get()
        if doc.exists:
            gene_metadata_list.append(doc.to_dict())
    
    # gene_ids = [doc.id for doc in gene_metadata_ref.stream()]
    return gene_metadata_list


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


def get_gene_metadata_by_gene_id_fs(gene_names: str, table: str):
    """Returns filtered list of gene metadata, if gene_name in gene_names.

    Args:
        gene_names (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str): Table in format 'ExperimentID_TissueID_DataType'

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    gene_names_list = gene_names.split(',')
    experiment_id, tissue_id, data_type = table.split('_')
    gene_metadata_ref = (
        fs.collection(u'experiments').document(experiment_id)
        .collection(u'tissues').document(tissue_id)
        .collection(u'gene_expression')
        )
    
    # Get the gene metadata documents that match the provided gene names
    gene_metadata_list = []
    for gene_name in gene_names_list:
        doc_ref = gene_metadata_ref.document(gene_name)
        doc = doc_ref.get()
        if doc.exists:
            gene_metadata_list.append(doc.to_dict())
    
    # gene_ids = [doc.id for doc in gene_metadata_ref.stream()]
    return gene_metadata_list


def get_gene_metadata_by_chr(chrs: str, table: str):
    """Returns filtered list of gene metadata, if chr in chrs.

    Args:
        chrs (str): List of chromosomes in string format e.g. chr7,chr8
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    chr_strs = ",".join([f"'{chr}'" for chr in chrs.split(",")])
    QUERY = f"""SELECT * FROM `{db}.{table}` 
                WHERE chr IN ({chr_strs})"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)
