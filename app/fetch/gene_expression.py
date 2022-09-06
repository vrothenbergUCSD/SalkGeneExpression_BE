# from sqlalchemy.orm import Session
from google.cloud import bigquery

client = bigquery.Client()

db = "rbio-p-datasharing.gene_expression_database"


def get_gene_expression_data_by_range(
    hi: int, lo: int, skip: int, limit: int, table: str
):
    """Returns filtered list of gene expression data, if averaged expression
        value across replicates is between hi and lo.

    Args:
        hi (int): Upper bound of average gene expression value
        lo (int): Lower bound of average gene expression value
        skip (int): Number of rows to skip
        limit (int): Maximum number of rows to return
        table (str): Name of table in database

    Returns:
        list: List of gene expression JSON row objects from database
    """
    QUERY = f"""SELECT gene_name, group_name, time_point, 
    AVG(gene_expression) AS gene_expression FROM `{db}.{table}` GROUP BY gene_name, 
    group_name, time_point HAVING AVG(gene_expression) > {lo} AND
    AVG(gene_expression) < {hi} LIMIT {limit}"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_expression_data_by_gene_names(gene_names: str, table: str):
    """Returns filtered list of gene expression data, if gene_name in gene_names.

    Args:
        gene_names (str): List of gene names in string format
            e.g. Alb,Serpina3k
        table (str): Name of table in database

    Returns:
        list: List of gene expression JSON row objects from database
    """
    gene_name_strs = ",".join([f"'{gene_name}'" for gene_name in gene_names.split(",")])
    QUERY = f"""SELECT * 
    FROM `{db}.{table}` 
    WHERE gene_id IN ({gene_name_strs})"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_expression_data_by_gene_id(gene_id: str, table: str):
    """Returns filtered list of gene expression data for one gene_id.

    Args:
        gene_id (str): Gene name in string format
            e.g. Alb
        table (str): Name of table in database

    Returns:
        list: List of gene expression JSON row objects from database
    """
    # gene_name_strs = ",".join([f"'{gene_name}'" for gene_name in gene_names.split(",")])
    QUERY = f"""SELECT *
    FROM `{db}.{table}`
    WHERE gene_id = {gene_id}"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_expression_data_by_sample_name(sample_names: str, table: str):
    """Returns filtered list of gene expression data, if sample_name in sample_names.

    Args:
        sample_names (str): List of sample names in string format
            e.g. ALF_ZT0-1,TRF_ZT10-2
        table (str): Name of table in database

    Returns:
        list: List of gene expression JSON row objects from database
    """
    sample_names_strs = ",".join(
        [f"'{sample_name}'" for sample_name in sample_names.split(",")]
    )
    QUERY = f"""SELECT gene_name, group_name, time_point, AVG(gene_expression) 
    AS gene_expression FROM `{db}.{table}` 
    WHERE sample_name IN ({sample_names_strs}) 
    GROUP BY gene_name, group_name, time_point"""
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_gene_expression_data_by_gene_names_genders_conditions(
    gene_names: str,
    genders: str = "",
    conditions: str = "",
    table: str = "TRF_2018_Mouse_EWAT_gene_expression_data_UCb0eBc2ewPjv9ipwLaEUYSwdhh1",
):
    """Returns filtered list of gene expression data based on:
        gene_expression_data table gene_id in gene_names,
        sample_metadata table gender in genders,
        sample_metadata table group_name in conditions,

    Args:
        gene_names (str): List of gene names in string format
            e.g. Alb,Serpina3k
        genders (str): List of genders in string format
            e.g. Male,Female
        conditions (str): List of group names in string format
            e.g. ALF,TRF
        table (str): Name of table in database

    Returns:
        list: List of gene expression JSON row objects from database
    """
    gene_expr = table
    sample_table = table.replace("gene_expression_data", "sample_metadata")
    gene_name_strs = ",".join([f"'{gene}'" for gene in gene_names.split(",")])
    gender_strs = ",".join([f"'{gender}'" for gender in genders.split(",")])
    condition_strs = ",".join([f"'{cond}'" for cond in conditions.split(",")])
    QUERY = f"""
        SELECT 
        gene_exp.gene_id, 
        gene_exp.sample_name, 
        gene_exp.gene_expression,
        
        FROM `{db}.{gene_expr}` as gene_exp
        RIGHT JOIN `{db}.{sample_table}` as samples
        ON samples.sample_name = gene_exp.sample_name
        WHERE gene_exp.gene_id IN ({gene_name_strs})
        """
    if len(conditions):
        QUERY += f""" AND samples.group_name IN ({condition_strs})"""
    if len(genders):
        QUERY += f""" AND samples.gender IN ({gender_strs})"""

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)
