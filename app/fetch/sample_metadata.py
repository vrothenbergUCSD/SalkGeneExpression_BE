from google.cloud import bigquery
from app.main import fs

client = bigquery.Client()

db = "rbio-p-datasharing.gene_expression_database"


def get_sample_metadata(table: str):
    """Returns unfiltered list of sample metadata.

    Args:
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    QUERY = f"SELECT * FROM `{db}.{table}`"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_sample_metadata_fs(table:str):
    """Returns unfiltered list of sample metadata from Firestore.

    Args:
        experiment_id (str): Name of experiment document in experiments collection
        tissue_id (str): Name of tissue document in tissues collection

    Returns:
        list: List of sample metadata JSON row objects from database
    """

    experiment_id, tissue_id, data_type = table.split('_')

    sample_metadata_ref = (
        fs.collection(u'experiments').document(experiment_id)
        .collection(u'tissues').document(tissue_id)
        .collection(u'sample_metadata')
        )
    
    sample_metadata_docs = sample_metadata_ref.stream()
    sample_metadata_list = [doc.to_dict() for doc in sample_metadata_docs]
    return sample_metadata_list



def get_sample_metadata_by_sample_name(sample_names: str, table: str):
    """Returns filtered list of sample metadata, if sample_name in sample_names.

    Args:
        sample_names (str): List of sample names in string format
            e.g. ALF_ZT0-1,TRF_ZT10-2
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    sample_name_strs = ",".join(
        [f"'{sample_name}'" for sample_name in sample_names.split(",")]
    )
    QUERY = f"SELECT * FROM `{db}.{table}` WHERE sample_name IN ({sample_name_strs})"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_sample_metadata_by_group_name(group_names: str, table: str):
    """Returns filtered list of sample metadata, if group_name in group_names

    Args:
        group_names (str): List of group names in string format e.g. ALF
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """

    group_name_strs = ",".join(
        [f"'{group_name}'" for group_name in group_names.split(",")]
    )
    QUERY = f"SELECT * FROM `{db}.{table}` WHERE group_name IN ({group_name_strs})"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_sample_metadata_by_time_point(time_points: str, table: str):
    """Returns filtered list of filtered sample metadata, if time_point in time_points

    Args:
        time_points (str): List of time points in string format e.g. 0,2,4
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    time_point_strs = ",".join(
        [f"'{time_point}'" for time_point in time_points.split(",")]
    )
    QUERY = f"SELECT * FROM `{db}.{table}` WHERE time_point IN ({time_point_strs})"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_sample_metadata_by_gender(genders: str, table: str):
    """Returns filtered list of filtered sample metadata, if gender in genders.

    Args:
        genders (str): List of genders in string format e.g. Male
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    gender_strs = ",".join([f"'{gender}'" for gender in genders.split(",")])
    QUERY = f"SELECT * FROM `{db}.{table}` WHERE gender IN ({gender_strs})"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_sample_metadata_by_tissue(tissues: str, table: str):
    """Returns filtered list of filtered sample metadata, if tissue in tissues

    Args:
        tissues (str): List of tissues in string format e.g. Liver
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    tissue_strs = ",".join([f"'{tissue}'" for tissue in tissues.split(",")])
    QUERY = f"SELECT * FROM `{db}.{table}` WHERE tissue IN ({tissue_strs})"
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)


def get_sample_metadata_by_genders_conditions(
    genders: str, conditions: str, table: str
):
    """Returns filtered list of filtered sample metadata, if gender in genders
        and condition in conditions

    Args:
        genders (str): List of genders in string format e.g. Male,Female
        conditions (str): List of conditions in string format e.g. ALF,TRF
        table (str): Name of table in database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    gender_strs = ",".join([f"'{gender}'" for gender in genders.split(",")])
    condition_strs = ",".join([f"'{cond}'" for cond in conditions.split(",")])
    QUERY = f"""SELECT * FROM `{db}.{table}` 
        WHERE gender IN ({gender_strs})
        AND condition IN ({condition_strs})"""

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    return list(rows)
