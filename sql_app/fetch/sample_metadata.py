from sqlalchemy.orm import Session


def get_sample_metadata(limit: int, table: str, db: Session):
    """Returns unfiltered list of sample metadata.

    Args:
        limit (int): Maximum number of rows to return
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    statement = f"SELECT * FROM {table} LIMIT {limit}"
    return db.execute(statement).all()


def get_sample_metadata_by_sample_name(sample_names: str, table: str, db: Session):
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
    statement = f"SELECT * FROM {table} WHERE sample_name IN ({sample_name_strs})"
    return db.execute(statement).all()


def get_sample_metadata_by_group_name(group_names: str, table: str, db: Session):
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
    statement = f"SELECT * FROM {table} WHERE group_name IN ({group_name_strs})"
    return db.execute(statement).all()


def get_sample_metadata_by_time_point(time_points: str, table: str, db: Session):
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
    statement = f"SELECT * FROM {table} WHERE time_point IN ({time_point_strs})"
    return db.execute(statement).all()


def get_sample_metadata_by_gender(genders: str, table: str, db: Session):
    """Returns filtered list of filtered sample metadata, if gender in genders.

    Args:
        genders (str): List of genders in string format e.g. Male
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    gender_strs = ",".join([f"'{gender}'" for gender in genders.split(",")])
    statement = f"SELECT * FROM {table} WHERE gender IN ({gender_strs})"
    return db.execute(statement).all()


def get_sample_metadata_by_tissue(tissues: str, table: str, db: Session):
    """Returns filtered list of filtered sample metadata, if tissue in tissues

    Args:
        tissues (str): List of tissues in string format e.g. Liver
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    tissue_strs = ",".join([f"'{tissue}'" for tissue in tissues.split(",")])
    statement = f"SELECT * FROM {table} WHERE tissue IN ({tissue_strs})"
    return db.execute(statement).all()
