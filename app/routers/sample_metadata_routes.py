from fastapi import APIRouter

import app.fetch.sample_metadata


router = APIRouter()


@router.get("/sample_metadata/data")
async def get_sample_metadata(
    table: str = "Mouse_TRF_2018_Liver_sample_metadata",
    # db: Session = Depends(get_db),
):
    """Returns unfiltered list of sample metadata.

    Args:
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_sample_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.sample_metadata.get_sample_metadata(table)


@router.get("/sample_metadata/data_fs")
async def get_sample_metadata_fs(
    table: str = "TRF Experiment 2018_adrenal_sample",
    # db: Session = Depends(get_db),
):
    """Returns unfiltered list of sample metadata.

    Args:
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_sample_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.sample_metadata.get_sample_metadata_fs(table)


@router.get("/sample_metadata/sample_name")
async def get_sample_metadata_by_sample_name(
    sample_name: str,
    table: str = "Mouse_TRF_2018_Liver_sample_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of sample metadata, if sample_name in sample_names.

    Args:
        sample_name (str): List of sample names in string format
            e.g. ALF_ZT0-1,TRF_ZT10-2
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_sample_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.sample_metadata.get_sample_metadata_by_sample_name(
        sample_name, table
    )


@router.get("/sample_metadata/group_name")
async def get_sample_metadata_by_group_name(
    group_name: str,
    table: str = "Mouse_TRF_2018_Liver_sample_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of sample metadata, if group_name in group_names.

    Args:
        group_name (str): List of group names in string format e.g. ALF
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_sample_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.sample_metadata.get_sample_metadata_by_group_name(
        group_name, table
    )


@router.get("/sample_metadata/time_point")
async def get_sample_metadata_by_time_point(
    time_point: str = "0",
    table: str = "Mouse_TRF_2018_Liver_sample_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of filtered sample metadata, if time_point
        in time_points.

    Args:
        time_point (str, optional): List of time points in string format
            e.g. 0,2,4. Defaults to "0".
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_sample_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.sample_metadata.get_sample_metadata_by_time_point(
        time_point, table
    )


@router.get("/sample_metadata/gender")
async def get_sample_metadata_by_gender(
    gender: str,
    table: str = "Mouse_TRF_2018_Liver_sample_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of filtered sample metadata, if gender in genders.

    Args:
        gender (str): List of genders in string format e.g. Male
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_sample_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database.
    """
    return app.fetch.sample_metadata.get_sample_metadata_by_gender(gender, table)


@router.get("/sample_metadata/tissue")
async def get_sample_metadata_by_tissue(
    tissue: str,
    table: str = "Mouse_TRF_2018_Liver_sample_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of filtered sample metadata, if tissue in tissues

    Args:
        tissue (str): List of tissues in string format e.g. Liver
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_sample_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return app.fetch.sample_metadata.get_sample_metadata_by_tissue(tissue, table)


@router.get("/sample_metadata/genders_conditions")
async def get_sample_metadata_by_genders_conditions(
    genders: str = "",
    conditions: str = "",
    table: str = "TRF_2018_Mouse_EWAT_gene_expression_data_UCb0eBc2ewPjv9ipwLaEUYSwdhh1",
):
    """Returns filtered list of gene expression data based on:
        gene_expression_data table gene_id in gene_names,
        sample_metadata table gender in genders,
        sample_metadata table group_name in conditions,

    Args:
        genders (str, optional): List of genders in string format
            e.g. Male,Female
        conditions (str, optional): List of group names in string format
            e.g. ALF,TRF
        table (str): Name of table in database

    Returns:
        list: List of gene expression JSON row objects from database
    """
    return app.fetch.sample_metadata.get_sample_metadata_by_genders_conditions(
        genders, conditions, table
    )
