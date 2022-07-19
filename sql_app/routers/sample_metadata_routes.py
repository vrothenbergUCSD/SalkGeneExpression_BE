from fastapi import APIRouter, Depends

from sqlalchemy.orm import Session

import fetch.sample_metadata

from dependencies import get_db

router = APIRouter()

@router.get("/sample_metadata/data")
async def get_sample_metadata(limit: int = 100, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    """Returns unfiltered list of sample metadata.

    Args:
        limit (int, optional): Maximum number of rows to return. Defaults to 100.
        table (str, optional): Name of table in database. Defaults to 'mouse_trf_2018_liver_sample_metadata'.
        db (Session, optional): Session instance of database. Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return fetch.sample_metadata.get_sample_metadata(limit, table, db)

@router.get("/sample_metadata/sample_name")
async def get_sample_metadata_by_sample_name(sample_name: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    """Returns filtered list of sample metadata, if sample_name in sample_names.

    Args:
        sample_name (str): List of sample names in string format e.g. ALF_ZT0-1,TRF_ZT10-2
        table (str, optional): Name of table in database. Defaults to 'mouse_trf_2018_liver_sample_metadata'.
        db (Session, optional): Session instance of database. Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return fetch.sample_metadata.get_sample_metadata_by_sample_name(sample_name, table, db)

@router.get("/sample_metadata/group_name")
async def get_sample_metadata_by_group_name(group_name: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    """Returns filtered list of sample metadata, if group_name in group_names.

    Args:
        group_name (str): List of group names in string format e.g. ALF
        table (str, optional): Name of table in database. Defaults to 'mouse_trf_2018_liver_sample_metadata'.
        db (Session, optional): Session instance of database. Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return fetch.sample_metadata.get_sample_metadata_by_group_name(group_name, table, db)

@router.get("/sample_metadata/time_point")
async def get_sample_metadata_by_time_point(time_point: str = "0", table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    """Returns filtered list of filtered sample metadata, if time_point in time_points.

    Args:
        time_point (str, optional): List of time points in string format e.g. 0,2,4. Defaults to "0".
        table (str, optional): Name of table in database. Defaults to 'mouse_trf_2018_liver_sample_metadata'.
        db (Session, optional): Session instance of database. Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return fetch.sample_metadata.get_sample_metadata_by_time_point(time_point, table, db)

@router.get("/sample_metadata/gender")
async def get_sample_metadata_by_gender(gender: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    """Returns filtered list of filtered sample metadata, if gender in genders.

    Args:
        gender (str): List of genders in string format e.g. Male
        table (str, optional): Name of table in database. Defaults to 'mouse_trf_2018_liver_sample_metadata'.
        db (Session, optional): Session instance of database. Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database.
    """
    return fetch.sample_metadata.get_sample_metadata_by_gender(gender, table, db)

@router.get("/sample_metadata/tissue")
async def get_sample_metadata_by_tissue(tissue: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    """Returns filtered list of filtered sample metadata, if tissue in tissues

    Args:
        tissue (str): List of tissues in string format e.g. Liver
        table (str, optional): Name of table in database. Defaults to 'mouse_trf_2018_liver_sample_metadata'.
        db (Session, optional): Session instance of database. Defaults to Depends(get_db).

    Returns:
        list: List of sample metadata JSON row objects from database
    """
    return fetch.sample_metadata.get_sample_metadata_by_tissue(tissue, table, db)

