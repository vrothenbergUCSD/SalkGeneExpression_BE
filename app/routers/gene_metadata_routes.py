from fastapi import APIRouter, Depends

# from sqlalchemy.orm import Session

import app.fetch.gene_metadata

# from dependencies import get_db

router = APIRouter()


@router.get("/gene_metadata/all_names")
async def get_gene_metadata_all_names(
    table: str = "Mouse_TRF_2018_Liver_gene_metadata"
):
    """Returns unfiltered list of gene names.

    Args:
        table (str, optional): _description_. Defaults to "Mouse_TRF_2018_Liver_gene_metadata".

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_all_names(table)

@router.get("/gene_metadata/all_names_fs")
async def get_gene_metadata_all_names_fs(
    table: str = "Mouse_TRF_2018_Liver_gene_metadata"
):
    """Returns unfiltered list of gene names.

    Args:
        table (str, optional): _description_. Defaults to "Mouse_TRF_2018_Liver_gene_metadata".

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_all_names_fs(table)


@router.get("/gene_metadata/data")
async def get_gene_metadata(
    table: str = "Mouse_TRF_2018_Liver_gene_metadata",
):
    """Returns unfiltered list of gene metadata.

    Args:
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_metadata'.

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata(table)


@router.get("/gene_metadata/gene_name")
async def get_gene_metadata_by_gene_name(
    gene_name: str,
    table: str = "Mouse_TRF_2018_Liver_gene_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of gene metadata, if gene_name in gene_names.

    Args:
        gene_name (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_by_gene_name(gene_name, table)


@router.get("/gene_metadata/gene_name_fs")
async def get_gene_metadata_by_gene_name_fs(
    gene_name: str,
    table: str = "Mouse_TRF_2018_Liver_gene_metadata",
):
    """Returns filtered list of gene metadata, if gene_name in gene_names.

    Args:
        gene_name (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_by_gene_name_fs(gene_name, table)


@router.get("/gene_metadata/gene_id")
async def get_gene_metadata_by_gene_id(
    gene_id: str,
    table: str = "Mouse_TRF_2018_Liver_gene_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of gene metadata, if gene_id in gene_id string of ids.

    Args:
        gene_id (str): List of gene ids in string format e.g. Alb,Serpina3k
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_metadata'.

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_by_gene_name(gene_id, table)


@router.get("/gene_metadata/chr")
async def get_gene_metadata_by_chr(
    chr: str,
    table: str = "Mouse_TRF_2018_Liver_gene_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of gene metadata, if chr in chrs.

    Args:
        chr (str): List of chromosomes in string format e.g. chr7,chr8
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_by_chr(chr, table)
