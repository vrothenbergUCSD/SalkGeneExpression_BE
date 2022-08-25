from fastapi import APIRouter, Depends

# from sqlalchemy.orm import Session

import app.fetch.gene_metadata

# from dependencies import get_db

router = APIRouter()


@router.get("/gene_metadata/all_names")
async def get_gene_metadata_all_names(
    limit: int = 50000, table: str = "Mouse_TRF_2018_Liver_gene_metadata"
):
    """Returns unfiltered list of gene names.

    Args:
        limit (int, optional): _description_. Defaults to 50000.
        table (str, optional): _description_. Defaults to "Mouse_TRF_2018_Liver_gene_metadata".

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_all_names(limit, table)


@router.get("/gene_metadata/data")
async def get_gene_metadata(
    limit: int = 100,
    table: str = "Mouse_TRF_2018_Liver_gene_metadata",
):
    """Returns unfiltered list of gene metadata.

    Args:
        limit (int, optional): Maximum number of rows to return.
            Defaults to 100.
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_metadata'.

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata(limit, table)


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
    limit: int = 100,
    table: str = "Mouse_TRF_2018_Liver_gene_metadata",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of gene metadata, if chr in chrs.

    Args:
        chr (str): List of chromosomes in string format e.g. chr7,chr8
        limit (int, optional): Maximum number of rows to return.
            Defaults to 100.
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_metadata'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    return app.fetch.gene_metadata.get_gene_metadata_by_chr(chr, limit, table)
