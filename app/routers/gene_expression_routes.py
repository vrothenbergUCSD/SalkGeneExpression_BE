from fastapi import APIRouter, Depends

# from sqlalchemy.orm import Session

import fetch.gene_expression

# from dependencies import get_db

router = APIRouter()


@router.get("/gene_expression/range")
async def get_gene_expression_data_by_range(
    hi: float = 100000,
    lo: float = 0,
    skip: int = 0,
    limit: int = 100,
    table: str = "Mouse_TRF_2018_Liver_gene_sample_data",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of gene expression data, if averaged expression
        value across replicates is between hi and lo.

    Args:
        hi (float): Upper bound of average gene expression value.
            Defaults to 100000.
        lo (float): Lower bound of average gene expression value.
            Defaults to 0.
        skip (int, optional): Number of rows to skip.
            Defaults to 0.
        limit (int, optional): Maximum number of rows to return.
            Defaults to 100.
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_sample_data'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of gene expression JSON row objects from database
    """
    return fetch.gene_expression.get_gene_expression_data_by_range(
        hi, lo, skip, limit, table
    )


@router.get("/gene_expression/gene_name")
async def get_gene_expression_data_by_gene_name(
    gene_name: str,
    table: str = "Mouse_TRF_2018_Liver_gene_sample_data",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of gene expression data, if gene_name in gene_names.

    Args:
        gene_name (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_sample_data'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of gene expression JSON row objects from database
    """
    return fetch.gene_expression.get_gene_expression_data_by_gene_name(gene_name, table)


@router.get("/gene_expression/sample_name")
async def get_gene_expression_data_by_sample_name(
    sample_name: str,
    table: str = "Mouse_TRF_2018_Liver_gene_sample_data",
    # db: Session = Depends(get_db),
):
    """Returns filtered list of gene expression data, if sample_name in sample_names.

    Args:
        sample_name (str): List of sample names in string format
            e.g. ALF_ZT0-1,TRF_ZT10-2
        table (str, optional): Name of table in database.
            Defaults to 'Mouse_TRF_2018_Liver_gene_sample_data'.
        db (Session, optional): Session instance of database.
            Defaults to Depends(get_db).

    Returns:
        list: List of gene expression JSON row objects from database
    """
    return fetch.gene_expression.get_gene_expression_data_by_sample_name(
        sample_name, table
    )
