from sqlalchemy.orm import Session


def get_gene_metadata(limit: str, table: str, db: Session):
    """Returns unfiltered list of gene metadata.

    Args:
        limit (str): Maximum number of rows to return
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    statement = f"SELECT * FROM {table} LIMIT {limit}"
    return db.execute(statement).all()


def get_gene_metadata_by_gene_name(gene_names: str, table: str, db: Session):
    """Returns filtered list of gene metadata, if gene_name in gene_names.

    Args:
        gene_names (str): List of gene names in string format e.g. Alb,Serpina3k
        table (str): Name of table in database
        db (Session): Session instance of database

    Returns:
        list: List of gene metadata JSON row objects from database
    """
    gene_name_strs = ",".join([f"'{gene_name}'" for gene_name in gene_names.split(",")])
    statement = f"SELECT * FROM {table} WHERE gene_name IN ({gene_name_strs})"
    return db.execute(statement).all()


def get_gene_metadata_by_chr(chrs: str, limit: int, table: str, db: Session):
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
    statement = "SELECT * FROM {table} WHERE chr IN ({chr_strs}) LIMIT {limit}"
    return db.execute(statement).all()
