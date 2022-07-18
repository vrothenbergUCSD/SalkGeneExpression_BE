from sqlalchemy.orm import Session

import models, schemas

def get_samples_metadata_all(db: Session):
    return db.query(models.Sample).all()

def get_sample_metadata_by_sample_name(db: Session, sample_name: str):
    return db.query(models.Sample).filter(models.Sample.sample_name == sample_name).first()

def get_samples_metadata_by_group_name(db: Session, group_name: str, skip: int = 0, limit: int = 100):
    return db.query(models.Sample).filter(models.Sample.group_name == group_name).offset(skip).limit(limit).all()

def get_samples_metadata_by_time_point(db: Session, time_point: str, skip: int = 0, limit: int = 100):
    return db.query(models.Sample).filter(models.Sample.time_point == time_point).offset(skip).limit(limit).all()

def get_samples_metadata_by_gender(db: Session, gender: str, skip: int = 0, limit: int = 100):
    return db.query(models.Sample).filter(models.Sample.gender == gender).offset(skip).limit(limit).all()

def get_samples_metadata_by_tissue(db: Session, tissue: str, skip: int = 0, limit: int = 100):
    return db.query(models.Sample).filter(models.Sample.tissue == tissue).offset(skip).limit(limit).all()

def get_gene_metadata(db: Session, limit: int = 100000, skip: int = 1):
    return db.query(models.Gene).offset(skip).limit(limit).all()

def get_gene_metadata_by_gene_name(db: Session, gene_name: str):
    return db.query(models.Gene).filter(models.Gene.gene_name == gene_name).first()

def get_genes_metadata_by_chr(db: Session, chr: str, skip: int = 0, limit: int = 100):
    return db.query(models.Gene).filter(models.Gene.chr == chr).offset(skip).limit(limit).all()

def get_expression_data_by_gene_name(db: Session, gene_name: str, skip: int = 0, limit: int = 100):
    return db.query(models.Expression).filter(models.Expression.gene_name == gene_name).offset(skip).limit(limit).all()

def get_expression_data_by_sample_name(db: Session, sample_name: str, skip: int = 0, limit: int = 100):
    return db.query(models.Expression).filter(models.Expression.sample_name == sample_name).offset(skip).limit(limit).all()

def get_expression_data_by_gene_expression(db: Session, gene_expression: float, compare: int = 1, skip: int = 0, limit: int = 100):
    '''
    Returns list of expression data where the gene expression value is greater than the provided value if compare is set to 1.
    If compare set to 0, returns expression data less than the value.  
    '''
    if compare:
        return db.query(models.Expression).filter(models.Expression.gene_expression > gene_expression).offset(skip).limit(limit).all()
    else:
        return db.query(models.Expression).filter(models.Expression.gene_expression < gene_expression).offset(skip).limit(limit).all()

def get_expression_data(db: Session, hi: float, lo: float, skip: int = 0, limit: int = 100):
    '''
    Returns list of expression data where the gene expression value is greater than the provided value if compare is set to 1.
    If compare set to 0, returns expression data less than the value.  
    '''
    return db.query(models.Expression).filter(
            models.Expression.gene_expression > lo).filter(
            models.Expression.gene_expression < hi).offset(skip).limit(limit).all()

def get_expression_data_by_genes(db: Session, arr: list, hi: float, lo: float, skip: int = 0, limit: int = 100000):
    '''
    Returns list of expression data where the gene name is in comma separated list of genes.
    '''
    return db.query(models.Expression).filter(models.Expression.gene_name.in_(arr)).offset(skip).limit(limit).all()