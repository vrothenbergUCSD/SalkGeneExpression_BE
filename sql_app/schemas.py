from typing import List, Union
from pydantic import BaseModel


class Expression(BaseModel):
    gene_name: str
    sample_name: str
    gene_expression: float

    class Config:
       orm_mode = True
    
    def to_dict(self):
        return {
            'gene_name' : self.gene_name,
            'sample_name' : self.sample_name,
            'gene_expression' : self.gene_expression,
        }

class Sample(BaseModel):
    sample_name: str
    group_name: str
    time_point: str 
    organism: str 
    type_of_data: str 
    age: str
    gender: str 
    tissue: str 
    number_of_replicates: int 

    class Config:
       orm_mode = True

class Gene(BaseModel):
    gene_name: str
    refseq: str
    chr: str
    start: int
    end: int
    strand: str
    length: int
    copies: int
    annotation_divergence: str
    ensembl_gene_id: str
    description: str
    external_gene_name: str
    gene_biotype: str
    ensembl_peptide_id: str

    class Config:
       orm_mode = True
    

