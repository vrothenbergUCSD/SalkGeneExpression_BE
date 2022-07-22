from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship

from database import Base

class Expression(Base):
    __tablename__ = "gene_sample_data"

    id = Column(Integer, primary_key=True, index=True) # Needed?
    gene_name = Column(String, index=True)
    sample_name = Column(String, index=True)
    gene_expression = Column(Float)

    def to_dict(self):
        return {
            'gene_name' : self.gene_name,
            'sample_name' : self.sample_name,
            'gene_expression' : self.gene_expression,
        }

class Sample(Base):
    __tablename__ = "sample_metadata"

    id = Column(Integer, primary_key=True, index=True) # Needed?
    sample_name = Column(String, index=True)
    group_name = Column(String, index=True)
    time_point = Column(String, index=True) 
    organism = Column(String, index=True) 
    type_of_data = Column(String, index=True) 
    age = Column(String, index=True)
    gender = Column(String, index=True) 
    tissue = Column(String, index=True) 
    number_of_replicates = Column(Integer) # int

class Gene(Base):
    __tablename__ = "gene_metadata"

    id = Column(Integer, primary_key=True, index=True) # Needed?
    gene_name = Column(String, index=True)
    refseq = Column(String, index=True)
    chr = Column(String, index=True)
    start = Column(Integer, index=True)
    end = Column(Integer, index=True)
    strand = Column(String, index=True)
    length = Column(Integer, index=True)
    copies = Column(Integer, index=True)
    annotation_divergence = Column(String, index=True)
    ensembl_gene_id = Column(String, index=True)
    description = Column(String, index=True)
    external_gene_name = Column(String, index=True)
    gene_biotype = Column(String, index=True)
    ensembl_peptide_id = Column(String, index=True)



    

