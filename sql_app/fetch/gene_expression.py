from sqlalchemy.orm import Session

def get_gene_expression_data_by_range(hi: int, lo: int, skip: int, limit: int, table: str, db: Session):
  """Returns filtered list of gene expression data, if averaged expression value across replicates is between hi and lo.

  Args:
      hi (int): Upper bound of average gene expression value
      lo (int): Lower bound of average gene expression value
      skip (int): Number of rows to skip
      limit (int): Maximum number of rows to return
      table (str): Name of table in database
      db (Session): Session instance of database

  Returns:
      list: List of gene expression JSON row objects from database
  """
  statement = f"SELECT gene_name, group_name, time_point, AVG(gene_expression) AS gene_expression FROM {table} GROUP BY gene_name, group_name, time_point HAVING AVG(gene_expression) > {lo} AND AVG(gene_expression) < {hi} LIMIT {limit}"
  return db.execute(statement).all()

def get_gene_expression_data_by_gene_name(gene_names: str, table: str, db: Session):
  """Returns filtered list of gene expression data, if gene_name in gene_names.

  Args:
      gene_names (str): List of gene names in string format e.g. Alb,Serpina3k
      table (str): Name of table in database
      db (Session): Session instance of database

  Returns:
      list: List of gene expression JSON row objects from database
  """
  gene_name_strs = ','.join([f'\'{gene_name}\'' for gene_name in gene_names.split(',')])
  statement = f"SELECT gene_name, group_name, time_point, AVG(gene_expression) AS gene_expression FROM {table} WHERE gene_name IN ({gene_name_strs}) GROUP BY gene_name, group_name, time_point"
  return db.execute(statement).all()

def get_gene_expression_data_by_sample_name(sample_names: str, table: str, db: Session):
  """Returns filtered list of gene expression data, if sample_name in sample_names.

  Args:
      sample_names (str): List of sample names in string format e.g. ALF_ZT0-1,TRF_ZT10-2
      table (str): Name of table in database
      db (Session): Session instance of database

  Returns:
      list: List of gene expression JSON row objects from database
  """
  sample_names_strs = ','.join([f'\'{sample_name}\'' for sample_name in sample_names.split(',')])
  statement = f"SELECT gene_name, group_name, time_point, AVG(gene_expression) AS gene_expression FROM {table} WHERE sample_name IN ({sample_names_strs}) GROUP BY gene_name, group_name, time_point"
  return db.execute(statement).all()

