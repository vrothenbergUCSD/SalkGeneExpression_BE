def get_sample_metadata(limit, table, db):
  '''
  Input: table name

  Returns list of all sample metadata.
  '''
  statement = "SELECT * FROM {table} LIMIT {limit}".format(table=table, limit=limit)
  return db.execute(statement).all()

def get_sample_metadata_by_sample_name(sample_names, table, db):
  '''
  Input: List of sample_name strings, table name.  
  Ex: ALF_ZT0-1,TRF_ZT8-2

  Returns list of sample metadata. 
  '''
  sample_name_strs = ','.join(['\'{}\''.format(sample_name) for sample_name in sample_names.split(',')])
  statement = "SELECT * FROM {table} WHERE sample_name IN ({sample_name_strs})".format(table=table, sample_name_strs=sample_name_strs)
  return db.execute(statement).all()


def get_sample_metadata_by_group_name(group_names, table, db):
  '''
  Input: List of group_name strings, table name.
  Ex: ALF

  Returns list of sample metadata. 
  '''
  group_name_strs = ','.join(['\'{}\''.format(group_name) for group_name in group_names.split(',')])
  statement = "SELECT * FROM {table} WHERE group_name IN ({group_name_strs})".format(table=table, group_name_strs=group_name_strs)
  return db.execute(statement).all()

def get_sample_metadata_by_time_point(time_points, table, db):
  '''
  Input: List of time_point strings, table name.
  Ex: 0,2,4

  Returns list of sample metadata. 
  '''
  time_point_strs = ','.join(['\'{}\''.format(time_point) for time_point in time_points.split(',')])
  statement = "SELECT * FROM {table} WHERE time_point IN ({time_point_strs})".format(table=table, time_point_strs=time_point_strs)
  return db.execute(statement).all()

def get_sample_metadata_by_gender(genders, table, db):
  '''
  Input: List of gender strings, table name.
  Ex: Male

  Returns list of sample metadata. 
  '''
  gender_strs = ','.join(['\'{}\''.format(time_point) for time_point in genders.split(',')])
  statement = "SELECT * FROM {table} WHERE gender IN ({gender_strs})".format(table=table, gender_strs=gender_strs)
  return db.execute(statement).all()

def get_sample_metadata_by_tissue(tissues, table, db):
  '''
  Input: List of tissue strings, table name.
  Ex: Liver

  Returns list of sample metadata. 
  '''
  tissue_strs = ','.join(['\'{}\''.format(time_point) for time_point in tissues.split(',')])
  statement = "SELECT * FROM {table} WHERE tissue IN ({tissue_strs})".format(table=table, tissue_strs=tissue_strs)
  return db.execute(statement).all()

def get_gene_metadata(limit, table, db):
  '''
  Input: Limit number of genes to return, table name.
  Ex: 100

  Returns list of sample metadata. 
  '''
  statement = "SELECT * FROM {table} LIMIT {limit}".format(table=table, limit=limit)
  return db.execute(statement).all()

def get_gene_metadata_by_gene_name(gene_names, table, db):
  '''
  Input: List of gene_name strings, table name.  
  Ex: Alb,Serpina3k

  Returns list of sample metadata. 
  '''
  gene_name_strs = ','.join(['\'{}\''.format(gene_name) for gene_name in gene_names.split(',')])
  statement = "SELECT * FROM {table} WHERE gene_name IN ({gene_name_strs})".format(table=table, gene_name_strs=gene_name_strs)
  return db.execute(statement).all()

def get_gene_metadata_by_chr(chrs, limit, table, db):
  '''
  Input: List of chr strings, table name.  
  Ex: chr7,chr8

  Returns list of gene metadata. 
  '''
  chr_strs = ','.join(['\'{}\''.format(chr) for chr in chrs.split(',')])
  statement = "SELECT * FROM {table} WHERE chr IN ({chr_strs}) LIMIT {limit}".format(table=table, chr_strs=chr_strs, limit=limit)
  return db.execute(statement).all()

def get_expression_data_by_gene_name(gene_names, table, db):
  '''
  Input: List of gene_name strings, table name.  
  Ex: Alb,Serpina3k

  Returns list of gene expression data. 
  '''
  gene_name_strs = ','.join(['\'{}\''.format(gene_name) for gene_name in gene_names.split(',')])
  statement = "SELECT gene_name, group_name, time_point, AVG(gene_expression) FROM {table} WHERE gene_name IN ({genes_strs}) GROUP BY gene_name, group_name, time_point".format(table=table, gene_name_strs=gene_name_strs)
  return db.execute(statement).all()

def get_expression_data_by_sample_name(sample_names, table, db):
  '''
  Input: List of sample_name strings, table name.  
  Ex: ALF_ZT0-1,TRF_ZT8-2

  Returns list of gene expression data. 
  '''
  sample_names_strs = ','.join(['\'{}\''.format(sample_name) for sample_name in sample_names.split(',')])
  statement = "SELECT gene_name, group_name, time_point, AVG(gene_expression) FROM {table} WHERE sample_name IN ({sample_names_strs}) GROUP BY gene_name, group_name, time_point".format(table=table, sample_names_strs=sample_names_strs)
  return db.execute(statement).all()

def get_expression_data(hi, lo, skip, limit, table, db):
  '''
  Input: High and low thresholds for gene expression.  Skip offset value, limit number of entries to return, table name.  
  Ex: ?hi=30000&lo=10000

  Returns list of average gene expression data by gene_name, group_name and time_point. 
  '''
  statement = "SELECT gene_name, group_name, time_point, AVG(gene_expression) FROM {table} GROUP BY gene_name, group_name, time_point HAVING AVG(gene_expression) > {lo} AND AVG(gene_expression) < {hi} LIMIT {limit}".format(table=table, hi=hi, lo=lo, limit=limit, skip=skip)
  return db.execute(statement).all()


def get_top_genes(limit, table, db):
  '''
  Returns list of top genes by average expression.
  '''
  statement = 'SELECT * FROM {table} LIMIT {limit}'.format(table=table, limit=limit)
  return db.execute(statement).all()




  




