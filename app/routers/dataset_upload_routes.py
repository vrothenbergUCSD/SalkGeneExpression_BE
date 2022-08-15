from fastapi import APIRouter, File, UploadFile, Form, Depends

from typing import List

import app.post.upload
from app.models.dataset_models import DatasetMetadata

router = APIRouter()


@router.post("/upload/dataset")
async def post_dataset(
    metadata: DatasetMetadata = Depends(DatasetMetadata.as_form),
    gene_metadata_filename: str = Form(),
    sample_metadata_filename: str = Form(),
    gene_expression_data_filename: str = Form(),
    files: List[UploadFile] = File(None),
):
    """Uploads entire dataset and its associated metadata to BigQuery.

    Args:
        metadata (DatasetMetadata): _description_. Defaults to Depends(DatasetMetadata.as_form).
        gene_metadata_filename (str): _description_. Defaults to Form().
        sample_metadata_filename (str): _description_. Defaults to Form().
        gene_expression_data_filename (str): _description_. Defaults to Form().
        files (List[UploadFile]): List of CSV file objects. Defaults to File(None).

    Returns:
        _type_: _description_
    """
    print("/upload/dataset: post_dataset")

    gene_metadata_file = [
        file for file in files if file.filename == gene_metadata_filename
    ][0]
    sample_metadata_file = [
        file for file in files if file.filename == sample_metadata_filename
    ][0]
    gene_expression_data_file = [
        file for file in files if file.filename == gene_expression_data_filename
    ][0]

    return await app.post.upload.post_dataset(
        metadata=metadata,
        gene_metadata_file=gene_metadata_file,
        sample_metadata_file=sample_metadata_file,
        gene_expression_data_file=gene_expression_data_file,
    )
