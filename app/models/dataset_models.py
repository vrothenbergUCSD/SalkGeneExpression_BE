# pylint: disable=no-name-in-module
# pylint: disable=no-self-argument
from fastapi import File, UploadFile, Form
from pydantic import BaseModel, root_validator
from typing import List, Union, Literal, Type
import inspect


def root_unique_validator(field):
    def validator(cls, values):
        root_values = values.get("__root__")
        value_set = set()
        for value in root_values:
            if value[field] in value_set:
                raise ValueError(f"Duplicate {field}")
            else:
                value_set.add(value[field])
        return values

    return root_validator(pre=True, allow_reuse=True)(validator)


def as_form(cls: Type[BaseModel]):
    """
    Adds an as_form class method to decorated models. The as_form class method
    can be used with FastAPI endpoints
    """
    new_params = [
        inspect.Parameter(
            field.alias,
            inspect.Parameter.POSITIONAL_ONLY,
            default=(Form(field.default) if not field.required else Form(...)),
        )
        for field in cls.__fields__.values()
    ]

    async def _as_form(**data):
        return cls(**data)

    sig = inspect.signature(_as_form)
    sig = sig.replace(parameters=new_params)
    _as_form.__signature__ = sig
    setattr(cls, "as_form", _as_form)
    return cls


def form_body(cls):
    cls.__signature__ = cls.__signature__.replace(
        parameters=[
            arg.replace(default=Form(...))
            for arg in cls.__signature__.parameters.values()
        ]
    )
    return cls


# @form_body
@as_form
class DatasetMetadata(BaseModel):
    owner: str
    experiment: str
    institution: Union[str, None]
    species: str
    tissue: str
    year: int
    doi: Union[str, None]
    otherInformation: Union[str, None]
    permittedUsers: Union[List[str], None]
    permissionGroups: Union[List[str], None]
    gene_metadata_table_name: str
    sample_metadata_table_name: str
    gene_expression_data_table_name: str
    admin_groups: Union[List[str], None]
    editor_groups: Union[List[str], None]
    reader_groups: Union[List[str], None]
    gender: Union[str, None]
    condition: Union[str, None]
    valid: bool


class GeneMetadata(BaseModel):
    gene_id: str
    gene_name: str
    refseq: str
    chr: str
    start: int
    end: int
    strand: Union[Literal["+"], Literal["-"]]
    length: float
    description: str
    ensembl_gene_id: str
    gene_biotype: str
    copies: Union[int, None]
    annotation_divergence: Union[str, None]
    external_gene_name: Union[str, None]
    ensembl_peptide_id: Union[str, None]


class GeneMetadataTable(BaseModel):
    __root__: List[GeneMetadata]

    _validate_unique_gene_id = root_unique_validator("gene_id")


class SampleMetadata(BaseModel):
    sample_name: str
    species: str
    time_point: str
    group_name: str
    condition: str
    age_years: float
    age_months: int
    gender: str
    tissue: str
    number_of_replicates: int
    data_type: str


class SampleMetadataTable(BaseModel):
    __root__: List[SampleMetadata]
    _validate_unique_sample_name = root_unique_validator("sample_name")


class GeneExpressionData(BaseModel):
    gene_id: str
    sample_name: str
    gene_expression: float


class GeneExpressionDataTable(BaseModel):
    __root__: List[GeneExpressionData]

    @root_validator(pre=True)
    def unique_gene_id_sample_name(cls, values):
        root_values = values.get("__root__")
        value_set = set()
        for value in root_values:
            gene_id_sample_name = value["gene_id"] + "_" + value["sample_name"]
            if gene_id_sample_name in value_set:
                raise ValueError("Duplicate gene_id_sample_name")
            else:
                value_set.add(gene_id_sample_name)
        return values


class Dataset(BaseModel):
    metadata: DatasetMetadata
    gene_metadata_table: GeneMetadataTable
    sample_metadata: SampleMetadataTable
    gene_expression_data_table: GeneExpressionDataTable


# class DatasetReq(BaseModel):
#     # metadata: DatasetMetadata
#     files: List[UploadFile] = File(...)
