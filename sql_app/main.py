from typing import List

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from yaml import parse

import crud, models, schemas, fetch
from database import SessionLocal, engine

import pandas as pd
from pydantic import parse_obj_as

import json

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

origins = [
    #"http://localhost.tiangolo.com",
    #"https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def root():
    return {"message" : "Hello world"}

@app.get("/sample_metadata/")
async def get_sample_metadata(limit: int = 100, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db) ):
    return fetch.get_sample_metadata(limit, table, db)

@app.get("/sample_metadata_by_sample_name/{sample_name}")
async def get_sample_metadata_by_sample_name(sample_name: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    return fetch.get_sample_metadata_by_sample_name(sample_name, table, db)

@app.get("/sample_metadata_by_group_name/{group_name}")
async def get_sample_metadata_by_group_name(group_name: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    return fetch.get_sample_metadata_by_group_name(group_name, table, db)

@app.get("/sample_metadata_by_time_point/{time_point}")
async def get_sample_metadata_by_time_point(time_point: str = "0", table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    return fetch.get_sample_metadata_by_time_point(time_point, table, db)

@app.get("/sample_metadata_by_gender/{gender}")
async def get_sample_metadata_by_gender(gender: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    return fetch.get_sample_metadata_by_gender(gender, table, db)

@app.get("/sample_metadata_by_tissue/{tissue}")
async def get_sample_metadata_by_tissue(tissue: str, table: str = 'mouse_trf_2018_liver_sample_metadata', db: Session = Depends(get_db)):
    return fetch.get_sample_metadata_by_tissue(tissue, table, db)

@app.get("/gene_metadata/")
async def get_gene_metadata(limit: int = 100, table: str = 'mouse_trf_2018_liver_gene_metadata', db: Session = Depends(get_db)):
    return fetch.get_gene_metadata(limit, table, db)

@app.get("/gene_metadata_by_gene_name/{gene_name}")
async def get_gene_metadata_by_gene_name(gene_name: str, table: str = 'mouse_trf_2018_liver_gene_metadata', db: Session = Depends(get_db)):
    return fetch.get_gene_metadata_by_gene_name(gene_name, table, db)

@app.get("/gene_metadata_by_chr/{chr}")
async def get_gene_metadata_by_chr(chr: str, limit: int = 100, table: str = 'mouse_trf_2018_liver_gene_metadata', db: Session = Depends(get_db)):
    return fetch.get_gene_metadata_by_chr(chr, limit, table, db)

@app.get("/gene_expression_data/")
async def get_expression_data(hi: float, lo: float, skip: int = 0, limit: int = 100, table: str = 'mouse_trf_2018_liver_gene_expression_data', db: Session = Depends(get_db)):
    return fetch.get_expression_data(hi, lo, skip, limit, table, db)

@app.get("/gene_expression_data_by_gene_name/{gene_name}")
async def get_expression_data_by_gene_name(gene_name: str, table: str = 'mouse_trf_2018_liver_gene_expression_data', db: Session = Depends(get_db)):
    return fetch.get_expression_data_by_gene_name(gene_name, table, db)

@app.get("/gene_expression_data_by_sample_name/{sample_name}")
async def get_expression_data_by_sample_name(sample_name: str, table: str = 'mouse_trf_2018_liver_gene_expression_data', db: Session = Depends(get_db)):
    return fetch.get_expression_data_by_sample_name(sample_name, table, db)

@app.get("/top_genes/")
async def get_top_genes(limit: int = 100, table: str = 'mouse_trf_2018_liver_top_genes', db: Session = Depends(get_db)):
    return fetch.get_top_genes(limit, table, db)