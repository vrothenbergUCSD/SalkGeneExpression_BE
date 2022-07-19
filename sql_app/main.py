from typing import List

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from yaml import parse

# import fetch
import models

from database import SessionLocal, engine

from dependencies import get_db

# import routers.gene_expression_routes as gene_expression_routes
# import routers.gene_metadata_routes as gene_metadata_routes
# import routers.sample_metadata_routes as sample_metadata_routes

from routers import gene_expression_routes, gene_metadata_routes, sample_metadata_routes

models.Base.metadata.create_all(bind=engine)

app = FastAPI(dependencies=[Depends(get_db)])

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

app.include_router(gene_expression_routes.router)
app.include_router(gene_metadata_routes.router)
app.include_router(sample_metadata_routes.router)

# # Dependency
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# @app.get("/")
# def root():
#     return {"message" : "Hello world"}



# @app.get("/top_genes/")
# async def get_top_genes(limit: int = 100, table: str = 'mouse_trf_2018_liver_top_genes', db: Session = Depends(get_db)):
#     return fetch.get_top_genes(limit, table, db)