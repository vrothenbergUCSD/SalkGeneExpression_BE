from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

# import models

# from database import SessionLocal, engine

# from dependencies import get_db

# import routers.gene_expression_routes as gene_expression_routes
# import routers.gene_metadata_routes as gene_metadata_routes
# import routers.sample_metadata_routes as sample_metadata_routes

from routers import gene_expression_routes, gene_metadata_routes, sample_metadata_routes

# models.Base.metadata.create_all(bind=engine)

# app = FastAPI(dependencies=[Depends(get_db)])
app = FastAPI()

origins = [
    # "http://localhost.tiangolo.com",
    # "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:4173",
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
