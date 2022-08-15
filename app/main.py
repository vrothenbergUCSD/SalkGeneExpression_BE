# import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# from fastapi_utils.timing import add_timing_middleware


# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

from app.routers import (
    gene_expression_routes,
    gene_metadata_routes,
    sample_metadata_routes,
    database_metadata_routes,
    dataset_upload_routes,
)

app = FastAPI()


origins = [
    "*",
    #     "rbio-p-datasharing.web.app",
    #     "*.rbio-p-datasharing.web.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# add_timing_middleware(app, record=logger.info, prefix="app", exclude="untimed")

app.include_router(gene_expression_routes.router)
app.include_router(gene_metadata_routes.router)
app.include_router(sample_metadata_routes.router)
app.include_router(database_metadata_routes.router)
app.include_router(dataset_upload_routes.router)
