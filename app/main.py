from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import (
    gene_expression_routes,
    gene_metadata_routes,
    sample_metadata_routes,
)

app = FastAPI()


origins = [
    # "*",
    "https://rbio-p-datasharing.web.app",
    "https://rbio-p-datasharing.web.app/*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return {"message": "Health Check Passed! Test.."}


app.include_router(gene_expression_routes.router)
app.include_router(gene_metadata_routes.router)
app.include_router(sample_metadata_routes.router)
