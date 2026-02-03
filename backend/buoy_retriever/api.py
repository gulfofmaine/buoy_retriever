from datasets.api import dataset_router, config_router
from ninja import NinjaAPI
from pipelines.api import router as pipelines_router

api = NinjaAPI(docs_url="/docs/")

api.add_router("/configs/", config_router)
api.add_router("/datasets/", dataset_router)
api.add_router("/pipelines/", pipelines_router)
