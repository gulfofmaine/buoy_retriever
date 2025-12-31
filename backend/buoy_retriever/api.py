from datasets.api import router as datasets_router
from ninja import NinjaAPI
from pipelines.api import router as pipelines_router

api = NinjaAPI(docs_url="/docs/")

api.add_router("/datasets/", datasets_router)
api.add_router("/pipelines/", pipelines_router)
