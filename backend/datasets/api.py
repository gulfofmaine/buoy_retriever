from django.http import HttpRequest
from django.shortcuts import get_object_or_404
from ninja import ModelSchema, PatchDict, Router

from pipelines.api import pipeline_api_key_auth
from pipelines.models import Pipeline

from .models import SimplifiedDataset

router = Router()


class DatasetSchema(ModelSchema):
    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "pipeline", "config", "created", "edited"]


class DatasetPostSchema(ModelSchema):
    pipeline_id: int

    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "config"]


@router.get("/", response=list[DatasetSchema])
def list_datasets(request):
    """List all datasets"""
    return SimplifiedDataset.objects.all()


@router.post("/", response=DatasetSchema)
def create_dataset(request, payload: DatasetPostSchema):
    """Create a new dataset"""
    pipeline = get_object_or_404(Pipeline, id=payload.pipeline_id)
    data = payload.dict()
    del data["pipeline_id"]
    dataset = SimplifiedDataset(**data, pipeline=pipeline)
    dataset.save()
    return dataset


@router.get("/{slug}", response=DatasetSchema)
def get_dataset(request, slug: str):
    """Get a specific dataset by slug"""
    return SimplifiedDataset.objects.get(slug=slug)


@router.patch("/{slug}", response=DatasetSchema)
def patch_dataset(request, slug: str, payload: PatchDict[DatasetPostSchema]):
    """Update a specific dataset by slug"""
    print(f"Payload: {payload}")
    dataset = SimplifiedDataset.objects.get(slug=slug)
    for attr, value in payload.items():
        setattr(dataset, attr, value)
    dataset.save()
    print(f"Updated dataset: {dataset}")
    return dataset


@router.get(
    "/by-pipeline/{pipeline_slug}",
    response=list[DatasetSchema],
    auth=pipeline_api_key_auth,
)
def get_datasets_by_pipeline(request: HttpRequest, pipeline_slug: str):
    """Get all datasets for a specific pipeline"""
    return SimplifiedDataset.objects.filter(pipeline__slug=pipeline_slug)
