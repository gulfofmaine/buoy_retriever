from django.http import HttpRequest
from django.shortcuts import get_object_or_404
from guardian.core import ObjectPermissionChecker
from guardian.shortcuts import get_objects_for_user
from ninja import ModelSchema, PatchDict, Router
from ninja.security import django_auth

from pipelines.api import pipeline_api_key_auth
from pipelines.models import Pipeline

from .models import SimplifiedDataset, Dataset

router = Router()


class DatasetSchema(ModelSchema):
    user_can_edit: bool
    user_can_publish: bool

    class Meta:
        model = Dataset
        fields = ["slug", "state", "created", "edited"]


class SimplifiedDatasetSchema(ModelSchema):
    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "pipeline", "config", "created", "edited"]


class DatasetPostSchema(ModelSchema):
    pipeline_id: int

    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "config"]


@router.get(
    "/",
    response=list[DatasetSchema],
    auth=django_auth,
)
def list_datasets(request: HttpRequest):
    """List all datasets"""
    datasets = get_objects_for_user(
        request.user,
        "datasets.view_dataset",
    )
    checker = ObjectPermissionChecker(request.user)
    checker.prefetch_perms(datasets)
    return [
        {
            "slug": d.slug,
            "state": d.state,
            "created": d.created,
            "edited": d.edited,
            "user_can_edit": d.can_edit(request.user),
            "user_can_publish": d.can_publish(request.user),
        }
        for d in datasets
    ]


@router.post("/", response=SimplifiedDatasetSchema)
def create_dataset(request: HttpRequest, payload: DatasetPostSchema):
    """Create a new dataset"""
    pipeline = get_object_or_404(Pipeline, id=payload.pipeline_id)
    data = payload.dict()
    del data["pipeline_id"]
    dataset = SimplifiedDataset(**data, pipeline=pipeline)
    dataset.save()
    return dataset


@router.get("/{slug}", response=SimplifiedDatasetSchema)
def get_dataset(request: HttpRequest, slug: str):
    """Get a specific dataset by slug"""
    return SimplifiedDataset.objects.get(slug=slug)


@router.patch("/{slug}", response=SimplifiedDatasetSchema)
def patch_dataset(
    request: HttpRequest,
    slug: str,
    payload: PatchDict[DatasetPostSchema],
):
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
    response=list[SimplifiedDatasetSchema],
    auth=pipeline_api_key_auth,
)
def get_datasets_by_pipeline(request: HttpRequest, pipeline_slug: str):
    """Get all datasets for a specific pipeline"""
    return SimplifiedDataset.objects.filter(pipeline__slug=pipeline_slug)
