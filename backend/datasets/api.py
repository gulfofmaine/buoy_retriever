from django.http import HttpRequest
from django.shortcuts import get_object_or_404
from guardian.core import ObjectPermissionChecker
from guardian.shortcuts import get_objects_for_user
from ninja import ModelSchema, PatchDict, Router
from ninja.security import django_auth

from pipelines.api import pipeline_api_key_auth
from pipelines.models import Pipeline

from .models import SimplifiedDataset, Dataset, DatasetConfig

router = Router()


class DatasetCompactSchema(ModelSchema):
    user_can_edit: bool
    user_can_publish: bool

    class Meta:
        model = Dataset
        fields = ["slug", "state", "created", "edited"]


class DatasetCreateSchema(ModelSchema):
    pipeline_id: int

    class Meta:
        model = Dataset
        fields = ["slug"]


class DatasetSchema(ModelSchema):
    configs: list["DatasetConfigSchema"]

    class Meta:
        model = Dataset
        fields = ["id", "slug", "pipeline", "state", "created", "edited"]


class DatasetConfigSchema(ModelSchema):
    class Meta:
        model = DatasetConfig
        fields = ["config", "created", "edited", "state", "id"]


class SimplifiedDatasetSchema(ModelSchema):
    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "pipeline", "config", "created", "edited"]


class DatasetPostSchema(ModelSchema):
    pipeline_id: int

    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "config"]


@router.get("/", response=list[DatasetCompactSchema], auth=django_auth)
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


@router.post(
    "/",
    response=DatasetSchema,
    auth=django_auth,
)
def create_dataset(request: HttpRequest, payload: DatasetCreateSchema):
    """Create a new dataset"""
    pipeline = get_object_or_404(Pipeline, id=payload.pipeline_id)
    user = request.user
    print(f"Creating dataset {payload.slug} for pipeline: {pipeline} by user: {user}")

    data = payload.dict()
    del data["pipeline_id"]

    dataset = Dataset(**data, pipeline=pipeline)
    dataset.save()
    dataset.assign_edit_permission(user)
    config = DatasetConfig(dataset=dataset)
    config.save()
    return dataset


@router.get("/{slug}/", response=DatasetSchema, auth=django_auth)
def get_dataset(request: HttpRequest, slug: str):
    """Get a specific dataset by slug"""
    dataset = Dataset.objects.prefetch_related("configs").get(slug=slug)

    return dataset


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
