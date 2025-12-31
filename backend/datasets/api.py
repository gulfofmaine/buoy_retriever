from django.http import HttpRequest
from django.shortcuts import get_object_or_404
from guardian.core import ObjectPermissionChecker
from guardian.shortcuts import get_objects_for_user
from ninja import ModelSchema, PatchDict, Router, Schema
from ninja.security import django_auth

from pipelines.api import pipeline_api_key_auth
from pipelines.models import Pipeline

from .models import SimplifiedDataset, Dataset, DatasetConfig

dataset_router = Router()
config_router = Router()


class DatasetCompactSchema(ModelSchema):
    class Meta:
        model = Dataset
        fields = ["slug", "state", "created", "edited"]


# class DatasetCompactPermissionsSchema(DatasetCompactSchema):
#     user_can_edit: bool
#     user_can_publish: bool


class DatasetCompactPermissionsSchema(Schema):
    user_can_edit: bool
    user_can_publish: bool

    slug: str
    state: str
    # created
    # edited

    @staticmethod
    def resolve_user_can_edit(obj: Dataset, context) -> bool:
        user = context["request"].user
        return obj.can_edit(user)

    @staticmethod
    def resolve_user_can_publish(obj: Dataset, context) -> bool:
        user = context["request"].user
        return obj.can_publish(user)


class DatasetCreateSchema(ModelSchema):
    pipeline_id: int

    class Meta:
        model = Dataset
        fields = ["slug"]


class DatasetSchema(ModelSchema):
    configs: list["DatasetConfigCompactSchema"]

    class Meta:
        model = Dataset
        fields = ["id", "slug", "pipeline", "state", "created", "edited"]


class DatasetConfigCompactSchema(ModelSchema):
    class Meta:
        model = DatasetConfig
        fields = ["config", "created", "edited", "state", "id"]


class DatasetConfigSchema(ModelSchema):
    dataset: DatasetCompactSchema

    class Meta:
        model = DatasetConfig
        fields = ["id", "dataset", "config", "state", "created", "edited"]


class DatasetConfigPostSchema(ModelSchema):
    class Meta:
        model = DatasetConfig
        fields = ["config", "state"]


class SimplifiedDatasetSchema(ModelSchema):
    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "pipeline", "config", "created", "edited"]


class DatasetPostSchema(ModelSchema):
    pipeline_id: int

    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "config"]


@dataset_router.get(
    "/",
    response=list[DatasetCompactPermissionsSchema],
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
    # return [
    #     {
    #         "slug": d.slug,
    #         "state": d.state,
    #         "created": d.created,
    #         "edited": d.edited,
    #         "user_can_edit": d.can_edit(request.user),
    #         "user_can_publish": d.can_publish(request.user),
    #     }
    #     for d in datasets
    # ]
    return datasets


@dataset_router.post(
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


@dataset_router.get("/{slug}/", response=DatasetSchema, auth=django_auth)
def get_dataset(request: HttpRequest, slug: str):
    """Get a specific dataset by slug"""
    dataset = Dataset.objects.prefetch_related("configs").get(slug=slug)

    return dataset


@dataset_router.patch("/{slug}", response=SimplifiedDatasetSchema)
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


@dataset_router.get(
    "/by-pipeline/{pipeline_slug}/",
    response=list[DatasetSchema],
    auth=pipeline_api_key_auth,
)
def get_datasets_by_pipeline(request: HttpRequest, pipeline_slug: str):
    """Get all datasets for a specific pipeline"""
    # return SimplifiedDataset.objects.filter(pipeline__slug=pipeline_slug)
    datasets = Dataset.objects.filter(
        pipeline__slug=pipeline_slug,
        configs__state__in=[DatasetConfig.State.PUBLISHED, DatasetConfig.State.TESTING],
    ).prefetch_related("configs")

    return datasets


@config_router.get("{id}/", response=DatasetConfigSchema, auth=django_auth)
def get_config(request: HttpRequest, id: int):
    """Get a specific dataset config by ID"""
    config = get_object_or_404(DatasetConfig, id=id)
    config.dataset.can_view(request.user)
    return config


@config_router.post("{id}/", response=DatasetConfigSchema, auth=django_auth)
def post_config(request: HttpRequest, id: int, payload: DatasetConfigPostSchema):
    """Update a specific dataset config by ID"""
    config = get_object_or_404(DatasetConfig, id=id)
    if not config.dataset.can_edit(request.user):
        raise PermissionError("You do not have permission to edit this dataset config.")
    for attr, value in payload.dict().items():
        setattr(config, attr, value)
    config.save()
    return config


@config_router.get(
    "by-pipeline/{pipeline_slug}/",
    response=list[DatasetConfigSchema],
    auth=pipeline_api_key_auth,
)
def get_configs_by_pipeline(request: HttpRequest, pipeline_slug: str):
    """Get all active published or testing dataset configs for a specific pipeline"""
    configs = DatasetConfig.objects.filter(
        dataset__pipeline__slug=pipeline_slug,
        state__in=[DatasetConfig.State.PUBLISHED, DatasetConfig.State.TESTING],
        dataset__state=Dataset.State.ACTIVE,
    ).select_related("dataset")

    return configs
