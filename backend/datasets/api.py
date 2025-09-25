from django.shortcuts import get_object_or_404
from ninja import Router, ModelSchema, PatchDict

from .models import SimplifiedDataset
from pipelines.models import Pipeline

router = Router()


class DatasetSchema(ModelSchema):
    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "pipeline", "config", "created", "edited"]


@router.get("/", response=list[DatasetSchema])
def list_datasets(request):
    return SimplifiedDataset.objects.all()


class DatasetPostSchema(ModelSchema):
    pipeline_id: int

    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "config"]


@router.post("/", response=DatasetSchema)
def create_dataset(request, payload: DatasetPostSchema):
    pipeline = get_object_or_404(Pipeline, id=payload.pipeline_id)
    data = payload.dict()
    del data["pipeline_id"]
    dataset = SimplifiedDataset(**data, pipeline=pipeline)
    dataset.save()
    return dataset


@router.get("/{slug}", response=DatasetSchema)
def get_dataset(request, slug: str):
    return SimplifiedDataset.objects.get(slug=slug)


@router.patch("/{slug}", response=DatasetSchema)
def patch_dataset(request, slug: str, payload: PatchDict[DatasetPostSchema]):
    print(f"Payload: {payload}")
    dataset = SimplifiedDataset.objects.get(slug=slug)
    for attr, value in payload.items():
        setattr(dataset, attr, value)
    dataset.save()
    print(f"Updated dataset: {dataset}")
    return dataset


@router.get("/by-pipeline/{pipeline_slug}", response=list[DatasetSchema])
def get_datasets_by_pipeline(request, pipeline_slug: str):
    return SimplifiedDataset.objects.filter(pipeline__slug=pipeline_slug)
