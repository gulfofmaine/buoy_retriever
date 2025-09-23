from django.shortcuts import get_object_or_404
from ninja import Router, ModelSchema, PatchDict

from .models import SimplifiedDataset
from pipelines.models import Runner

router = Router()


class DatasetSchema(ModelSchema):
    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "runner", "config", "created", "edited"]


@router.get("/", response=list[DatasetSchema])
def list_datasets(request):
    return SimplifiedDataset.objects.all()


class DatasetPostSchema(ModelSchema):
    runner_id: int

    class Meta:
        model = SimplifiedDataset
        fields = ["slug", "config"]


@router.post("/", response=DatasetSchema)
def create_dataset(request, payload: DatasetPostSchema):
    runner = get_object_or_404(Runner, id=payload.runner_id)
    data = payload.dict()
    del data["runner_id"]
    dataset = SimplifiedDataset(**data, runner=runner)
    dataset.save()
    return dataset


@router.get("/{slug}", response=DatasetSchema)
def get_dataset(request, slug: str):
    return SimplifiedDataset.objects.get(slug=slug)


@router.patch("/{slug}", response=PatchDict[DatasetSchema])
def patch_dataset(request, slug: str, payload: PatchDict[DatasetPostSchema]):
    dataset = SimplifiedDataset.objects.get(slug=slug)
    for attr, value in payload.items():
        setattr(dataset, attr, value)
    dataset.save()
    return dataset
