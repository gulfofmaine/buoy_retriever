from django.shortcuts import get_object_or_404
from ninja import Router, ModelSchema

from .models import Runner

router = Router()


class RunnerSchema(ModelSchema):
    class Meta:
        model = Runner
        fields = [
            "id",
            "slug",
            "name",
            "config_schema",
            "description",
            "created",
            "edited",
            "active",
        ]


@router.get("/", response=list[RunnerSchema])
def list_runners(request):
    return Runner.objects.all()


class RunnerPostSchema(ModelSchema):
    class Meta:
        model = Runner
        fields = ["slug", "name", "config_schema", "description"]


@router.post("/", response=RunnerSchema)
def create_update_runner(request, payload: RunnerPostSchema):
    """If a runner with the given slug already exists, update it instead of creating a new one."""
    try:
        existing = Runner.objects.get(slug=payload.slug)
        existing.name = payload.name
        existing.config_schema = payload.config_schema
        existing.description = payload.description
        existing.active = True
        existing.save()
        return existing
    except Runner.DoesNotExist:
        runner = Runner(**payload.dict())
        runner.save()
        return runner


# @router.patch("/{slug}", response=RunnerSchema)
# def patch_runner(request, slug: str, payload: PatchDict[RunnerPostSchema]):
#     runner = get_object_or_404(Runner, slug=slug)
#     for attr, value in payload.items():
#         setattr(runner, attr, value)
#     runner.save()
#     return runner


# @router.get("/{slug}", response=RunnerSchema)
# def get_runner(request, slug: str):
#     runner = get_object_or_404(Runner, slug=slug)
#     return runner


@router.get("/{id}", response=RunnerSchema)
def get_runner_by_id(request, id: int):
    runner = get_object_or_404(Runner, id=id)
    return runner
