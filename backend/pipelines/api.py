from django.shortcuts import get_object_or_404
from ninja import ModelSchema, Router

from .models import Pipeline

router = Router()


class PipelineSchema(ModelSchema):
    class Meta:
        model = Pipeline
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


class PipelinePostSchema(ModelSchema):
    class Meta:
        model = Pipeline
        fields = ["slug", "name", "config_schema", "description"]


@router.get("/", response=list[PipelineSchema])
def list_pipelines(request):
    """List all pipelines"""
    return Pipeline.objects.all()


@router.post("/", response=PipelineSchema)
def create_update_pipeline(request, payload: PipelinePostSchema):
    """If a pipeline with the given slug already exists, update it instead of creating a new one."""
    try:
        existing = Pipeline.objects.get(slug=payload.slug)
        existing.name = payload.name
        existing.config_schema = payload.config_schema
        existing.description = payload.description
        existing.active = True
        existing.save()
        return existing
    except Pipeline.DoesNotExist:
        pipeline = Pipeline(**payload.dict())
        pipeline.save()
        return pipeline


# @router.patch("/{slug}", response=PipelineSchema)
# def patch_pipeline(request, slug: str, payload: PatchDict[PipelinePostSchema]):
#     pipeline = get_object_or_404(Pipeline, slug=slug)
#     for attr, value in payload.items():
#         setattr(pipeline, attr, value)
#     pipeline.save()
#     return pipeline


# @router.get("/{slug}", response=PipelineSchema)
# def get_pipeline(request, slug: str):
#     pipeline = get_object_or_404(Pipeline, slug=slug)
#     return pipeline


@router.get("/{id}", response=PipelineSchema)
def get_pipeline_by_id(request, id: int):
    """Get a specific pipeline by ID"""
    pipeline = get_object_or_404(Pipeline, id=id)
    return pipeline
