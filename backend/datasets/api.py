from ninja import Router, ModelSchema

from .models import Dataset

router = Router()


class DatasetSchema(ModelSchema):
    class Meta:
        model = Dataset
        fields = ["slug", "state", "created", "edited"]


@router.get("/", response=list[DatasetSchema])
def list_datasets(request):
    return Dataset.objects.all()
