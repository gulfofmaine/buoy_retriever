from django.contrib import admin

from guardian.admin import GuardedModelAdmin

from .models import Dataset, DatasetConfig, SimplifiedDataset


@admin.register(Dataset)
class DatasetAdmin(GuardedModelAdmin):
    pass


admin.site.register(DatasetConfig)
admin.site.register(SimplifiedDataset)
