from django.contrib import admin

from .models import Dataset, DatasetConfig, SimplifiedDataset

admin.site.register(Dataset)
admin.site.register(DatasetConfig)
admin.site.register(SimplifiedDataset)
