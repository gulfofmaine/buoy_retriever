from django.contrib import admin

from .models import Dataset, DatasetConfig

admin.site.register(Dataset)
admin.site.register(DatasetConfig)
