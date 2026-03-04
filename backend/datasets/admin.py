from django.contrib import admin

from guardian.admin import GuardedModelAdmin

from .models import Dataset, DatasetConfig


class InlineDatasetConfigAdmin(admin.TabularInline):
    model = DatasetConfig
    extra = 0


@admin.register(Dataset)
class DatasetAdmin(GuardedModelAdmin):
    inlines = [InlineDatasetConfigAdmin]


admin.site.register(DatasetConfig)
