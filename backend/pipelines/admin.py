from django.contrib import admin

from .models import Pipeline, PipelineApiKey

# Register your models here.
admin.site.register(Pipeline)
admin.site.register(PipelineApiKey)
