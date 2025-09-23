from django.db import models


class Runner(models.Model):
    slug = models.SlugField(unique=True)
    name = models.CharField(max_length=255)
    config_schema = models.JSONField(blank=True)
    description = models.TextField()

    created = models.DateTimeField(auto_now_add=True)
    edited = models.DateTimeField(auto_now=True)
    active = models.BooleanField(default=True)

    def __repr__(self):
        return f"{self.name} ({self.slug})"

    def __str__(self):
        return self.__repr__()
