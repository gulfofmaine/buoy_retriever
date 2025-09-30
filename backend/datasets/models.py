from django.db import models


class SimplifiedDataset(models.Model):
    """A simplified dataset model for demonstration purposes"""

    slug = models.SlugField(unique=True)
    pipeline = models.ForeignKey("pipelines.Pipeline", on_delete=models.CASCADE)
    config = models.JSONField(blank=True)

    created = models.DateTimeField(auto_now_add=True)
    edited = models.DateTimeField(auto_now=True)

    def __repr__(self):
        return f"{self.slug}"

    def __str__(self):
        return self.__repr__()


class Dataset(models.Model):
    """A slightly more full featured dataset model where
    the configuration is separated so that it can be versioned"""

    slug = models.SlugField(
        "Admin slug",
        unique=True,
        help_text="A unique slug to identify this dataset in the admin",
    )

    class State(models.TextChoices):
        ACTIVE = "Active"
        # DEVELOPMENT = "Development"
        DISABLED = "Disabled"

    state = models.TextField(choices=State)
    created = models.DateTimeField(auto_now_add=True)
    edited = models.DateTimeField(auto_now=True)


# class DatasetPermissions(models.Model):
#     dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
#     group = models.ForeignKey("auth.Group", on_delete=models.CASCADE)

#     class Permissions(models.TextChoices):
#         READ = "Read"
#         OWNER = "Owner"
#         DATA_MANAGER = "Data Manager"

#     permission = models.TextField(choices=Permissions)


class DatasetConfig(models.Model):
    """A versionable configuration for a dataset.

    Needs to have state management so that there can only be one active and testing
    configuration at a time per dataset. Trying to edit an active
    config should create a new draft config instead.
    """

    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    pipeline = models.ForeignKey("pipelines.Pipeline", on_delete=models.CASCADE)
    config = models.JSONField()

    created = models.DateTimeField(auto_now_add=True)
    edited = models.DateTimeField(auto_now=True)

    class State(models.TextChoices):
        DRAFT = "Draft"
        TESTING = "Testing"
        CURRENT = "Active"

    state = models.TextField(choices=State, default=State.DRAFT)


# class Run(models.Model):
#     """A processing run of the dataset"""
#     config = models.ForeignKey(DatasetConfig, on_delete=models.CASCADE)
#     started = models.DateTimeField(auto_now_add=True)
#     finished = models.DateTimeField(null=True, blank=True)

#     class Status(models.TextChoices):
#         REQUESTED = "Requested"
#         STARTED = "Started"
#         SUCCESS = "Success"
#         ERROR = "Error"

#     public_info = models.JSONField(null=True, blank=True)
#     private_info = models.JSONField(null=True, blank=True)
