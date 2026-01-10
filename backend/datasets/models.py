from django.db import models
from django.contrib.auth.models import User, Group
from guardian.shortcuts import assign_perm


class Dataset(models.Model):
    """A slightly more full featured dataset model where
    the configuration is separated so that it can be versioned"""

    slug = models.SlugField(
        "Admin slug",
        unique=True,
        help_text="A unique slug to identify this dataset in the admin",
    )
    pipeline = models.ForeignKey(
        "pipelines.Pipeline",
        related_name="datasets",
        on_delete=models.CASCADE,
    )

    class State(models.TextChoices):
        ACTIVE = "Active"
        # DEVELOPMENT = "Development"
        DISABLED = "Disabled"

    state = models.TextField(choices=State, default=State.ACTIVE)
    created = models.DateTimeField(auto_now_add=True)
    edited = models.DateTimeField(auto_now=True)

    class Meta:
        permissions = (("publish_dataset", "Can publish dataset"),)

    def __str__(self):
        return self.slug

    def can_view(self, user: User) -> bool:
        """Check if the user has view permission for this dataset."""
        return user.has_perm("datasets.view_dataset", self)

    def assign_view_permission(self, user_group: User | Group):
        """Assign view permission for this dataset to a user or group."""
        assign_perm("view_dataset", user_group, self)

    def can_edit(self, user: User) -> bool:
        """Check if the user has edit permission for this dataset."""
        return user.has_perms(
            ["datasets.view_dataset", "datasets.change_dataset"],
            self,
        )

    def assign_edit_permission(self, user_group: User | Group):
        """Assign edit permission for this dataset to a user or group."""
        self.assign_view_permission(user_group)
        assign_perm("change_dataset", user_group, self)

    def can_publish(self, user: User) -> bool:
        """Check if the user has publish permission for this dataset."""
        return user.has_perms(
            [
                "datasets.view_dataset",
                "datasets.change_dataset",
                "datasets.publish_dataset",
            ],
            self,
        )

    def assign_publish_permission(self, user_group: User | Group):
        """Assign publish permission for this dataset to a user or group."""
        self.assign_edit_permission(user_group)
        assign_perm("publish_dataset", user_group, self)


class DatasetConfig(models.Model):
    """A versionable configuration for a dataset.

    Needs to have state management so that there can only be one active and testing
    configuration at a time per dataset. Trying to edit an active
    config should create a new draft config instead.
    """

    dataset = models.ForeignKey(
        Dataset,
        related_name="configs",
        on_delete=models.CASCADE,
    )
    config = models.JSONField(default=dict, blank=True)

    created = models.DateTimeField(auto_now_add=True)
    edited = models.DateTimeField(auto_now=True)

    class State(models.TextChoices):
        DRAFT = "Draft"
        TESTING = "Testing"
        PUBLISHED = "Published"

    state = models.TextField(choices=State, default=State.DRAFT)

    def __str__(self):
        return f"{self.dataset.slug} - Config {self.id} - {self.state} ({self.created})"

    def save(self, *args, **kwargs):
        # Ensure only one TESTING and one PUBLISHED config per dataset
        if self.state in [self.State.TESTING, self.State.PUBLISHED]:
            DatasetConfig.objects.filter(
                dataset=self.dataset,
                state=self.state,
            ).exclude(id=self.id).update(state=self.State.DRAFT)
        super().save(*args, **kwargs)


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
