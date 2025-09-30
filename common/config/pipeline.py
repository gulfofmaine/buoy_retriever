from typing import Annotated, Type

from pydantic import BaseModel, Field

from .dataset import DatasetConfigBase


class PipelineConfig(BaseModel):
    """Configuration for a pipeline."""

    slug: Annotated[str, Field(description="The unique slug of the pipeline.")]
    name: Annotated[str, Field(description="The name of the pipeline.")]
    description: Annotated[
        str,
        Field(
            description="A description of the pipeline. This will be displayed to data providers, and can include Markdown",
        ),
    ]

    dataset_config: Annotated[
        Type[DatasetConfigBase],
        Field(description="The configuration for the dataset."),
    ]

    def to_json(self):
        """Return a JSON-serializable representation of the pipeline configuration compatible with the Backend API."""
        return {
            "slug": self.slug,
            "name": self.name,
            "description": self.description,
            "config_schema": self.dataset_config.model_json_schema(),
        }
