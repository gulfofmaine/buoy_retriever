from enum import Enum
from typing import TypeVar

from pydantic import BaseModel, Field


class DatasetConfigBase(BaseModel):
    """Base model for dataset configuration

    This will be serialized as JSON Schema to the backend and used to generate the forms for data providers to configure.
    """


# Avoid type errors with the generic config
# See https://docs.pydantic.dev/latest/concepts/models/#generic-models
ConfigT = TypeVar("ConfigT", bound=DatasetConfigBase)


class ConfigState(str, Enum):
    """State of the dataset configuration"""

    DRAFT = "Draft"
    TESTING = "Testing"
    PUBLISHED = "Published"


class DatasetBase[ConfigT](BaseModel):
    """Dataset"""

    slug: str = Field(..., description="Unique dataset slug")
    config: ConfigT = Field(
        ...,
        description="The configuration for the dataset.",
    )

    config_state: ConfigState = ConfigState.DRAFT

    @property
    def safe_slug(self) -> str:
        """Dagster safe name for this dataset"""
        return self.slug.lower().replace("-", "_")
