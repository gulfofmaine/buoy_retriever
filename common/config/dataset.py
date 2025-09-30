from pydantic import BaseModel, Field


class DatasetConfigBase(BaseModel):
    """Base model for dataset configuration

    This will be serialized as JSON Schema to the backend and used to generate the forms for data providers to configure.
    """


class DatasetBase(BaseModel):
    """Dataset"""

    slug: str = Field(..., description="Unique dataset slug")
    config: DatasetConfigBase = Field(
        ...,
        description="The configuration for the dataset.",
    )

    @property
    def safe_slug(self) -> str:
        """Dagster safe name for this dataset"""
        return self.slug.lower().replace("-", "_")
