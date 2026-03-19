import json
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Self, TypeVar

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

    @classmethod
    def from_fixture(cls: type[Self], path: Path, created_dt_str: str) -> Self:
        """Load a dataset config from a JSON fixture file"""
        with path.open() as f:
            data = json.load(f)

        dataset_data = next((x for x in data if x["model"] == "datasets.dataset"), None)
        if not dataset_data:
            raise ValueError(f"No dataset found in fixture {path}")
        config_data = next(
            (
                x
                for x in data
                if x["model"] == "datasets.datasetconfig"
                and (
                    datetime.fromisoformat(x["fields"]["created"])
                    >= datetime.fromisoformat(created_dt_str) - timedelta(seconds=1)
                    and datetime.fromisoformat(x["fields"]["created"])
                    <= datetime.fromisoformat(created_dt_str) + timedelta(seconds=1)
                )
            ),
            None,
        )
        if not config_data:
            raise ValueError(
                f"No dataset config found in fixture {path} with created datetime {created_dt_str}",
            )

        data = {
            "slug": dataset_data["fields"]["slug"],
            "config": config_data["fields"]["config"],
            "config_state": config_data["fields"]["state"],
        }

        return cls(**data)
