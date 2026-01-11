from datetime import date
from typing import Annotated

import pytest
from pydantic import Field

from common import config


class HohonuConfig(
    config.DatasetConfigBase,
    # config.AttributeConfigMixin,
):
    """Configuration for Hohonu Dataset"""

    station: Annotated[str, Field(description="Station name/timeseries_id")]
    hohonu_id: Annotated[str, Field(description="Hohonu station ID")]
    start_date: date

    latitude: Annotated[
        float,
        Field(description="Fixed latitude of the station"),
    ]
    longitude: Annotated[
        float,
        Field(description="Fixed longitude of the station"),
    ]


class HohonuDataset(config.DatasetBase):
    """Hohonu Dataset"""

    config: Annotated[
        HohonuConfig,
        Field(description="The configuration for the dataset."),
    ]

    def daily_partition_path(self):
        """Path to daily partitions"""
        return (
            self.safe_slug
            + "/daily/{partition_key_dt:%Y}/{partition_key_dt:%m}/{partition_key_dt:%Y-%m-%d}.csv"
        )

    def monthly_partition_path(self):
        """Path to monthly partitions"""
        return self.safe_slug + "/" + self.slug + "_{partition_key_dt:%Y-%m}.nc"


@pytest.mark.vcr()
def test_can_register_pipeline():
    """Test we can register a pipeline with the backend API"""
    from common.backend_api import BackendAPIClient

    pipeline = config.PipelineConfig(
        slug="hohonu",
        name="Hohonu",
        description="Fetch tide data from Hohonu's API",
        dataset_config=HohonuConfig,
    )

    api_client = BackendAPIClient(api_endpoint="http://localhost:8080/backend/api/")
    result = api_client.register_pipeline(pipeline)

    assert result["slug"] == "hohonu"


@pytest.mark.vcr()
def test_can_get_datasets():
    """Test we can get datasets for a pipeline from the backend API"""
    from common.backend_api import BackendAPIClient

    api_client = BackendAPIClient(api_endpoint="http://localhost:3000/backend/api/")
    datasets = api_client.datasets_for_pipeline(
        "hohonu",
        HohonuDataset,
    )

    assert len(datasets) > 0
    for ds in datasets:
        assert isinstance(ds, HohonuDataset)
