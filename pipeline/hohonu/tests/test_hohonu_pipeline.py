import os
from pathlib import Path

import dagster as dg
import pandas as pd
import pytest
import xarray as xr

from common import io, test_utils
from pipeline import HohonuDataset, defs_for_dataset

from hohonu_api import HohonuApi

TEST_DATA_DIR = Path("/mnt/test-data/hohonu/")


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": [("Authorization", "FAKE")],
        "ignore_hosts": ["spotlight"],
    }


@pytest.fixture
def dataset():
    config_path = TEST_DATA_DIR / "fixtures/boothbay_dmr.json"
    return HohonuDataset.from_fixture(config_path, "2026-01-10T14:03:55.644Z")


@pytest.fixture
def defs(dataset):
    return defs_for_dataset(dataset)


def test_can_build_defs(defs):
    assert defs is not None
    assert len(defs.assets) == 2


@pytest.mark.vcr(TEST_DATA_DIR / "cassettes/test_hohonu_pipeline/test_daily_asset.yaml")
def test_daily_asset(defs, dataset):
    daily_df = test_utils.get_asset_by_name(defs, "daily_df")
    spec = daily_df.get_asset_spec()

    assert daily_df is not None, "There should be a daily_df asset"
    assert spec.group_name == "boothbay_dmr", "The group name should be boothbay_dmr"
    assert spec.description == "Download daily dataframe from Hohonu for boothbay_dmr"
    assert spec.metadata[io.DESIRED_PATH] == dataset.daily_partition_path()

    context = dg.build_asset_context(partition_key="2025-09-30")

    api_key = os.environ.get("HOHONU_API_KEY", "FAKE")
    hohonu_api = HohonuApi(api_key=api_key)

    df = daily_df(context, hohonu_api=hohonu_api)

    assert isinstance(df, pd.DataFrame)

    # Uncomment to update CSV snapshot
    # df.to_csv(TEST_DATA_DIR / "test_daily_asset.csv", index=False)
    snapshot = pd.read_csv(TEST_DATA_DIR / "test_daily_asset.csv", parse_dates=["time"])
    pd.testing.assert_frame_equal(df, snapshot)


def test_monthly_asset(defs, dataset):
    monthly_ds = test_utils.get_asset_by_name(defs, "monthly_ds")
    spec = monthly_ds.get_asset_spec()
    assert monthly_ds is not None
    assert spec.group_name == "boothbay_dmr"
    assert spec.description == "Monthly NetCDFs for boothbay_dmr"
    assert spec.metadata[io.DESIRED_PATH] == dataset.monthly_partition_path()
    context = dg.build_asset_context(partition_key="2025-09-01")

    daily_df = {
        "2025-09-01": pd.read_csv(
            TEST_DATA_DIR / "test_daily_asset.csv",
            parse_dates=["time"],
        ),
    }
    daily_df["2025-09-02"] = daily_df["2025-09-01"].copy()
    daily_df["2025-09-02"]["time"] += pd.Timedelta(days=1)

    ds = monthly_ds(context, daily_df=daily_df)

    assert isinstance(ds, xr.Dataset)
    assert "navd88_meters" in ds.data_vars, "The dataset should have a metric variable"
    assert (
        ds["navd88_meters"].attrs["standard_name"]
        == "sea_surface_height_above_geopotential_datum"
    ), "Attributes should be applied"
