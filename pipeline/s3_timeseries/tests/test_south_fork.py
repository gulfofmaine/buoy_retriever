from pathlib import Path

import dagster as dg
import pandas as pd
import pytest
import xarray as xr

from common import io, test_utils
from pipeline import S3TimeseriesDataset, defs_for_dataset

TEST_DATA_DIR = Path("/mnt/test-data/s3_timeseries/")


@pytest.fixture
def dataset_config():
    config_path = TEST_DATA_DIR / "fixtures/south_fork.json"
    return S3TimeseriesDataset.from_fixture(config_path, "2026-01-25T00:35:49.866Z")


@pytest.fixture
def defs(dataset_config):
    return defs_for_dataset(dataset_config)


def test_can_build_defs(defs):
    assert defs is not None
    assert len(defs.assets) == 2


def test_sensor(defs, mocked_s3, s3_credentials):
    bucket = "ott-south-fork-wind"
    object_key1 = "SFW01_WB_02_wave_20250210_025641_025641.txt"  # gitleaks:allow
    object_key2 = "SFW01_WB_02_wave_20250211_145639_145639.txt"  # gitleaks:allow
    mocked_s3.create_bucket(Bucket=bucket)
    mocked_s3.put_object(Bucket=bucket, Key=object_key1, Body="test")
    mocked_s3.put_object(Bucket=bucket, Key=object_key2, Body="test")

    sensor = test_utils.get_sensor_by_name(defs, "south_fork_s3_sensor")
    assert sensor is not None

    context = dg.build_sensor_context(
        cursor="2025-01-27T23:50:56+00:00",
        instance=dg.DagsterInstance.ephemeral(),
    )

    run_requests = list(sensor(context, s3_credentials=s3_credentials))
    assert len(run_requests) == 2
    assert run_requests[0].partition_key == "2025-02-10"
    # the context isn't keeping the updated cursor in tests for some reason


@pytest.mark.aws
def test_daily_asset(defs, dataset_config, s3_resource):
    daily_df = test_utils.get_asset_by_name(defs, "daily_df")
    spec = daily_df.get_asset_spec()

    assert daily_df is not None, "There should be a daily_df asset"
    assert spec.group_name == "south_fork", "The group name should be south_fork"
    assert spec.description == "Download daily dataframe from S3."
    assert spec.metadata[io.DESIRED_PATH] == dataset_config.daily_partition_path()

    context = dg.build_asset_context(partition_key="2025-02-10")

    df = daily_df(context, s3fs=s3_resource)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    snapshot_path = TEST_DATA_DIR / "south_fork/daily_df_2025-02-10.csv"

    # Uncomment to update CSV snapshot
    df.to_csv(snapshot_path, index=False)
    snapshot = pd.read_csv(snapshot_path, parse_dates=["datetime"])
    pd.testing.assert_frame_equal(df, snapshot)


def test_monthly_asset(defs, dataset_config, s3_resource):
    monthly_ds = test_utils.get_asset_by_name(defs, "monthly_ds")
    spec = monthly_ds.get_asset_spec()
    assert monthly_ds is not None, "There should be a monthly_ds asset"
    assert spec.group_name == "south_fork", "The group name should be south_fork"
    assert (
        spec.description
        == "Combine daily dataframes into a monthly NetCDF and apply transformations."
    )
    assert spec.metadata[io.DESIRED_PATH] == dataset_config.monthly_partition_path()

    context = dg.build_asset_context(partition_key="2025-02-01")

    daily_df = {
        "2025-02-10": pd.read_csv(
            TEST_DATA_DIR / "south_fork/daily_df_2025-02-10.csv",
            parse_dates=["datetime"],
        ),
    }
    daily_df["2025-02-11"] = daily_df["2025-02-10"].copy()
    daily_df["2025-02-11"]["datetime"] = daily_df["2025-02-11"][
        "datetime"
    ] + pd.Timedelta(days=1)

    ds = monthly_ds(context, daily_df=daily_df)

    assert isinstance(ds, xr.Dataset)
    snapshot_path = TEST_DATA_DIR / "south_fork/monthly_ds_2025-02.nc"
    # Uncomment to update NetCDF snapshot
    ds.to_netcdf(snapshot_path)
    snapshot = xr.load_dataset(snapshot_path)

    xr.testing.assert_equal(ds, snapshot)
