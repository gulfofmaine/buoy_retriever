import os

import boto3
import dagster as dg
import pandas as pd
import pytest
import xarray as xr
from moto import mock_aws

from common import io, test_utils
from common.resource.s3fs_resource import S3Credentials, S3FSResource
from pipeline import S3TimeseriesDataset, defs_for_dataset


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": [
            ("Authorization", "FAKE"),
        ],
        "ignore_hosts": ["spotlight"],
    }


@pytest.fixture
def dataset_config():
    return S3TimeseriesDataset(
        slug="empire-met-test",
        config={
            "reader": {"sep": ";", "comment": "#"},
            "station": "EW1-met",
            "latitude": None,
            "s3_source": {"bucket": "ott-empire", "prefix": "/"},
            "start_date": "2025-07-03",
            "file_pattern": {"day_pattern": "EW01_met_{partition_date:%Y%m%d}_*.txt"},
            "variable_mappings": [
                {"output": "time", "source": "datetime"},
                {"output": "latitude", "source": "Latitude_Avg"},
                {"output": "longitude", "source": "Longitude_Avg"},
            ],
        },
    )


@pytest.fixture
def defs(dataset_config):
    return defs_for_dataset(dataset_config)


def test_can_build_defs(defs):
    assert defs is not None
    assert len(defs.assets) == 2


@pytest.fixture
def s3_credentials():
    return S3Credentials(
        access_key_id=os.environ["S3_TS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["S3_TS_SECRET_ACCESS_KEY"],
    )


@pytest.fixture
def s3_resource(s3_credentials):
    return S3FSResource(
        credentials=s3_credentials,
        region_name="us-east-1",
    )


@pytest.fixture
def mocked_s3():
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


def test_sensor(defs, mocked_s3, s3_credentials):
    bucket = "ott-empire"
    object_key1 = "EW01_met_20251112_120000.txt"  # gitleaks:allow
    object_key2 = "EW01_met_20251113_235056.txt"  # gitleaks:allow
    mocked_s3.create_bucket(Bucket=bucket)
    mocked_s3.put_object(Bucket=bucket, Key=object_key1, Body="test")
    mocked_s3.put_object(Bucket=bucket, Key=object_key2, Body="test")

    sensor = test_utils.get_sensor_by_name(defs, "empire_met_test_s3_sensor")
    assert sensor is not None

    context = dg.build_sensor_context(
        cursor="2025-11-12T23:50:56+00:00",
        instance=dg.DagsterInstance.ephemeral(),
    )

    run_requests = list(sensor(context, s3_credentials=s3_credentials))
    assert len(run_requests) == 2
    assert run_requests[0].partition_key == "2025-11-12"
    assert context.cursor == "2025-11-12T23:50:56+00:00"


# @pytest.mark.vcr()
@pytest.mark.aws
def test_daily_asset(defs, dataset_config, s3_resource):
    # bucket = "ott-empire"
    # mocked_s3.create_bucket(Bucket=bucket)

    # test_data_dir = Path(__file__).parent / "test_data" / "empire_met"
    # for file_path in test_data_dir.glob("*.txt"):
    #     with file_path.open("rb") as f:
    #         mocked_s3.put_object(
    #             Bucket=bucket,
    #             Key=file_path.name,
    #             Body=f,
    #         )

    daily_df = test_utils.get_asset_by_name(defs, "daily_df")
    spec = daily_df.get_asset_spec()

    assert daily_df is not None, "There should be a daily_df asset"
    assert spec.group_name == "empire_met_test", (
        "The group name should be empire_met_test"
    )
    assert spec.description == "Download daily dataframe from S3."
    assert spec.metadata[io.DESIRED_PATH] == dataset_config.daily_partition_path()

    context = dg.build_asset_context(partition_key="2025-11-13")

    df = daily_df(context, s3fs=s3_resource)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    snapshot_path = "tests/test_data/empire_met/test_empire_met_daily_asset.csv"

    # Uncomment to update CSV snapshot
    # df.to_csv(snapshot_path, index=False)
    snapshot = pd.read_csv(snapshot_path, parse_dates=["datetime"])
    pd.testing.assert_frame_equal(df, snapshot)


def test_monthly_asset(defs, dataset_config):
    monthly_ds = test_utils.get_asset_by_name(defs, "monthly_ds")
    spec = monthly_ds.get_asset_spec()
    assert monthly_ds is not None
    assert spec.group_name == "empire_met_test"
    assert (
        spec.description
        == "Combine daily dataframes into a monthly NetCDF and apply transformations."
    )
    assert spec.metadata[io.DESIRED_PATH] == dataset_config.monthly_partition_path()
    context = dg.build_asset_context(partition_key="2025-11-01")

    daily_df = {
        "2025-11-12": pd.read_csv(
            "tests/test_data/empire_met/test_empire_met_daily_asset.csv",
            parse_dates=["datetime"],
        ),
    }
    daily_df["2025-11-13"] = daily_df["2025-11-12"].copy()
    daily_df["2025-11-13"]["datetime"] += pd.Timedelta(days=1)

    ds = monthly_ds(context, daily_df=daily_df)

    assert isinstance(ds, xr.Dataset)
    snapshot_path = "tests/test_data/empire_met/test_empire_met_monthly_asset.nc"
    # ds.to_netcdf(snapshot_path)
    snapshot = xr.load_dataset(snapshot_path)

    xr.testing.assert_equal(ds, snapshot)
