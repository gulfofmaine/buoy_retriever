from datetime import date
from textwrap import dedent
from typing import Annotated

import dagster as dg
import pandas as pd
import sentry_sdk
import xarray as xr
from pydantic import BaseModel, Field

from common import assets, config, io
from common.backend_api import BackendAPIClient
from common.config import mappings, s3_source, attributes
from common.readers.pandas_csv import PandasCSVReader
from common.resource.s3fs_resource import S3Credentials, S3FSResource
from common.sentry import SentryConfig
import yaml
sentry = SentryConfig(pipeline_name="s3_timeseries")


class DayGlob(BaseModel):
    """Configure glob patterns for daily files in S3."""

    day_pattern: Annotated[
        str | None,
        Field(
            description=dedent("""
                Glob pattern for daily files.
                The `partition_date` is available for formatting
                """),
            examples=[
                "EW01_ADCP_{partition_date:%Y%m%d}_*.txt",
                "EW01_wave_ioos_{partition_date:%Y%m%d}_*.txt",
            ],
        ),
    ] = None


class S3TimeseriesConfig(
    config.DatasetConfigBase,
    # config.AttributeConfigMixin,
    mappings.VariableMappingMixin,
    mappings.OptionalProfileDepthMixin,
    s3_source.S3SourceMixin,
    attributes.AttributeConfigMixin,
    mappings.VariableConverterMixIn
):
    """Configuration for S3 Timeseries Dataset."""

    start_date: date

    reader: PandasCSVReader

    dataset_type: Annotated[str, Field(description="Dateset type (timeseries or profile)")]

       
    source_time_var : str = "datetime"
    
    file_pattern: Annotated[DayGlob, Field(description="Source file name pattern")]
    drop_vars : Annotated[list[str],        
            Field(
            description="Variables to drop from the dataset",
            default_factory=list,
        ),] = None
    latitude: Annotated[
        float | None,
        Field(description="Fixed latitude of the station"),
    ] = None
    longitude: Annotated[
        float | None,
        Field(description="Fixed longitude of the station"),
    ] = None
    station: Annotated[str, Field(description="Station name/timeseries_id")]


class S3TimeseriesDataset(config.DatasetBase):
    """S3 Timeseries Dataset."""

    config: Annotated[
        S3TimeseriesConfig,
        Field(description="The configuration for the dataset."),
    ]

    def daily_partition_path(self):
        """Path to daily partitions."""
        return (
            self.safe_slug
            + "/daily/{partition_key_dt:%Y}/{partition_key_dt:%m}/{partition_key_dt:%Y-%m-%d}.csv"
        )

    def monthly_partition_path(self):
        """Path to monthly partitions."""
        return (
            self.safe_slug
            # + "/monthly/{partition_key_dt:%Y}/"
            + "/"
            + self.slug
            + "_{partition_key_dt:%Y-%m}.nc"
        )


def defs_for_dataset(dataset: S3TimeseriesDataset) -> dg.Definitions:
    """Definitions for a single S3 Timeseries dataset."""
    common_asset_kwargs = {
        "key_prefix": ["s3_timeseries", dataset.safe_slug],
        "group_name": dataset.safe_slug,
    }

    daily_partitions = dg.DailyPartitionsDefinition(
        start_date=dataset.config.start_date.isoformat(),
        end_offset=1,
    )

    monthly_partitions = dg.MonthlyPartitionsDefinition(
        start_date=dataset.config.start_date.strftime("%Y-%m-01"),
        end_offset=1,
    )

    @dg.asset(
        partitions_def=daily_partitions,
        metadata={io.DESIRED_PATH: dataset.daily_partition_path()},
        **io.CSV_ASSET_KWARGS,
        **common_asset_kwargs,
    )
    @sentry.capture_op_exceptions
    def daily_df(context: dg.AssetExecutionContext, s3fs: S3FSResource) -> pd.DataFrame:
        """Download daily dataframe from S3."""
        partition_date_string = context.asset_partition_key_for_output()
        partition_date = date.fromisoformat(partition_date_string)

        day_glob = (
            dataset.config.s3_source.bucket
            + dataset.config.s3_source.prefix
            + dataset.config.file_pattern.day_pattern.format(
                partition_date=partition_date,
            )
        )

        context.log.info(
            f"Reading daily data for {partition_date_string} from S3 with glob: {day_glob}",
        )
       
        s3_keys = s3fs.fs.glob(day_glob)
        s3_keys.sort()

        context.log.info(f"Found {len(s3_keys)} files: \n{s3_keys}")
        context.add_output_metadata({"Source S3 keys": dg.MetadataValue.json(s3_keys)})

        daily_dfs = []
    

        for day_f in s3_keys:
            context.log.debug(f"Reading {day_f}")
            with s3fs.fs.open(day_f, "rb") as f:

               
                df = dataset.config.reader.read_df(f)

                if dataset.config.variable_converter is not None:
                    for split_conv in dataset.config.variable_converter.split_operations:
                        splt_col =df[split_conv.source_variable].str.split(split_conv.sep, expand=True)
                        for n_var in split_conv.output_variables:

                            df[split_conv.output_variables[n_var]] = splt_col[n_var]
                           
                            
                        df.drop(split_conv.source_variable,axis=1,inplace=True)
                if dataset.config.drop_vars is not None:   
                    df.drop(columns=dataset.config.drop_vars,inplace =True)
                    
                if dataset.config.dataset_type =='profile':
                    # Translate the profile data from multiple columns for each variable (CurSpd1, curSpd2,..curSpdN) to
                    # two columns: curSpd, depth
                    
                    all_profile_vars = [var for depth_conf in dataset.config.profile_data for var in depth_conf.mappings.keys()]

                    non_profile_vars = df.columns.difference(all_profile_vars).tolist()
                    
                    for depth in dataset.config.profile_data:
                        
                        keep = non_profile_vars + list(depth.mappings.keys())
      
                        
                        df_depth = df[keep].copy()
                        df_depth.rename(columns=depth.mappings,inplace=True)
                        
                        if depth.depth is not None: 
                            df_depth['depth'] = float(depth.depth)
                        
                        daily_dfs.append(df_depth)
                        indx_var = [dataset.config.source_time_var,"depth"]

                else:       
                    daily_dfs.append(df)
                    indx_var = dataset.config.source_time_var
        df = pd.concat(daily_dfs)

        df[dataset.config.source_time_var] = pd.to_datetime(df[dataset.config.source_time_var])
        df = df.sort_values(indx_var)
        df = df.reset_index()


        return df

    @dg.asset(
        ins={
            "daily_df": dg.AssetIn(
                partition_mapping=dg.TimeWindowPartitionMapping(
                    allow_nonexistent_upstream_partitions=True,
                ),
                metadata={io.ALLOW_MISSING_PARTITIONS: True},
            ),
        },
        partitions_def=monthly_partitions,
        metadata={
            io.DESIRED_PATH: dataset.monthly_partition_path(),
            # io.S3_DESIRED_PATH: config.s3_path(),
            # io.S3_PUBLIC: True,
        },
        automation_condition=assets.auto_condition_eager_allow_missing(),
        **io.NETCDF_ASSET_KWARGS,
        **common_asset_kwargs,
    )
    @sentry.capture_op_exceptions
    def monthly_ds(
        context: dg.AssetExecutionContext,
        daily_df: dict[str, pd.DataFrame],
    ) -> xr.Dataset:
        """Combine daily dataframes into a monthly NetCDF and apply transformations."""
        daily_dfs = []

        for df_date, df in daily_df.items():
            for var_map in dataset.config.variable_mappings:
                if var_map.source in df.columns:
                    df = df.rename(columns={var_map.source: var_map.output})
                else:
                    context.log.warning(
                        f"Source variable '{var_map.source}' not found in data for {df_date}",
                    )

            if len(set(df.columns)) != len(df.columns):
                context.log.warning(
                    f"Column name collision after renaming for data on {df_date}, trying to squish duplicates",
                )
                df = df.groupby(df.columns, axis=1).first()
            daily_dfs.append(df)

        df = pd.concat(daily_dfs, ignore_index=True)
        if dataset.config.dataset_type =='profile':
            indx_var = ["time","depth"]
        else:
            indx_var = "time"
        
        
        df = df.sort_values(indx_var)
        df = df.drop_duplicates(subset=indx_var)
        df["time"] = pd.to_datetime(df["time"])
        df = df.set_index(indx_var)

        ds = df.to_xarray()

        ds["station"] = dataset.config.station
        if dataset.config.latitude is not None:
            ds["latitude"] = dataset.config.latitude
        if dataset.config.longitude is not None:
            ds["longitude"] = dataset.config.longitude

        ds = ds.set_coords(["station", "latitude", "longitude"])
        ds = ds.drop_vars("index")
        # apply attributes

        ds["time"].encoding.update(
            {"units": "seconds since 1970-01-01T00:00:00Z", "calendar": "gregorian", "standard_name":"time"},
        )

        dataset.config.attributes.add_attributes_from_yaml()


        dataset.config.attributes.apply_to_dataset(ds)
        return ds

    return dg.Definitions(assets=[daily_df, monthly_ds])


@dg.definitions
def build_defs() -> dg.Definitions:
    """Build definitions for S3 Timeseries pipeline and register with backend."""
    with sentry_sdk.start_transaction(
        op="build_defs",
        name="Build S3 Timeseries Pipeline Definitions",
    ):
        pipeline = config.PipelineConfig(
            slug="s3_timeseries",
            name="S3 Timeseries",
            description="Fetch time series data from CSV files in S3",
            dataset_config=S3TimeseriesConfig,
        )

        api_client = BackendAPIClient()
        api_client.register_pipeline(pipeline)

        datastore, io_managers = io.common_resources(path_stub="s3_timeseries")

        credentials = S3Credentials(
            access_key_id=dg.EnvVar("S3_TS_ACCESS_KEY_ID"),
            secret_access_key=dg.EnvVar("S3_TS_SECRET_ACCESS_KEY"),
        )

        defs = dg.Definitions(
            resources={
                "s3_credentials": credentials,
                "s3fs": S3FSResource(
                    credentials=credentials,
                    region_name="us-east-1",
                ),
                "datastore": datastore,
                **io_managers,
            },
        )

        datasets = api_client.datasets_for_pipeline(pipeline.slug, S3TimeseriesDataset)

        for dataset in datasets:
            dataset_defs = defs_for_dataset(dataset)
            defs = dg.Definitions.merge(defs, dataset_defs)

        return defs
