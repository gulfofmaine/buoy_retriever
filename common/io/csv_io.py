"""
Load and save CSV files with Pandas to the datastore
"""

from pathlib import Path

import pandas as pd
from dagster import InputContext, MetadataValue, OutputContext

from .base import IOManagerBase

# ERDDAP requires ISO 8601 datetimes
# https://coastwatch.pfeg.noaa.gov/erddap/download/setupDatasetsXml.html#stringTimeUnits
ISO_8601_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class PandasCsvIoManager(IOManagerBase):
    """Load and save CSV files with Pandas"""

    def dump_to_path(
        self,
        context: OutputContext,
        obj: pd.DataFrame,
        path: Path,
    ) -> None:
        """Save dataframe to a given path as a CSV"""
        with path.open("w") as f:
            obj.to_csv(f, index=False, date_format=ISO_8601_DATE_FORMAT)

        context.add_output_metadata(
            {
                "csv.head": MetadataValue.md(f"{obj.head().to_markdown()}"),
                "csv.tail": MetadataValue.md(f"{obj.tail().to_markdown()}"),
                "csv.describe": MetadataValue.md(f"{obj.describe().to_markdown()}"),
            },
        )

    def load_from_path(self, context: InputContext, path: Path) -> pd.DataFrame:
        """Load a dataframe from a given CSV path"""
        with path.open() as f:
            return pd.read_csv(f)
