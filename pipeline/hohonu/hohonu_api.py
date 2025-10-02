"""
Load data from Hohonu's API
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from dagster import ConfigurableResource
from pydantic import BaseModel

DATE_FORMAT = "%Y-%m-%d"

HOHONU_TIMEOUT = int(os.environ.get("HOHONU_TIMEOUT", 30))


class HohonuApi(ConfigurableResource):
    """Access the Hohonu API

    API reference: https://hohonu.readme.io/reference/getting-started-with-your-api
    """

    api_key: str

    def headers(self):
        """Set up Hohonu API auth headers"""
        return {
            "Authorization": self.api_key,
            "accept": "application/json",
        }

    def load_daily_data(
        self,
        station_id: str,
        day: str,
        datum: str = "NAVD",
        cleaned: bool = True,
        forecast: bool = False,
    ):
        """Load data for a given day based on YYYY-MM-DD string
        from https://dashboard.hohonu.io/api/v1/stations/{stationId}/statistic/

        API reference: https://hohonu.readme.io/reference/searchinventory
        """
        start_dt = datetime.strptime(day, DATE_FORMAT)
        end_dt = start_dt + timedelta(days=1)
        cleaned = 1 if cleaned else 0

        url = (
            f"https://dashboard.hohonu.io/api/v1/stations/{station_id}/waterlevel"
            f"?from={start_dt.strftime(DATE_FORMAT)}&to={end_dt.strftime(DATE_FORMAT)}"
            f"&datum={datum}&qc_level={cleaned}&predictions={'true' if forecast else 'false'}"
        )

        headers = self.headers()
        response = requests.get(url, headers=headers, timeout=HOHONU_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            e.add_note(f"Failed to load daily data for {url}: {response.text}")
            raise e

        return DataResponse.model_validate(response.json())


class DataDatum(BaseModel):
    """Datum selected for returned data"""

    label: str
    unit: str


class DataMeta(BaseModel):
    """Station metadata for data response"""

    location: str
    station_id: str
    data_source: str
    measurement_type: str
    datum: DataDatum


class DataWaterLevel(BaseModel):
    """Water level data nesting"""

    waterlevel: list[dict]


class DataResponse(BaseModel):
    """Data response from Hohonu API"""

    meta: DataMeta
    data: DataWaterLevel

    def to_df(self):
        """ "Convert the response to Pandas DataFrame, and reshape to be more user-friendly"""
        df = pd.DataFrame(self.data.waterlevel)
        df = df.rename(
            columns={"t": "time", "o": "observed", "p": "forecast", "f": "flags"},
        )
        try:
            df["time"] = pd.to_datetime(df["time"])
        except KeyError as e:
            e.add_note(
                f"'time' column missing from data: {df.columns}, {df.head()}, {self.data}",
            )
            raise

        if "flags" in df.columns:
            flags = df["flags"].str.split(",").to_list()
            df[
                [
                    "gap_test",
                    "gross_range_test",
                    "spike_test",
                    "flat_line_test",
                    "rate_of_change_test",
                    "neighbor_test",
                ]
            ] = flags
            del df["flags"]

        return df
