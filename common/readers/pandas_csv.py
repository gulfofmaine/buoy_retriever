from typing import Annotated

import pandas as pd
from pydantic import BaseModel, Field


class PandasCSVReader(BaseModel):
    """Configure Pandas to read CSVs in S3.

    Passed through to `pd.read_csv`, so see full argument definitions at
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
    """

    sep: Annotated[str | None, Field(description="CSV delimiter")] = None
    comment: Annotated[str | None, Field(description="CSV line comment character")] = (
        None
    )
    na_values: str = "None"
    # delim_whitespace
    # skiprows
    # sep
    # on_bad_lines
    # usecols

    def read_df(self, file_path) -> pd.DataFrame:
        """Read a CSV file from S3 into a Pandas DataFrame"""
        reader_kwargs = self.model_dump()
        
        return pd.read_csv(file_path, **reader_kwargs)
