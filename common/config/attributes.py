from pathlib import Path
from typing import Annotated, Any

import xarray as xr
from pydantic import BaseModel, Field

# Xarray has typed attributes as dict[Any, Any]
# https://github.com/pydata/xarray/blob/64704605a4912946d2835e54baa2905b8b4396a9/xarray/namedarray/core.py#L252
Attributes = dict[Any, Any]


class NcAttributes(BaseModel):
    """Configure variable and global dataset attributes"""

    global_attributes: Annotated[
        Attributes,
        Field(
            description="Global attributes to add to the NetCDF file",
            default_factory=dict,
        ),
    ]

    variables: Annotated[
        dict[str, Attributes],
        Field(description="Variable-specific attributes", default_factory=dict),
    ]

    def apply_to_dataset(self, ds: xr.Dataset):
        """Apply the configured attributes to an xarray Dataset"""
        for var_name, attrs in self.variables.items():
            if var_name in ds:
                ds[var_name].attrs.update(attrs)

        ds.attrs.update(self.global_attributes)

    @classmethod
    def from_yaml(cls, path: Path):
        """Load attributes from a YAML file"""
        import yaml

        with path.open("r") as f:
            data = yaml.safe_load(f)

        return cls(**data)


class AttributeConfigMixin:
    """Mixin to add attribute configuration to a dataset or reader"""

    attributes: Annotated[
        NcAttributes,
        Field(
            default_factory=NcAttributes,
            description="NetCDF attributes to add to the dataset and variables",
        ),
    ]
