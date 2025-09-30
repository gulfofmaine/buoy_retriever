from pathlib import Path

import xarray as xr

from common.config.attributes import NcAttributes


def test_apply_attributes():
    ds = xr.Dataset(
        {
            "var1": (("x", "y"), [[1, 2], [3, 4]]),
            "var2": (("x", "y"), [[5, 6], [7, 8]]),
        },
        coords={"x": [0, 1], "y": [0, 1]},
    )

    attrs = NcAttributes(
        global_attributes={"title": "Test Dataset", "institution": "Test Institute"},
        variables={
            "var1": {"units": "meters", "long_name": "Variable 1"},
            "var2": {"units": "seconds", "long_name": "Variable 2"},
        },
    )

    attrs.apply_to_dataset(ds)

    assert ds.attrs["title"] == "Test Dataset"
    assert ds.attrs["institution"] == "Test Institute"
    assert ds["var1"].attrs["units"] == "meters"
    assert ds["var1"].attrs["long_name"] == "Variable 1"
    assert ds["var2"].attrs["units"] == "seconds"
    assert ds["var2"].attrs["long_name"] == "Variable 2"


def test_load_attributes_from_yaml():
    path = Path(__file__).parent / "attributes.yaml"
    attrs = NcAttributes.from_yaml(path)

    assert attrs.global_attributes["project"] == "NERACOOS"
    assert attrs.global_attributes["institution"] == "Gulf of Maine Research Institute"
    assert attrs.variables["time"]["standard_name"] == "time"
    assert attrs.variables["time"]["axis"] == "T"
    assert attrs.variables["latitude"]["units"] == "degrees_north"
    assert attrs.variables["latitude"]["long_name"] == "Latitude"
