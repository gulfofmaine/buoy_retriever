"""
Save and load JSON data to the EFS datastore
"""

import json
from pathlib import Path
from typing import Any

from dagster import InputContext, OutputContext
from pydantic import BaseModel

from .base import IOManagerBase


class JsonIOManager(IOManagerBase):
    """Save and load JSON data to the datastore"""

    def dump_to_path(self, context: OutputContext, obj: Any, path: Path) -> None:
        """Save JSON data to given path"""
        if isinstance(obj, BaseModel):
            try:
                json_str = obj.model_dump_json(indent=4)
            except AttributeError:  # Pydantic v1
                json_str = obj.json(indent=4)
            path.write_text(json_str)

        else:
            with path.open("w") as f:
                json.dump(obj, f, indent=4)

    def load_from_path(self, context: InputContext, path: Path):
        """Load JSON data from a given path"""
        with path.open() as f:
            obj = json.load(f)

        if issubclass(context.dagster_type.typing_type, BaseModel):
            obj = context.dagster_type.typing_type(**obj)

        return obj
