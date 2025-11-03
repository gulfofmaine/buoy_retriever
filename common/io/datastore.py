"""
Shared resource for configuring workflow access to the EFS datastore.
"""

from contextlib import contextmanager
from pathlib import Path

from dagster import ConfigurableResource
from pydantic import Field

from common import paths


class Datastore(ConfigurableResource):
    """Access the EFS datastore"""

    path_stub: str = Field(
        description="Path within datastore or scratch to store all repo data in",
    )

    def dataset_path(self) -> Path:
        """Base path to persist datasets to"""
        return paths.DATASET_PATH / self.path_stub

    def scratch_path(self) -> Path:
        """Scratch directory for ephemeral data"""
        scratch_path = paths.SCRATCH_PATH / self.path_stub
        scratch_path.mkdir(parents=True, exist_ok=True)

        return scratch_path

    @contextmanager
    def temp_dir(self):
        """Yields a temporary directory in the scratch directory"""
        from tempfile import TemporaryDirectory

        with TemporaryDirectory(
            dir=self.scratch_path(),
            prefix="ioos-temp-",
        ) as temp_dir:
            temp_dir = Path(temp_dir)
            yield temp_dir
