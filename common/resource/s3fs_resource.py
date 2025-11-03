"""Reusable S3FS resource"""

from typing import TYPE_CHECKING

import dagster as dg
from pydantic import PrivateAttr

if TYPE_CHECKING:
    import s3fs


class S3Credentials(dg.ConfigurableResource):
    """S3 credentials"""

    access_key_id: str
    secret_access_key: str


class S3FSResource(dg.ConfigurableResource):
    """Reusable S3FS resource"""

    credentials: dg.ResourceDependency[S3Credentials]
    region_name: str

    _fs: "s3fs.S3FileSystem" = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Prep the resource by caching the S3FileSystem instance"""
        from s3fs import S3FileSystem

        _fs = S3FileSystem(
            key=self.credentials.access_key_id,
            secret=self.credentials.secret_access_key,
            client_kwargs={"region_name": self.region_name},
        )
        self._fs = _fs

    @property
    def fs(self) -> "s3fs.S3FileSystem":
        """Access the S3 FsSpec instance"""
        return self._fs
