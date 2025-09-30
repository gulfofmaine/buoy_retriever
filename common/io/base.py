"""
A common base class for saving files to the EFS datastore.

This handles transforming an assets desired_path to a saved file.

It's largely modeled after dagster.UPathIOManager, but with some
adaptions to allow the use of path templates, instead of letting
UPathIOManager make things up on its own.
"""

from abc import abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional, Union

import dagster as dg
import pandas as pd

# from pydantic import Field
# from common import paths
# from ..resources.s3fs_resource import S3FSResource
# from .tags import ALLOW_MISSING_PARTITIONS, DESIRED_PATH, OUTPUT_PATH
from . import tags
from .datastore import Datastore


class PartitionedInputError(Exception):
    """
    An error that is raised when the input is partitioned
    and we need to make adjustments to handle that.
    """

    pass


class IOManagerBase(dg.ConfigurableIOManager):
    """
    A shared base class for format/tool specific IO Managers
    to be built that can work with the EFS datastore,
    and allows/requires assets to submit desired path template.

    If an input asset has partitions and is a dict, then return
    a dict of partition keys and values rather than a single value.
    """

    datastore: Datastore

    # sync_to_s3_bucket: Optional[str] = Field(
    #     None,
    #     description="Name of S3 bucket to sync output files to",
    # )
    # s3_default_access: bool = Field(
    #     False,
    #     description="Whether to make output files public on S3 by default",
    # )
    # s3: dg.ResourceDependency[Optional[S3FSResource]] = None

    def get_path(self, context: Union[dg.InputContext, dg.OutputContext]) -> Path:
        """Get file path"""
        if (
            isinstance(context, dg.InputContext)
            and context.has_asset_partitions
            and is_dict_type(context.dagster_type.typing_type)
        ):
            raise PartitionedInputError(
                "Input is partitioned, make sure the partition manager knows how to handle it",
            )

        partition_key = context.partition_key if context.has_partition_key else None
        path = self.get_output_path(context, partition_key)

        if isinstance(context, dg.OutputContext):
            self.prepare_for_output(context, path)

        return path

    def prepare_for_output(self, context, path):
        """Make directories and add metadata for output"""
        path.parent.mkdir(parents=True, exist_ok=True)
        context.add_output_metadata({tags.OUTPUT_PATH: str(path)})

    def get_output_path(
        self,
        context: Union[dg.InputContext, dg.OutputContext],
        partition_key: Optional[str] = None,
    ) -> Path:
        """Create path"""
        desired_path = self.desired_path_template(context)
        formatting_context = self.get_path_formatting_context(
            context,
            partition_key=partition_key,
        )
        formatted_path = desired_path.format_map(formatting_context)
        path = self.datastore.dataset_path() / formatted_path

        return path

    def desired_path_template(
        self,
        context: Union[dg.InputContext, dg.OutputContext],
    ) -> str:
        """Extract the template for the path desired from the desired path key in asset metadata"""
        try:
            desired_path: str = context.metadata[tags.DESIRED_PATH]
        except KeyError:
            try:
                desired_path = context.upstream_output.metadata[tags.DESIRED_PATH]
            except KeyError as e:
                raise KeyError(
                    f"Could not find `io.DESIRED_PATH` in asset output metadata: {context.metadata}",
                ) from e
        return desired_path

    def get_path_formatting_context(
        self,
        context: dg.OutputContext,
        partition_key: Optional[str] = None,
    ) -> dict:
        """Create path formatting context from output context"""
        path_context = {
            "context": context,
        }

        if partition_key:
            path_context["partition_key"] = partition_key
            path_context["partition_key_dt"] = pd.to_datetime(
                partition_key,
            ).to_pydatetime()

        return path_context

    @abstractmethod
    def dump_to_path(self, context: dg.OutputContext, obj: Any, path: Path) -> None:
        """Dump output to a given path"""

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        """Call subclass to handle dumping data to datastore.

        It will also handle the S3 sync if the sync_to_s3_bucket or a S3 path is set."""
        path = self.get_path(context)
        self.dump_to_path(context, obj, path)

        # if s3_path := self.get_s3_path(context, path):
        #     try:
        #         self.s3.fs.put_file(path, s3_path)
        #         if self.s3_public(context):
        #             self.s3.fs.chmod(s3_path, "public-read")

        #         bucket, obj_key = s3_path.split("/", 1)
        #         context.add_output_metadata(
        #             {
        #                 tags.S3_OUTPUT_PATH: dg.MetadataValue.url(f"s3://{s3_path}"),
        #                 tags.S3_URL: dg.MetadataValue.url(
        #                     f"https://{bucket}.s3.us-east-1.amazonaws.com/{obj_key}",
        #                 ),
        #             },
        #         )
        #     except AttributeError as e:
        #         raise AttributeError(
        #             f"Could not sync to S3 bucket {self.sync_to_s3_bucket}. "
        #             "Make sure the S3FSResource in `common_resources()` is configured correctly.",
        #         ) from e

    # def s3_public(self, context: dg.OutputContext) -> bool:
    #     """Whether to make the output public on S3"""
    #     try:
    #         return context.metadata[tags.S3_PUBLIC]
    #     except KeyError:
    #         return self.s3_default_access

    # def get_s3_path(self, context: dg.OutputContext, output_path: Path) -> Optional[str]:
    #     """Get the S3 path for the output.

    #     If S3_DESIRED_PATH is set in the metadata, use that,
    #     otherwise generate a path if using the sync_to_s3_bucket.
    #     """
    #     try:
    #         desired_path: str = context.metadata[tags.S3_DESIRED_PATH]
    #         partition_key = context.partition_key if context.has_partition_key else None
    #         formatting_context = self.get_path_formatting_context(
    #             context,
    #             partition_key=partition_key,
    #         )
    #         formatted_path = desired_path.format_map(formatting_context)
    #         return formatted_path
    #     except KeyError:
    #         pass
    #     if self.sync_to_s3_bucket:
    #         path = output_path.relative_to(paths.DATASET_PATH)
    #         return f"s3://{self.sync_to_s3_bucket}/{path}"
    #     return None

    @abstractmethod
    def load_from_path(self, context: dg.InputContext, path: Path):
        """Load input from a given path"""

    def load_input(self, context: dg.InputContext) -> Any:
        """Call subclass to load data into Dagster"""
        try:
            path = self.get_path(context)
            return self.load_from_path(context, path)
        except PartitionedInputError:
            partition_keys = context.asset_partition_keys

            partition_map = {}

            allow_missing_partitons = context.metadata.get(
                tags.ALLOW_MISSING_PARTITIONS,
                False,
            )

            for key in partition_keys:
                path = self.get_output_path(context, key)

                try:
                    obj = self.load_from_path(context, path)
                    partition_map[key] = obj
                except FileNotFoundError as e:
                    if allow_missing_partitons:
                        context.log.warning(
                            f"Could not find {path} for partition key {key}",
                        )
                    else:
                        msg = (
                            f"Could not find {path} for partition key {key}. "
                            "Set `AssetIn(metadata={io.ALLOW_MISSING_PARTITIONS=True})` "
                            " if this should be allowed."
                        )
                        raise FileNotFoundError(msg) from e

            return partition_map


def is_dict_type(type_obj) -> bool:
    """Check if a type is a dict or a reasonable subclass"""
    if isinstance(type_obj, dict):
        return True

    try:
        if type_obj.__origin__ in {dict, Mapping}:
            return True

    except AttributeError:
        pass

    return False
