from typing import Annotated

from pydantic import BaseModel, Field


class S3SourceConfig(BaseModel):
    """Configuration for data coming from an S3 Bucket"""

    bucket: Annotated[str, Field(description="The S3 bucket name")]
    prefix: Annotated[
        str,
        Field(description="The S3 prefix/folder where files are located"),
    ] = "/"
    # region: Annotated[
    #     str,
    #     Field(description="The AWS region where the bucket is located"),
    # ] = "us-east-1"


class S3SourceMixin:
    """Mixin to add S3 source configuration to a dataset or reader"""

    s3_source: Annotated[
        S3SourceConfig,
        Field(
            default_factory=S3SourceConfig,
            description="Configuration for accessing data in S3",
        ),
    ]
