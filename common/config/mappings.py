from typing import Annotated

from pydantic import BaseModel, Field


class VarMap(BaseModel):
    """Configure a single variable mapping"""

    source: Annotated[
        str,
        Field(description="The source variable name in the input data"),
    ]
    output: Annotated[
        str,
        Field(description="The output variable name in the dataset"),
    ]


class VariableMappingMixin:
    """Mixin to add variable mapping configuration to a dataset or reader"""

    variable_mappings: Annotated[
        list[VarMap],
        Field(
            description="Variable name mappings, source to output dataset destination name",
            default_factory=list,
        ),
    ]


class DepthMap(BaseModel):
    """Configure depth mapping for a single variable"""

    source_variable: Annotated[
        str,
        Field(description="The source variable name in the input data"),
    ]
    depth: Annotated[
        int,
        Field(description="The depth (in meters) for this variable"),
    ]


class DepthGroup(BaseModel):
    """Configure depth mappings for a variable"""

    output_variable: Annotated[
        str,
        Field(description="The output variable name in the dataset"),
    ]
    depths: Annotated[
        list[DepthMap],
        Field(description="List of source variables and their corresponding depths"),
    ]


class DepthMappingMixin:
    """Mixin to add depth mapping configuration to a dataset or reader"""

    depth_mappings: Annotated[
        list[DepthGroup],
        Field(
            description="Depth mappings for variables with multiple depth levels",
            default_factory=list,
        ),
    ]


class OptionalDepthMappingMixin:
    """Mixin to add depth mapping configuration to a dataset or reader"""

    depth_mappings: Annotated[
        list[DepthGroup] | None,
        Field(
            description="Depth mappings for variables with multiple depth levels",
        ),
    ] = None
