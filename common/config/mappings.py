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



class SplitOperator(BaseModel):
    ''' Takes the source variable, splits it on the seperator and  maps the resulting array to new variables'''
    
    sep : Annotated [
        str,
        Field(description="The seperator")]
    
    output_variables : Annotated[
        dict[int,str],
        Field(description="Mapping of index number to output variable." )]
    
    
    source_variable : Annotated[
        str,
        Field(description="The source variable to split into multiple columns")]
             
class SplitOperations(BaseModel):
    split_operations : Annotated[
        list[SplitOperator],
        Field(description="List of variables to split into multiple variables")]


class VariableConverterMixIn:
    ''' Mixin to add column coversion rules to a dataset '''
    variable_converter : Annotated [
        SplitOperations,
        Field(
            description="Split variable converter")
        ] =None                       

    
    
class ProfileDepthMappings(BaseModel):
    depth : Annotated [
        float,
        Field( 
           description="Optional- fixed depth for the mapping."
        )
        ] = None
  
    mappings : Annotated [
        dict[str,str],
         Field(
            description="Maps input variables to output variables at the current depth ",
        )
        ]

    

class OptionalProfileDepthMixin:
    ''' Mixin to add profile depth mappings configuration to a dataset'''

    profile_data: Annotated[
        list[ProfileDepthMappings], 
         Field(
            description="Mapping for variables with multiple depth levels",
        )
        ] =None
    