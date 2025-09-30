from common.config import DatasetConfigBase, PipelineConfig


class DatasetTestConfig(DatasetConfigBase):
    """Test Dataset Config"""

    test_field: str


def test_pipeline_config():
    """Test PipelineConfig"""
    pipeline = PipelineConfig(
        slug="test-pipeline",
        name="Test Pipeline",
        description="A test pipeline",
        dataset_config=DatasetTestConfig,
    )
    assert pipeline.slug == "test-pipeline"
    assert pipeline.name == "Test Pipeline"
    assert pipeline.description == "A test pipeline"
    assert pipeline.to_json() == {
        "slug": "test-pipeline",
        "name": "Test Pipeline",
        "description": "A test pipeline",
        "config_schema": {
            "title": "DatasetTestConfig",
            "type": "object",
            "properties": {
                "test_field": {"title": "Test Field", "type": "string"},
            },
            "required": ["test_field"],
            "description": "Test Dataset Config",
        },
    }
