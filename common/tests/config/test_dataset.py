from common.config import ConfigState, DatasetBase, DatasetConfigBase


class DatasetTestConfig(DatasetConfigBase):
    """Test Dataset Config"""

    test_field: str


class DatasetTest(DatasetBase):
    """Test Dataset"""

    config: DatasetTestConfig


def test_dataset_config():
    """Test DatasetConfigBase"""
    config = DatasetTestConfig(test_field="test")
    assert config.test_field == "test"
    assert config.model_dump() == {"test_field": "test"}


def test_dataset_config_schema():
    """Test DatasetConfigBase JSON Schema"""
    assert DatasetTestConfig.model_json_schema() == {
        "title": "DatasetTestConfig",
        "type": "object",
        "properties": {
            "test_field": {"title": "Test Field", "type": "string"},
        },
        "required": ["test_field"],
        "description": "Test Dataset Config",
    }


def test_dataset():
    dataset = DatasetTest(
        slug="test-dataset",
        config=DatasetTestConfig(test_field="test"),
    )
    assert dataset.slug == "test-dataset"
    assert dataset.safe_slug == "test_dataset"
    assert dataset.config.test_field == "test"
    assert dataset.model_dump() == {
        "slug": "test-dataset",
        "config": {"test_field": "test"},
        "config_state": ConfigState.DRAFT,
    }
