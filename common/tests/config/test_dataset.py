from pathlib import Path

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


def test_dataset_from_fixture():
    path = Path("docker-data/test-data/s3_timeseries/fixtures/empire_met_frontend.json")
    created_dt_str = "2026-01-05T21:15:24.530Z"
    dataset = DatasetTest.from_fixture(path, created_dt_str)
    assert dataset.slug == "empire-met-frontend"
    assert dataset.config_state == ConfigState.DRAFT
    assert dataset.config.test_field == "test"
