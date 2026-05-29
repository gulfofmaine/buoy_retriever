"""Tests for load_dataset and dump_dataset management commands."""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from django.core.management import call_command

from datasets.models import Dataset, DatasetConfig
from pipelines.models import Pipeline


DATASET_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
CONFIG_TIME = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)


def _make_fixture_json(pipeline_slug, dataset_slug, configs=None):
    """Build JSON fixture content matching Django's natural-key serialization format."""
    objects = [
        {
            "model": "datasets.dataset",
            "fields": {
                "slug": dataset_slug,
                "pipeline": [pipeline_slug],
                "state": "Active",
                "created": DATASET_TIME.isoformat(),
                "edited": DATASET_TIME.isoformat(),
            },
        },
    ]
    for i, cfg in enumerate(configs or []):
        objects.append(
            {
                "model": "datasets.datasetconfig",
                "pk": cfg.get("pk", i + 1),
                "fields": {
                    "dataset": [dataset_slug],
                    "config": cfg.get("config", {}),
                    "state": cfg.get("state", DatasetConfig.State.DRAFT),
                    "created": cfg.get("created", CONFIG_TIME.isoformat()),
                    "edited": cfg.get("edited", CONFIG_TIME.isoformat()),
                },
            },
        )
    return json.dumps(objects)


def _write_fixture(content):
    """Write content to a temp JSON file and return its Path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False)
    tmp.write(content)
    tmp.close()
    return Path(tmp.name)


@pytest.fixture
def pipeline(db):
    return Pipeline.objects.create(
        slug="test-pipeline",
        name="Test Pipeline",
        config_schema={},
        description="A test pipeline",
    )


# ── create in empty database ──────────────────────────────────────────────────


@pytest.mark.django_db
def test_creates_dataset_when_none_exists(pipeline):
    """load_dataset creates a new dataset when none exists in the database."""
    fixture_path = _write_fixture(_make_fixture_json("test-pipeline", "new-dataset"))

    call_command("load_dataset", "new-dataset", input_path=fixture_path, write=True)

    dataset = Dataset.objects.get(slug="new-dataset")
    assert dataset.pipeline == pipeline
    assert dataset.state == Dataset.State.ACTIVE


@pytest.mark.django_db
def test_creates_config_for_new_dataset(pipeline):
    """load_dataset creates the dataset and associated config when neither exist."""
    fixture_path = _write_fixture(
        _make_fixture_json(
            "test-pipeline",
            "new-dataset",
            configs=[
                {"config": {"param": "value"}, "state": DatasetConfig.State.PUBLISHED},
            ],
        ),
    )

    call_command("load_dataset", "new-dataset", input_path=fixture_path, write=True)

    dataset = Dataset.objects.get(slug="new-dataset")
    assert dataset.configs.count() == 1
    config = dataset.configs.first()
    assert config.config == {"param": "value"}
    assert config.state == DatasetConfig.State.PUBLISHED


# ── update existing records ───────────────────────────────────────────────────


@pytest.mark.django_db
def test_updates_existing_dataset_without_creating_duplicate(pipeline):
    """load_dataset updates an existing dataset without creating a second one."""
    Dataset.objects.create(
        slug="existing-dataset",
        pipeline=pipeline,
        state=Dataset.State.DISABLED,
    )
    fixture_path = _write_fixture(
        _make_fixture_json("test-pipeline", "existing-dataset"),
    )

    call_command(
        "load_dataset",
        "existing-dataset",
        input_path=fixture_path,
        write=True,
    )

    assert Dataset.objects.filter(slug="existing-dataset").count() == 1
    dataset = Dataset.objects.get(slug="existing-dataset")
    assert dataset.state == Dataset.State.ACTIVE


@pytest.mark.django_db
def test_updates_existing_config_matched_by_timestamp(pipeline):
    """load_dataset updates an existing config when the fixture's created timestamp matches."""
    dataset = Dataset.objects.create(slug="test-dataset", pipeline=pipeline)
    config = DatasetConfig.objects.create(
        dataset=dataset,
        config={"old": "data"},
        state=DatasetConfig.State.DRAFT,
    )
    # Bypass auto_now_add to pin the created timestamp to the fixture value.
    DatasetConfig.objects.filter(pk=config.pk).update(created=CONFIG_TIME)

    fixture_path = _write_fixture(
        _make_fixture_json(
            "test-pipeline",
            "test-dataset",
            configs=[
                {
                    "pk": 999,  # Intentionally different pk from the DB record
                    "config": {"new": "data"},
                    "state": DatasetConfig.State.PUBLISHED,
                    "created": CONFIG_TIME.isoformat(),
                },
            ],
        ),
    )

    call_command("load_dataset", "test-dataset", input_path=fixture_path, write=True)

    assert dataset.configs.count() == 1
    config.refresh_from_db()
    assert config.config == {"new": "data"}
    assert config.state == DatasetConfig.State.PUBLISHED


# ── duplicate primary keys ────────────────────────────────────────────────────


@pytest.mark.django_db
def test_loading_same_fixture_twice_does_not_duplicate_records(pipeline):
    """Loading the same fixture a second time updates existing records instead of creating duplicates.

    This covers the case raised in PR #137 where fixture pk values could otherwise
    cause conflicts — load_dataset strips pks and matches by dataset slug + created
    timestamp, so a repeated load is idempotent.
    """
    fixture_path = _write_fixture(
        _make_fixture_json(
            "test-pipeline",
            "test-dataset",
            configs=[{"pk": 42, "config": {"key": "value"}}],
        ),
    )

    call_command("load_dataset", "test-dataset", input_path=fixture_path, write=True)
    call_command("load_dataset", "test-dataset", input_path=fixture_path, write=True)

    assert Dataset.objects.filter(slug="test-dataset").count() == 1
    assert DatasetConfig.objects.filter(dataset__slug="test-dataset").count() == 1


@pytest.mark.django_db
def test_fixture_pk_does_not_overwrite_config_with_different_timestamp(pipeline):
    """A fixture config whose pk matches an existing DB config is not overwritten
    when the created timestamps differ.

    Since load_dataset strips fixture pks and matches by timestamp, a config in the
    fixture with pk=X won't modify an unrelated existing config that also has pk=X.
    """
    dataset = Dataset.objects.create(slug="test-dataset", pipeline=pipeline)
    existing = DatasetConfig.objects.create(
        dataset=dataset,
        config={"original": "data"},
        state=DatasetConfig.State.DRAFT,
    )
    DatasetConfig.objects.filter(pk=existing.pk).update(created=CONFIG_TIME)

    # Fixture references the same pk but has a different created timestamp.
    different_time = CONFIG_TIME + timedelta(hours=1)
    fixture_path = _write_fixture(
        _make_fixture_json(
            "test-pipeline",
            "test-dataset",
            configs=[
                {
                    "pk": existing.pk,
                    "config": {"new": "data"},
                    "created": different_time.isoformat(),
                },
            ],
        ),
    )

    call_command("load_dataset", "test-dataset", input_path=fixture_path, write=True)

    existing.refresh_from_db()
    assert existing.config == {"original": "data"}, (
        "Config should not be overwritten when fixture pk matches but timestamps differ"
    )
    assert (
        dataset.configs.count() == 2
    )  # New config was created for the different timestamp


# ── dry run ───────────────────────────────────────────────────────────────────


@pytest.mark.django_db
def test_dry_run_does_not_write_to_database(pipeline):
    """Without --write, load_dataset makes no changes to the database."""
    fixture_path = _write_fixture(
        _make_fixture_json("test-pipeline", "dry-run-dataset"),
    )

    call_command("load_dataset", "dry-run-dataset", input_path=fixture_path)

    assert not Dataset.objects.filter(slug="dry-run-dataset").exists()


# ── timestamp fuzzy matching ──────────────────────────────────────────────────


@pytest.mark.django_db
def test_config_matched_within_one_second_window(pipeline):
    """A config in the DB within 1 second of the fixture timestamp is matched and updated."""
    dataset = Dataset.objects.create(slug="test-dataset", pipeline=pipeline)
    config = DatasetConfig.objects.create(
        dataset=dataset,
        config={"v": 1},
        state=DatasetConfig.State.DRAFT,
    )
    # DB timestamp is 500ms after the fixture value — still within the 1-second window.
    DatasetConfig.objects.filter(pk=config.pk).update(
        created=CONFIG_TIME + timedelta(milliseconds=500),
    )

    fixture_path = _write_fixture(
        _make_fixture_json(
            "test-pipeline",
            "test-dataset",
            configs=[{"config": {"v": 2}, "created": CONFIG_TIME.isoformat()}],
        ),
    )

    call_command("load_dataset", "test-dataset", input_path=fixture_path, write=True)

    assert dataset.configs.count() == 1
    config.refresh_from_db()
    assert config.config == {"v": 2}


# ── dump_dataset ──────────────────────────────────────────────────────────────


@pytest.mark.django_db
def test_dump_writes_dataset_and_configs_to_file(pipeline):
    """dump_dataset writes a JSON file containing the dataset and all its configs."""
    dataset = Dataset.objects.create(slug="test-dataset", pipeline=pipeline)
    DatasetConfig.objects.create(
        dataset=dataset,
        config={"a": 1},
        state=DatasetConfig.State.PUBLISHED,
    )
    DatasetConfig.objects.create(
        dataset=dataset,
        config={"b": 2},
        state=DatasetConfig.State.DRAFT,
    )

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        output_path = Path(f.name)

    call_command("dump_dataset", "test-dataset", output_path=output_path)

    payload = json.loads(output_path.read_text())
    models = [obj["model"] for obj in payload]
    assert models.count("datasets.dataset") == 1
    assert models.count("datasets.datasetconfig") == 2


@pytest.mark.django_db
def test_dump_then_load_updates_existing_dataset(pipeline):
    """A file produced by dump_dataset can be fed back to load_dataset to restore state."""
    dataset = Dataset.objects.create(slug="test-dataset", pipeline=pipeline)
    config = DatasetConfig.objects.create(
        dataset=dataset,
        config={"original": "data"},
        state=DatasetConfig.State.DRAFT,
    )

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        output_path = Path(f.name)

    call_command("dump_dataset", "test-dataset", output_path=output_path)

    # Mutate DB state after dumping.
    config.config = {"modified": "data"}
    config.save()

    # Reloading from the dump should restore the original config.
    call_command("load_dataset", "test-dataset", input_path=output_path, write=True)

    config.refresh_from_db()
    assert config.config == {"original": "data"}
