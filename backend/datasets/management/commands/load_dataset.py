from pathlib import Path
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.core import serializers
from django.db import transaction, DatabaseError

from datasets.models import Dataset, DatasetConfig


class Command(BaseCommand):
    help = "Loads a dataset and its config from a JSON file"

    def _find_existing_config(self, dataset_slug, created):
        """Find an existing config for the given dataset slug and created datetime.
        If an exact match is not found, find the nearest config within a 1 second window to allow for differences in timestamp precision"""
        exact_match = DatasetConfig.objects.filter(
            dataset__slug=dataset_slug,
            created=created,
        ).first()
        if exact_match:
            return exact_match

        candidates = list(
            DatasetConfig.objects.filter(
                dataset__slug=dataset_slug,
                created__gte=created - timedelta(seconds=1),
                created__lte=created + timedelta(seconds=1),
            ),
        )
        if not candidates:
            return None

        nearest = min(candidates, key=lambda config: abs(config.created - created))
        self.stdout.write(
            self.style.WARNING(
                "Using nearest created-time match for "
                f"{dataset_slug} config: fixture={created.isoformat()} db={nearest.created.isoformat()}",
            ),
        )
        return nearest

    def add_arguments(self, parser):
        parser.add_argument("dataset_slug", type=str)
        parser.add_argument(
            "--input_path",
            type=Path,
            help="Path to load the JSON file from instead of searching",
        )
        parser.add_argument(
            "--write",
            "-w",
            action="store_true",
            help="Write the updated configs to the database",
        )

    def handle(self, *args, **options):
        verbose = options["verbosity"]
        if verbose > 1:
            self.stdout.write(f"Loading dataset with slug: {options['dataset_slug']}")

        input_path = options["input_path"]
        if not input_path:
            base_path = Path("/mnt/test-data")
            fixtures = list(
                base_path.glob(f"*/fixtures/{options['dataset_slug']}.json"),
            )
            if len(fixtures) == 0:
                raise CommandError(
                    f"No fixture found for dataset slug '{options['dataset_slug']}' in {base_path}",
                )
            elif len(fixtures) > 1:
                raise CommandError(
                    f"Multiple fixtures found for dataset slug '{options['dataset_slug']}' in {base_path}: {fixtures}",
                    "Please specify the input path with --input_path",
                )
            input_path = fixtures[0]
            self.stdout.write(f"Loading from found fixture: {input_path}")

        if not input_path.exists():
            raise CommandError(f"Input file {input_path} does not exist")

        with input_path.open() as f:
            data = f.read()

        objects = []
        dataset_obj = None

        for obj in serializers.deserialize(
            "json",
            data,
            use_natural_foreign_keys=True,
            use_natural_primary_keys=True,
        ):
            if verbose > 1:
                self.stdout.write(f"Loading object: {obj.object}")

            if isinstance(obj.object, Dataset):
                existing = Dataset.objects.filter(slug=obj.object.slug).first()
                if existing:
                    # existing.update_from(obj.object)
                    existing.slug = obj.object.slug
                    existing.pipeline = obj.object.pipeline
                    existing.created = obj.object.created
                    existing.edited = obj.object.edited
                    existing.state = obj.object.state
                    self.stdout.write(f"Updating dataset with slug {obj.object.slug}")
                    objects.append(existing)
                    dataset_obj = existing
                else:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Creating new dataset with slug {obj.object.slug}",
                        ),
                    )
                    objects.append(obj.object)
                    dataset_obj = obj.object

            elif isinstance(obj.object, DatasetConfig):
                if dataset_obj is None:
                    raise CommandError(
                        "DatasetConfig encountered before Dataset in fixture; cannot associate config.",
                    )

                existing = self._find_existing_config(
                    dataset_slug=obj.object.dataset.slug,
                    created=obj.object.created,
                )
                if existing:
                    existing.config = obj.object.config
                    existing.state = obj.object.state
                    existing.created = obj.object.created
                    existing.edited = obj.object.edited
                    existing.dataset = dataset_obj
                    self.stdout.write(
                        f"Updating config for dataset {obj.object.dataset.slug} with created datetime {obj.object.created}",
                    )
                    objects.append(existing)
                else:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Creating new config for dataset {obj.object.dataset.slug} with created datetime {obj.object.created}",
                        ),
                    )
                    obj.object.dataset = dataset_obj
                    objects.append(obj.object)

        if not options["write"]:
            self.stdout.write(
                self.style.WARNING(
                    "Dry run mode - not saving to database. Use --write or -w to write changes.",
                ),
            )
            return

        try:
            with transaction.atomic():
                for obj in objects:
                    obj.save()
                    self.stdout.write(self.style.SUCCESS(f"Saved {obj}"))
        except DatabaseError as e:
            raise CommandError(
                f"Database error occurred while saving. Rolling back transaction: {e}",
            )
