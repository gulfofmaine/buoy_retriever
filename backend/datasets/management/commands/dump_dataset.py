from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.core import serializers

from datasets.models import Dataset


class Command(BaseCommand):
    help = "Saves a dataset and its config to a JSON file for testing purposes"

    def add_arguments(self, parser):
        parser.add_argument("dataset_slug", type=str)
        parser.add_argument(
            "--output_path",
            type=Path,
            help="Path to save the JSON file",
        )

    def handle(self, *args, **options):
        self.stdout.write(f"Dumping dataset with slug: {options['dataset_slug']}")

        try:
            dataset = Dataset.objects.get(slug=options["dataset_slug"])
        except Dataset.DoesNotExist:
            raise CommandError(
                f"Dataset with slug '{options['dataset_slug']}' does not exist",
            )

        configs = dataset.configs.all()
        self.stdout.write(f"Dataset: {dataset}")
        for config in configs:
            self.stdout.write(f"  Config: {config}")

        dump_path = Path(
            f"/mnt/test-data/{dataset.pipeline.slug}/fixtures/{dataset.slug}.json",
        )
        if options["output_path"]:
            dump_path = options["output_path"]
        else:
            self.stdout.write(f"Dumping to default path: {dump_path}")

        self.stdout.write("Serializing dataset and configs...")
        data = serializers.serialize(
            "json",
            [dataset, *configs],
            indent=4,
            use_natural_foreign_keys=True,
            use_natural_primary_keys=True,
        )

        dump_path.parent.mkdir(parents=True, exist_ok=True)

        with dump_path.open("w") as f:
            f.write(data)

        self.stdout.write(
            self.style.SUCCESS(f"Dataset and configs dumped to {dump_path}"),
        )
