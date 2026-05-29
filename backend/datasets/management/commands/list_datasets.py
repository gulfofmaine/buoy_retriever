from django.core.management.base import BaseCommand
from datasets.models import Dataset


class Command(BaseCommand):
    help = "Lists all datasets and their configs for testing purposes"

    def handle(self, *args, **options):
        datasets = Dataset.objects.all().prefetch_related("configs")
        for dataset in datasets:
            print(f"Dataset: {dataset}")
            configs = dataset.configs.all()
            for config in configs:
                print(f"  Config: {config}")
