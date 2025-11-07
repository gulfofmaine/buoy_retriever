from typing import Annotated

import httpx
import sentry_sdk
from pydantic import BaseModel, Field, ValidationError

from .config import DatasetBase, PipelineConfig


class BackendAPIClient(BaseModel):
    api_endpoint: str = "http://backend:8080/backend/api/"
    # api_key: str
    timeout: Annotated[
        int,
        Field(
            description="Default seconds before timing out connecting to backend API",
        ),
    ] = 30

    def headers(self):
        """Add API key to headers"""
        return {}

    def register_pipeline(self, pipeline: PipelineConfig):
        """Create or update a pipeline configuration"""
        with sentry_sdk.start_span(
            op="register_pipeline",
            name=f"Register pipeline {pipeline.slug}",
        ):
            json = pipeline.to_json()

            url = self.api_endpoint + "pipelines/"

            result = httpx.post(
                url,
                json=json,
                timeout=self.timeout,
                headers=self.headers(),
            )
            result.raise_for_status()

            return result.json()

    def datasets_for_pipeline(
        self,
        pipeline_slug: str,
        dataset_model: type[DatasetBase],
    ):
        """Get datasets for a given pipeline slug"""
        with sentry_sdk.start_span(
            op="datasets_for_pipeline",
            name=f"Get datasets for pipeline {pipeline_slug}",
        ):
            url = self.api_endpoint + f"datasets/by-pipeline/{pipeline_slug}"

            result = httpx.get(
                url,
                timeout=self.timeout,
                headers=self.headers(),
            )
            result.raise_for_status()

            datasets_json = result.json()

            datasets = []

            for d in datasets_json:
                try:
                    dataset = dataset_model(**d)
                except ValidationError as e:
                    print(f"Error validating dataset {d}: {e}")
                    print(
                        "Post back to message API about error parsing the dataset configuration",
                    )
                    continue
                datasets.append(dataset)

            return datasets
