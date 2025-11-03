# buoy_retriever

![Logo](./docs/logo.jpg)

IOOS Offshore Operations data management system

## Structure

This is setup as a mono-repo with a `backend/` Django server exposing Django-Ninja API endpoints and acting as a centeralized dataset configuration management system with Django's admin & auth systems for low level functionality.

Various `pipeline/`s report their capabilities into the Django backend, and retrieve dataset configurations for them to process. These will largely be implemented in Dagster, but there is the potential for other pipeline (AWS Lambda, Prefect, ...) to interact with the same backend and frontend.

Shared tooling will live in `common/` where they can be incorporated into the backend, pipelines, as well as potentially be published as a PyPI library that can be used in pipelines not included in this repo.

Most users (including RA/DAC admins) however, will be interacting with a Javascript `frontend/` which helps them configure their datasets based on the pipelines capabilities.

## Getting started

In `docker-data/secret.env` a few values need to be set before Django can be started

```
POSTGRES_PASSWORD=something_secret
POSTGRES_USER=buoy_retriever
POSTGRES_NAME=buoy_retriever
POSTGRES_HOST=db
BACKEND_SECRET_KEY=something_complex_and_random
```

Once the secret (/shared) values are set, `docker compose up --build backend` will start the database, cache, queue, and Django allowing for initial configuration.

The next steps are to create database tables with `make migrate` and then to create a superuser with `make user`.

Then to see the API docs go to http://localhost:8080/backend/api/docs and access the admin at http://localhost:8080/backend/admin/

I'd also suggest running `prek install` or `pre-commit install` to set up pre-commit hooks.

Once the migrations are run, the frontend and Dagster can be started with `make core`, though there won't be much functionality in either until one of the pipelines (I'd suggest Hohonu after adding an API key) is launched and has registered itself with the backend.

## Commands

- `make up` - Launch all Docker services
- `make down` - Stop all Docker services
- `make migrations` - Generate new Django database migration files
- `make migrate` - run all Django database migrations
- `make prune` - Remove old Docker images and other debris hanging around
- `make shell` - Start a Python shell in the Django backend

## Services

- [`backend`](./backend/) - Django system administration with API via Django Ninja
- `db` - Timescale DB with PostGIS
- [`frontend`](./frontend/) - NextJS dataset management
- [`dagster_ui` and `dagster_daemon`](./pipeline/_dagster/) - Dagster pipeline orchastration

## Pipelines

Pipelines can be managed from [Dagster UI](http://localhost:3002)

### S3 Timeseries

[`s3_timeseries`](./pipeline/s3_timeseries/)

Requires `S3_TS_ACCESS_KEY_ID` and `S3_TS_SECRET_ACCESS_KEY` environment variables for S3 access.

### Hohonu

[`hohonu`](./pipeline/hohonu/)

Requires a `HOHONU_API_KEY` environment variable for API access.


## Testing

Currently there is some testing for common utilities and Hohonu pipelines.

For common, cd into `common/` then `uv run pytest`.

For Hohonu, `docker compose exec hohonu pixi run pytest`.
