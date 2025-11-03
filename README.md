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

I'd also suggest running `prek install` or `pre-commit install` to set up pre-commit hooks.

## Commands

- `make up` - Launch all Docker services
- `make down` - Stop all Docker services
- `make migrations` - Generate new Django database migration files
- `make migrate` - run all Django database migrations
- `make prune` - Remove old Docker images and other debris hanging around
- `make shell` - Start a Python shell in the Django backend

## Services

- `backend` - Django system administration with API via Django Ninja
- `db` - Timescale DB with PostGIS
