# Build all Docker compose services
build:
	docker compose build

# Start all services. Most likely want to use `make core` instead
# and then start specific pipelines as needed.
up: down build
	# docker compose up --watch
	docker compose up

# Start core services: backend, frontend, dagster_ui, dagster_daemon
core:
	docker compose up --build backend frontend dagster_ui dagster_daemon spotlight

# Stop and remove all containers
down:
	docker compose -f docker-compose.yaml down --remove-orphans

# Stop all containers without removing them
stop:
	docker compose stop

# View real-time logs for all services
logs:
	docker compose logs -f

# Run all pending migrations
migrations:
	docker compose exec backend pixi run python manage.py makemigrations

# Create a blank migration file inside the backend container
blank-migration:
	# docker compose exec backend pixi run python manage.py makemigrations -n tide_data_types --empty deployments
	docker compose exec backend pixi run python manage.py makemigrations --empty deployments

# Auto generate migrations for any model changes
migrate:
	docker compose exec backend pixi run python manage.py migrate

# Cleanup unused Docker resources
prune:
	docker volume rm $(shell docker volume ls -qf dangling=true)
	docker buildx prune -f
	docker system prune --volumes
	docker system prune -a

# Create a superuser inside the backend container
user:
	docker compose exec backend pixi run python manage.py createsuperuser

# Open a Django shell inside the backend container
shell:
	docker compose exec backend pixi run python manage.py shell
