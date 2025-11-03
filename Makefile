build:
	docker compose build

up: down build
	# docker compose up --watch
	docker compose up

down:
	docker compose -f docker-compose.test.yaml -f docker-compose.yaml down

stop:
	docker compose stop

logs:
	docker compose logs -f

migrations:
	docker compose exec backend pixi run python manage.py makemigrations

blank-migration:
	# docker compose exec backend pixi run python manage.py makemigrations -n tide_data_types --empty deployments
	docker compose exec backend pixi run python manage.py makemigrations --empty deployments

migrate:
	docker compose exec backend pixi run python manage.py migrate

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
