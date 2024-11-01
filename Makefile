DOCKER_IMAGE = exercise-8
DOCKER_COMPOSE_FILE = docker-compose.yml

build:
	@echo "Removing existing Docker image (if any)..."
	@docker rmi -f $(DOCKER_IMAGE) || true
	@echo "Building Docker image..."
	@docker build --tag=$(DOCKER_IMAGE) .

test:
	@echo "Running tests with Docker Compose..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) up test

run:
	@echo "Starting application with Docker Compose..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) up run

clean:
	@echo "Cleaning up containers and networks..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) down

logs:
	@echo "Showing logs for the 'run' service..."
	@docker-compose -f $(DOCKER_COMPOSE_FILE) logs -f run

help:
	@echo "Available targets:"
	@echo "  make build       - Build the Docker image"
	@echo "  make test        - Run tests using Docker Compose"
	@echo "  make run         - Start the application using Docker Compose"
	@echo "  make clean       - Stop and remove containers and networks"
	@echo "  make logs        - View logs for the 'run' service"
	@echo "  make help        - Display this help message"