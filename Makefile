# Variables
MASTER_CMD=./cmd/master
WORKER_CMD=./cmd/worker

MASTER_BIN=bin/master
WORKER_BIN=bin/worker

DOCKER_COMPOSE=deploy/docker-compose.yml

# Crear carpeta bin si no existe
prepare:
	@mkdir -p bin

# Compilar master y worker
build: prepare
	go build -o $(MASTER_BIN) $(MASTER_CMD)
	go build -o $(WORKER_BIN) $(WORKER_CMD)

# Construir imágenes Docker
docker-build:
	docker build -t batchdag-master -f Dockerfile.master .
	docker build -t batchdag-worker -f Dockerfile.worker .

# Levantar el cluster
up:
	docker compose -f $(DOCKER_COMPOSE) up --build -d

# Parar el cluster
down:
	docker compose -f $(DOCKER_COMPOSE) down

# Ver logs
logs:
	docker compose -f $(DOCKER_COMPOSE) logs -f

# Limpiar binarios
clean:
	rm -rf bin
