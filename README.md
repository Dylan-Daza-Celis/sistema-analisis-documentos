# Distributed Document System

Sistema distribuido para clasificación y recuperación de documentos PDF sin uso de base de datos.

## Arquitectura
- Microservicios
- Comunicación por eventos (Kafka)
- Almacenamiento distribuido (MinIO)
- Sin base de datos (uso de metadata e índices en storage)

## Servicios
- API Gateway
- Auth Service
- Document Service
- Classification Service
- Citation Service

## Cómo correr en una sola máquina (validación previa)
```bash
cd infrastructure
set KAFKA_KRAFT_CLUSTER_ID=UUID_GENERADO
docker compose up --build
```

## Preparación para Docker Swarm (3 nodos)
1) Inicializar Swarm con 3 managers (HA):
```bash
docker swarm init
docker swarm join-token manager
```

2) Unir nodo2 y nodo3 como managers:
```bash
docker swarm join --token TOKEN_MANAGER IP_MANAGER:2377
```

3) Agregar workers para workloads (recomendado 2):
```bash
docker swarm join-token worker
docker swarm join --token TOKEN_WORKER IP_MANAGER:2377
```

4) Crear red overlay (obligatorio antes del deploy):
```bash
docker network create --driver overlay --attachable sistema-network
```

5) Crear registry privado en el nodo manager:
```bash
docker service create --name registry --publish 5000:5000 registry:2
```

6) Crear directorios de persistencia en TODOS los nodos:
```bash
sudo mkdir -p /mnt/distributed/minio1
sudo mkdir -p /mnt/distributed/minio2
sudo mkdir -p /mnt/distributed/minio3
sudo mkdir -p /mnt/distributed/minio4
sudo mkdir -p /mnt/distributed/kafka1
sudo mkdir -p /mnt/distributed/kafka2
sudo mkdir -p /mnt/distributed/kafka3
```

7) Generar el cluster id de Kafka (KRaft) y exportarlo:
```bash
kafka-storage.sh random-uuid
set KAFKA_KRAFT_CLUSTER_ID=UUID_GENERADO
```

8) Construir y publicar imágenes usando la IP/hostname real del manager:
```bash
set REGISTRY=192.168.1.10:5000
docker build -t %REGISTRY%/api-gateway:latest api-gateway
docker build -t %REGISTRY%/auth-service:latest services/auth-service
docker build -t %REGISTRY%/user-service:latest services/user-service
docker build -t %REGISTRY%/document-service:latest services/document-service
docker build -t %REGISTRY%/classification-service:latest services/classification-service
docker build -t %REGISTRY%/citation-service:latest services/citation-service
docker build -t %REGISTRY%/classification-model:latest services/classification-model-service
docker push %REGISTRY%/api-gateway:latest
docker push %REGISTRY%/auth-service:latest
docker push %REGISTRY%/user-service:latest
docker push %REGISTRY%/document-service:latest
docker push %REGISTRY%/classification-service:latest
docker push %REGISTRY%/citation-service:latest
docker push %REGISTRY%/classification-model:latest
```

9) Desplegar el stack:
```bash
set REGISTRY=192.168.1.10:5000
set KAFKA_KRAFT_CLUSTER_ID=UUID_GENERADO
docker stack deploy -c infrastructure/docker-stack.yml docs
```

## Limitaciones actuales
- La arquitectura es distribuida y tolera la caída de 1 nodo (HA parcial).
- El registry privado sigue siendo un punto único de falla si no se replica externamente.
- `classification-model` usa 2 réplicas con límites de CPU/RAM por costo.

## Validación obligatoria antes de multinodo
- Red overlay y DNS interno funcional.
- Kafka estable con 3 brokers, offsets persistentes y topics disponibles.
- Topics con replication factor 3 e ISR completo (1,2,3).
- Consumers estables sin loops ni re-suscripciones.
- MinIO funcional con acceso distribuido.
- Subida de PDFs y clasificación operativa.
- Comunicación entre microservicios estable.
- Retry logic funcionando ante retrasos en dependencias.
- Placement constraints correctos en Swarm.

## Cómo correr el proyecto
```bash
docker compose up --build


---

# gitignore (no la riegues aquí)

📄 `.gitignore`

```bash
node_modules/
dist/
.env
*.log
.DS_Store

# Angular
frontend/angular-app/node_modules/

# Docker
*.pid