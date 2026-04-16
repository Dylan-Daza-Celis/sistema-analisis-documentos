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

## Cómo correr el proyecto
```bash
docker-compose up --build


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