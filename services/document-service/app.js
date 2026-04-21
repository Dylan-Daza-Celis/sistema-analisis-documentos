const express = require("express");
const multer = require("multer");
const Minio = require("minio");
const { Kafka } = require("kafkajs");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

// 🔷 Configuración MinIO
const minioClient = new Minio.Client({
  endPoint: "minio",
  port: 9000,
  useSSL: false,
  accessKey: "admin",
  secretKey: "password123"
});

const bucket = "documents";

// 🔷 Configuración Kafka
const kafka = new Kafka({
  clientId: "document-service",
  brokers: ["kafka:9092"]
});

const producer = kafka.producer();

// 🔷 Conectar Kafka una sola vez
async function initKafka() {
  try {
    await producer.connect();
    console.log("Kafka conectado");
  } catch (err) {
    console.error("Error conectando a Kafka:", err);
  }
}

initKafka();

// 🔷 Función para enviar evento
async function sendEvent(data) {
  try {
    await producer.send({
      topic: "documents",
      messages: [
        { value: JSON.stringify(data) }
      ]
    });

    console.log("Evento enviado a Kafka:", data);
  } catch (err) {
    console.error("Error enviando evento:", err);
  }
}

// 🔷 Esperar a que MinIO esté listo
function waitForMinio(retries = 10) {
  minioClient.bucketExists(bucket, (err, exists) => {
    if (err) {
      console.log("Esperando MinIO...");
      if (retries > 0) {
        setTimeout(() => waitForMinio(retries - 1), 2000);
      } else {
        console.log("MinIO no disponible después de varios intentos");
      }
    } else {
      console.log("MinIO conectado");

      if (!exists) {
        minioClient.makeBucket(bucket, "us-east-1", (err) => {
          if (err) console.log("Error creando bucket:", err);
          else console.log("Bucket 'documents' creado");
        });
      } else {
        console.log("Bucket ya existe");
      }
    }
  });
}

// 🔷 Ejecutar conexión a MinIO
waitForMinio();

// 🔥 Endpoint para subir documento
app.post("/upload", upload.single("file"), async (req, res) => {
  const file = req.file;
  const user = req.body.user;

  if (!file) return res.status(400).send("No file");

  const id = Date.now().toString();

  const pdfPath = `${user}/${id}.pdf`;
  const metaPath = `${user}/${id}.json`;

  try {
    // 1. Guardar PDF
    await minioClient.putObject(bucket, pdfPath, file.buffer);

    // 2. Crear metadata
    const metadata = JSON.stringify({
      id,
      user,
      filename: file.originalname,
      status: "pending",
      tema: null,
      keywords: []
    });

    await minioClient.putObject(bucket, metaPath, metadata);

    // 3. Enviar evento a Kafka
    await sendEvent({ user, id });

    res.send("Documento subido y evento enviado");

  } catch (err) {
    console.error(err);
    res.status(500).send("Error al subir documento");
  }
});

// 🔷 Mantener servicio activo
setInterval(() => {
  console.log("Document Service activo...");
}, 10000);

// 🔷 Levantar servidor con delay
setTimeout(() => {
  app.listen(3000, () => {
    console.log("Document Service corriendo en puerto 3000");
  });
}, 8000);