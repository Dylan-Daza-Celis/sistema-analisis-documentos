const express = require("express");
const multer = require("multer");
const Minio = require("minio");
const { Kafka } = require("kafkajs");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
app.use(express.json());

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} es requerido`);
  }
  return value;
}

const minioEndpoint = requireEnv("MINIO_ENDPOINT");
const minioPort = Number(process.env.MINIO_PORT || "9000");
const minioUseSSL = String(process.env.MINIO_USE_SSL || "false").toLowerCase() === "true";
const minioAccessKey = requireEnv("MINIO_ACCESS_KEY");
const minioSecretKey = requireEnv("MINIO_SECRET_KEY");

//  Configuración MinIO
const minioClient = new Minio.Client({
  endPoint: minioEndpoint,
  port: minioPort,
  useSSL: minioUseSSL,
  accessKey: minioAccessKey,
  secretKey: minioSecretKey
});

const bucket = "documents";
const classificationServiceUrl = requireEnv("CLASSIFICATION_SERVICE_URL");
const retryDelayMs = Number(process.env.RETRY_DELAY_MS || "5000");
const kafkaBrokers = requireEnv("KAFKA_BROKERS")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

//  Configuración Kafka
const kafka = new Kafka({
  clientId: "document-service",
  brokers: kafkaBrokers
});

const producer = kafka.producer();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function retry(fn, retries = Infinity, delay = retryDelayMs) {
  let remaining = retries;
  while (remaining > 0 || retries === Infinity) {
    try {
      return await fn();
    } catch (err) {
      console.error("Retrying...", err.message);
      if (retries !== Infinity) {
        remaining -= 1;
      }
      await sleep(delay);
    }
  }

  throw new Error("Connection failed");
}

function bucketExistsAsync() {
  return new Promise((resolve, reject) => {
    minioClient.bucketExists(bucket, (err, exists) => {
      if (err) {
        return reject(err);
      }
      return resolve(exists);
    });
  });
}

function makeBucketAsync() {
  return new Promise((resolve, reject) => {
    minioClient.makeBucket(bucket, "us-east-1", (err) => {
      if (err) {
        if (err.code === "BucketAlreadyOwnedByYou" || err.code === "BucketAlreadyExists") {
          return resolve();
        }
        return reject(err);
      }
      return resolve();
    });
  });
}

async function ensureMinioBucket() {
  await retry(async () => {
    const exists = await bucketExistsAsync();
    if (!exists) {
      await makeBucketAsync();
    }
    return true;
  });
}

async function fetchWithRetry(url, options = {}, retries = 3) {
  return retry(async () => {
    const response = await fetch(url, options);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return response;
  }, retries, 2000);
}
function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];

    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

async function readObjectAsText(path) {
  const stream = await minioClient.getObject(bucket, path);
  const buffer = await streamToBuffer(stream);
  return buffer.toString("utf8");
}

async function readJsonObject(path, fallbackValue = null) {
  try {
    const content = await readObjectAsText(path);
    return JSON.parse(content);
  } catch (err) {
    if (err && (err.code === "NoSuchKey" || err.code === "NotFound")) {
      return fallbackValue;
    }
    throw err;
  }
}

async function removeFromIndex(userId, id) {
  try {
    const response = await fetchWithRetry(
      `${classificationServiceUrl}/indices/${userId}/document/${id}`,
      { method: "DELETE" }
    );
  } catch (err) {
    console.log("Error llamando a classification-service para indice:", err.message);
  }
}

//  Conectar Kafka una sola vez
async function initKafka() {
  await retry(async () => {
    await producer.connect();
    return true;
  });
  console.log("Kafka conectado");
}

initKafka();

// Función para enviar evento
async function sendEvent(data) {
  try {
    await retry(async () => {
      await producer.send({
        topic: "documents",
        messages: [
          { value: JSON.stringify(data) }
        ]
      });
      return true;
    }, 5, 2000);

    console.log("Evento enviado a Kafka:", data);
  } catch (err) {
    console.error("Error enviando evento:", err);
  }
}

ensureMinioBucket();

app.get("/health", (req, res) => {
  res.sendStatus(200);
});

// Endpoint para subir documento
app.post("/upload", upload.single("file"), async (req, res) => {
  const file = req.file;
  const userId = req.body.userId;

  if (!file) return res.status(400).send("No file");
  if (!userId) return res.status(400).send("userId es requerido");

  // Validar que sea PDF
  const isPdfMimeType = file.mimetype === "application/pdf";
  const isPdfExtension = file.originalname.toLowerCase().endsWith(".pdf");
  
  if (!isPdfMimeType || !isPdfExtension) {
    console.log(`Rechazo de archivo no-PDF: ${file.originalname} (MIME: ${file.mimetype})`);
    return res.status(400).json({ 
      error: "Solo se aceptan archivos PDF. Por favor, sube un archivo PDF válido." 
    });
  }

  const id = Date.now().toString();

  const pdfPath = `${userId}/${id}.pdf`;
  const metaPath = `${userId}/${id}.json`;
  const eventSentPath = `${userId}/${id}.event_sent`;

  try {
    // 1. Guardar PDF
    console.log(`Guardando PDF: ${pdfPath}`);
    await minioClient.putObject(bucket, pdfPath, file.buffer);

    // 2. Crear metadata
    const metadata = JSON.stringify({
      id,
      userId,
      filename: file.originalname,
      status: "pending",
      tema: null,
      keywords: [],
      createdAt: new Date().toISOString()
    });

    console.log(`Guardando metadata: ${metaPath}`);
    await minioClient.putObject(bucket, metaPath, metadata);

    // 3. Marcar que vamos a enviar el evento
    const eventMarker = JSON.stringify({
      documentId: id,
      userId,
      sentAt: new Date().toISOString()
    });
    console.log(`Marcando evento como enviado: ${eventSentPath}`);
    await minioClient.putObject(bucket, eventSentPath, eventMarker);

    // 4. Enviar evento a Kafka UNA SOLA VEZ
    console.log(`Enviando evento Kafka para documento: ${id}`);
    await sendEvent({ userId, id });

    console.log(`Documento completamente procesado: ${id}`);
    res.json({
      message: "Documento subido y evento enviado",
      id,
      userId
    });

  } catch (err) {
    console.error("Error al subir documento:", err);
    res.status(500).send("Error al subir documento");
  }
});

// Endpoint para descargar documento
app.get("/download/:userId/:id", async (req, res) => {
  const { userId, id } = req.params;
  const pdfPath = `${userId}/${id}.pdf`;
  const metaPath = `${userId}/${id}.json`;

  try {
    const metadata = await readJsonObject(metaPath, null);
    const filename = metadata && metadata.filename ? metadata.filename : `${id}.pdf`;

    const stream = await minioClient.getObject(bucket, pdfPath);
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    stream.on("error", (err) => {
      console.error("Error enviando PDF:", err.message);
      res.status(500).end("Error descargando documento");
    });
    stream.pipe(res);
  } catch (err) {
    if (err && (err.code === "NoSuchKey" || err.code === "NotFound")) {
      return res.status(404).send("Documento no encontrado");
    }

    console.error("Error descargando documento:", err);
    return res.status(500).send("Error descargando documento");
  }
});

// Endpoint para eliminar documento
app.delete("/documents/:userId/:id", async (req, res) => {
  const { userId, id } = req.params;
  const pdfPath = `${userId}/${id}.pdf`;
  const metaPath = `${userId}/${id}.json`;

  try {
    await minioClient.removeObject(bucket, pdfPath);
    await minioClient.removeObject(bucket, metaPath);
    await removeFromIndex(userId, id);

    return res.json({
      message: "Documento eliminado",
      userId,
      id
    });
  } catch (err) {
    console.error("Error eliminando documento:", err);
    return res.status(500).send("Error eliminando documento");
  }
});

// Mantener servicio activo
setInterval(() => {
  console.log("Document Service activo...");
}, 10000);

// Levantar servidor con delay
setTimeout(() => {
  app.listen(3000, () => {
    console.log("Document Service corriendo en puerto 3000");
  });
}, 8000);
