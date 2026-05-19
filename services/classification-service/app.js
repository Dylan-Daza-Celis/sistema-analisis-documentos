const express = require("express");
const Minio = require("minio");
const pdfParse = require("pdf-parse");
const { Kafka } = require("kafkajs");

const app = express();

// Configuración MinIO
const minioClient = new Minio.Client({
  endPoint: "minio1",
  port: 9000,
  useSSL: false,
  accessKey: "admin",
  secretKey: "password123"
});

const bucket = "documents";

// Configuración Kafka
const kafka = new Kafka({
  clientId: "classification-service",
  brokers: ["kafka:9092"]
});

const consumer = kafka.consumer({ groupId: "classification-group" });
let consumerStarted = false;
let consumerRestartTimer = null;
const inFlightClassifications = new Set();
const zeroShotModel = process.env.ZERO_SHOT_MODEL || "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli";
const hfToken = process.env.HUGGINGFACE_API_TOKEN || "";
const classificationThreshold = Number(process.env.CLASSIFICATION_THRESHOLD || "0.45");
const maxCharsForClassifier = Number(process.env.CLASSIFICATION_MAX_CHARS || "2500");
const userServiceUrl = process.env.USER_SERVICE_URL || "http://user-service:3000";

// Diccionario de temas
const temas = {
  Redes: ["tcp", "ip", "router"],
  "Sistemas Operativos": ["kernel", "process", "thread"],
  "Bases de Datos": ["sql", "query", "database"]
};
const labels = Object.keys(temas);
const defaultMaxKeywords = Number(process.env.KEYWORDS_MAX || "8");
const minKeywordLength = Number(process.env.KEYWORDS_MIN_LENGTH || "4");
const stopwords = new Set([
  "para", "como", "desde", "donde", "entre", "sobre", "hasta", "hacia", "este", "esta", "estos", "estas",
  "that", "with", "from", "this", "these", "those", "into", "over", "under", "have", "has", "had",
  "como", "pero", "porque", "cuando", "aunque", "deben", "deber", "puede", "pueden", "using", "used",
  "document", "documents", "proceso", "sistema", "sistemas", "metodo", "metodos", "metodologia", "metodologico",
  "redes", "sistemas", "operativos", "bases", "datos", "general", "tema", "temas"
]);

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

async function writeJsonObject(path, data) {
  await minioClient.putObject(bucket, path, JSON.stringify(data, null, 2));
}

function classifyTextByKeywords(text) {
  const scores = {};
  const matchedKeywords = new Set();

  Object.entries(temas).forEach(([tema, words]) => {
    let score = 0;

    words.forEach((word) => {
      if (text.includes(word)) {
        score += 1;
        matchedKeywords.add(word);
      }
    });

    scores[tema] = score;
  });

  let detectedTema = "General";
  let maxScore = 0;

  Object.entries(scores).forEach(([tema, score]) => {
    if (score > maxScore) {
      maxScore = score;
      detectedTema = tema;
    }
  });

  return {
    detectedTema,
    keywords: Array.from(matchedKeywords),
    confidence: maxScore,
    method: "keywords"
  };
}

function trimTextForClassifier(text) {
  if (!text) {
    return "";
  }

  const normalized = text.replace(/\s+/g, " ").trim();
  return normalized.slice(0, maxCharsForClassifier);
}

function extractKeywordsFromText(text, maxKeywords = defaultMaxKeywords) {
  const cleaned = (text || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, " ")
    .replace(/[^a-z0-9\s]/g, " ");

  const tokens = cleaned.split(/\s+/).filter((token) => {
    if (!token) {
      return false;
    }

    if (token.length < minKeywordLength) {
      return false;
    }

    if (/^\d+$/.test(token)) {
      return false;
    }

    return !stopwords.has(token);
  });

  const freq = new Map();
  tokens.forEach((token) => {
    freq.set(token, (freq.get(token) || 0) + 1);
  });

  return [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, maxKeywords)
    .map(([token]) => token);
}

async function getUserThemes(userId) {
  try {
    const response = await fetch(`${userServiceUrl}/themes/${userId}`);
    if (!response.ok) {
      return null;
    }

    const data = await response.json();
    return Array.isArray(data.themes) ? data.themes : null;
  } catch (err) {
    console.log("No se pudo obtener temas del user-service:", err.message);
    return null;
  }
}

function buildLabelContext(themes) {
  const candidateLabels = [];
  const subthemeMap = {};

  if (Array.isArray(themes)) {
    themes.forEach((theme) => {
      const themeName = theme && theme.name ? String(theme.name).trim() : "";
      if (!themeName) {
        return;
      }

      candidateLabels.push(themeName);

      const subthemes = Array.isArray(theme.subthemes) ? theme.subthemes : [];
      subthemes.forEach((sub) => {
        const subName = String(sub || "").trim();
        if (!subName) {
          return;
        }

        const label = `${themeName}::${subName}`;
        candidateLabels.push(label);
        subthemeMap[label] = {
          tema: themeName,
          subtema: subName
        };
      });
    });
  }

  if (candidateLabels.length === 0) {
    return {
      labels,
      subthemeMap: {}
    };
  }

  return {
    labels: candidateLabels,
    subthemeMap
  };
}

function resolveLabel(label, subthemeMap) {
  if (!label) {
    return { tema: "General", subtema: null };
  }

  if (label === "General") {
    return { tema: "General", subtema: null };
  }

  if (subthemeMap[label]) {
    return subthemeMap[label];
  }

  const parts = String(label).split("::");
  if (parts.length === 2) {
    return { tema: parts[0], subtema: parts[1] };
  }

  return { tema: label, subtema: null };
}

function parseZeroShotResponse(result) {
  if (!result) {
    return null;
  }

  // Formato clásico: { labels: [...], scores: [...] }
  if (Array.isArray(result.labels) && Array.isArray(result.scores) && result.labels.length > 0) {
    return {
      labels: result.labels,
      scores: result.scores
    };
  }

  // Formato envuelto: [ { labels: [...], scores: [...] } ]
  if (Array.isArray(result) && result.length > 0) {
    const first = result[0];
    if (first && Array.isArray(first.labels) && Array.isArray(first.scores) && first.labels.length > 0) {
      return {
        labels: first.labels,
        scores: first.scores
      };
    }

    // Formato alterno: [ { label: "X", score: 0.9 }, ... ]
    const allHaveLabelScore = result.every(
      (item) => item && typeof item.label === "string" && typeof item.score !== "undefined"
    );

    if (allHaveLabelScore) {
      const sorted = [...result].sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
      return {
        labels: sorted.map((item) => item.label),
        scores: sorted.map((item) => Number(item.score || 0))
      };
    }
  }

  return null;
}

async function classifyTextWithPythonModel(text) {
  const inputText = trimTextForClassifier(text);
  if (!inputText) {
    return null;
  }

  try {
    const response = await fetch("http://classification-model:5000/classify", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        text: inputText
      })
    });

    if (!response.ok) {
      console.error(`Error en classification-model: ${response.status}`);
      return null;
    }

    const result = await response.json();

    if (result.es_cientifico === false) {
      return {
        detectedTema: null,
        keywords: [],
        confidence: 0,
        method: "python-model",
        esCientifico: false,
        puntaje: result.puntaje || 0,
        secciones: result.secciones || 0,
        razonRechazo: "no_cientifico"
      };
    }

    if (!result.tema || typeof result.confianza !== "number") {
      console.error("Respuesta inválida del modelo Python:", result);
      return null;
    }

    return {
      detectedTema: result.tema,
      keywords: [],
      confidence: result.confianza,
      method: result.metodo || "python-model",
      esCientifico: result.es_cientifico !== false,
      puntaje: result.puntaje || 0,
      secciones: result.secciones || 0
    };

  } catch (err) {
    console.error("Error clasificando con Python model:", err.message);
    return null;
  }
}

async function classifyText(text, candidateLabels) {
  try {
    // Intentar con modelo Python primero
    const pythonResult = await classifyTextWithPythonModel(text);
    if (pythonResult) {
      return pythonResult;
    }
  } catch (err) {
    console.log("Fallback a keywords:", err.message);
  }

  return classifyTextByKeywords(text);
}

function removeDocumentFromIndex(index, documentId) {
  const normalizedDocumentId = normalizeDocumentId(documentId);
  const documents = Array.isArray(index.documents) ? index.documents : [];
  const filteredDocs = documents.filter((doc) => normalizeDocumentId(doc.id) !== normalizedDocumentId);
  const byTema = {};
  const bySubtema = {};

  filteredDocs.forEach((doc) => {
    const tema = doc.tema || "General";
    if (!byTema[tema]) {
      byTema[tema] = [];
    }
    byTema[tema].push(doc.id);

    if (doc.subtema) {
      if (!bySubtema[doc.subtema]) {
        bySubtema[doc.subtema] = [];
      }
      bySubtema[doc.subtema].push(doc.id);
    }
  });

  return {
    ...index,
    documents: filteredDocs,
    byTema,
    bySubtema,
    updatedAt: new Date().toISOString()
  };
}

function normalizeSearchTerm(term) {
  if (!term) {
    return "";
  }

  return term
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function normalizeDocumentId(value) {
  if (value === null || value === undefined) {
    return "";
  }

  return String(value).trim();
}

function normalizeUserId(value) {
  if (value === null || value === undefined) {
    return "";
  }

  return String(value).trim();
}

function deduplicateDocumentsById(documents) {
  const byId = new Map();

  (Array.isArray(documents) ? documents : []).forEach((doc) => {
    const normalizedId = normalizeDocumentId(doc && doc.id);
    if (!normalizedId) {
      return;
    }

    const current = byId.get(normalizedId) || {};
    const currentStatus = current.status || "";
    const incomingStatus = doc && doc.status ? doc.status : "";
    const preferIncoming = !currentStatus || (currentStatus !== "processed" && incomingStatus === "processed");

    if (preferIncoming || !byId.has(normalizedId)) {
      byId.set(normalizedId, {
        ...(doc || {}),
        id: normalizedId
      });
    }
  });

  return Array.from(byId.values());
}

async function updateUserIndex(userId, metadata) {
  const normalizedUserId = normalizeUserId(userId);
  const metadataId = normalizeDocumentId(metadata && metadata.id);
  const indexPath = `${normalizedUserId}/indices.json`;

  if (!normalizedUserId || !metadataId) {
    throw new Error(`updateUserIndex requiere userId e id válidos. userId='${normalizedUserId}', id='${metadataId}'`);
  }
  
  // RETRY LOOP para manejar race conditions
  let retries = 5;
  let lastError = null;
  
  while (retries > 0) {
    try {
      const currentIndex = await readJsonObject(indexPath, {
        user: normalizedUserId,
        userId: normalizedUserId,
        updatedAt: null,
        documents: [],
        byTema: {},
        version: 0
      });

      // Buscar si el documento ya existe usando ID normalizado.
      const documents = deduplicateDocumentsById(currentIndex.documents);
      const existingDocIndex = documents.findIndex((doc) => normalizeDocumentId(doc.id) === metadataId);
      
      // Si ya existe con status "processed", no duplicar
      if (existingDocIndex !== -1 && documents[existingDocIndex].status === "processed") {
        console.log(`Documento ${metadataId} ya estaba en el índice. Actualizando referencia.`);
        documents[existingDocIndex] = {
          ...documents[existingDocIndex],
          id: metadataId,
          updatedAt: new Date().toISOString()
        };
      } else {
        // Eliminar si existe una versión anterior y añadir la nueva
        const deduplicated = documents.filter((doc) => normalizeDocumentId(doc.id) !== metadataId);
        
        const documentEntry = {
          id: metadataId,
          filename: metadata.filename,
          tema: metadata.tema || "General",
          subtema: metadata.subtema || null,
          keywords: Array.isArray(metadata.keywords) ? metadata.keywords : [],
          status: metadata.status,
          esCientifico: metadata.esCientifico !== undefined ? metadata.esCientifico : true,
          razonRechazo: metadata.razonRechazo || null,
          updatedAt: new Date().toISOString()
        };
        
        deduplicated.push(documentEntry);
        documents.splice(0, documents.length, ...deduplicated);
      }

      const byTema = {};
      const bySubtema = {};
      documents.forEach((doc) => {
        doc.id = normalizeDocumentId(doc.id);
        if (!doc.id) {
          return;
        }

        const tema = doc.tema || "General";
        if (!byTema[tema]) {
          byTema[tema] = [];
        }
        byTema[tema].push(doc.id);

        if (doc.subtema) {
          if (!bySubtema[doc.subtema]) {
            bySubtema[doc.subtema] = [];
          }
          bySubtema[doc.subtema].push(doc.id);
        }
      });

      const newIndex = {
        user: currentIndex.user || normalizedUserId,
        userId: normalizedUserId,
        updatedAt: new Date().toISOString(),
        documents: deduplicateDocumentsById(documents),
        byTema,
        bySubtema,
        version: (currentIndex.version || 0) + 1
      };

      await writeJsonObject(indexPath, newIndex);
      console.log(`Índice actualizado para usuario ${normalizedUserId}. Total documentos: ${newIndex.documents.length}`);
      return;
      
    } catch (err) {
      lastError = err;
      retries--;
      if (retries > 0) {
        console.log(`Error actualizando índice. Reintentando... (${retries} intentos restantes)`);
        // Esperar un poco antes de reintentar
        await new Promise(resolve => setTimeout(resolve, 100 * (6 - retries)));
      }
    }
  }
  
  console.error(`Error crítico actualizando índice para usuario ${normalizedUserId} después de 5 intentos:`, lastError);
}

// Función de clasificación
async function classifyDocument(userId, id) {
  const normalizedUserId = normalizeUserId(userId);
  const normalizedId = normalizeDocumentId(id);
  const processingKey = `${normalizedUserId}:${normalizedId}`;

  if (!normalizedUserId || !normalizedId) {
    console.error("classifyDocument recibió userId/id inválidos", { userId, id });
    return;
  }

  if (inFlightClassifications.has(processingKey)) {
    console.log(`Documento ${normalizedId} ya está en procesamiento local. Saltando duplicado.`);
    return;
  }

  inFlightClassifications.add(processingKey);

  const pdfPath = `${normalizedUserId}/${normalizedId}.pdf`;
  const metaPath = `${normalizedUserId}/${normalizedId}.json`;
  const lockPath = `${normalizedUserId}/${normalizedId}.lock`;

  try {
    // VERIFICACIÓN DE LOCK: Comprobar si otro proceso está procesando esto
    try {
      const lockExists = await minioClient.statObject(bucket, lockPath);
      if (lockExists) {
        console.log(`Documento ${normalizedId} está siendo procesado por otro proceso. Saltando.`);
        return;
      }
    } catch (err) {
      // El lock no existe, es lo esperado
    }

    // Crear un lock temporal
    const lockData = JSON.stringify({
      startTime: new Date().toISOString(),
      processId: process.pid
    });
    await minioClient.putObject(bucket, lockPath, lockData);
    console.log(`Lock creado para documento: ${normalizedId}`);

    // Descargar metadata primero para verificar estado
    const currentMetadata = await readJsonObject(metaPath);
    if (!currentMetadata) {
      console.error(`Metadata no encontrada para ${metaPath}`);
      await minioClient.removeObject(bucket, lockPath);
      return;
    }

    currentMetadata.id = normalizeDocumentId(currentMetadata.id || normalizedId);
    currentMetadata.userId = normalizeUserId(currentMetadata.userId || normalizedUserId);

    // DEDUPLICACIÓN: Si el documento ya fue procesado, no procesarlo de nuevo
    if (currentMetadata.status === "processed" && currentMetadata.tema) {
      console.log(`Documento ${normalizedId} ya fue procesado anteriormente. Saltando clasificación.`);
      
      // Aun así, verificar que esté en el índice del usuario
      const indexPath = `${normalizedUserId}/indices.json`;
      const currentIndex = await readJsonObject(indexPath, {
        user: normalizedUserId,
        userId: normalizedUserId,
        updatedAt: null,
        documents: [],
        byTema: {}
      });
      
      const isInIndex = Array.isArray(currentIndex.documents) && 
                        currentIndex.documents.some(doc => normalizeDocumentId(doc.id) === normalizedId);
      
      if (!isInIndex) {
        console.log(`Añadiendo documento ya procesado al índice: ${normalizedId}`);
        await updateUserIndex(normalizedUserId, currentMetadata);
      }
      
      // Eliminar lock
      try {
        await minioClient.removeObject(bucket, lockPath);
      } catch (err) {
        console.log("No se pudo eliminar lock:", err.message);
      }
      return;
    }

    console.log(`Iniciando clasificación del documento: ${normalizedId}`);

    // Descargar PDF
    const stream = await minioClient.getObject(bucket, pdfPath);
    const pdfBuffer = await streamToBuffer(stream);
    const pdfData = await pdfParse(pdfBuffer);
    const text = pdfData.text.toLowerCase();
    const extractedKeywords = extractKeywordsFromText(text);

    const userThemes = await getUserThemes(userId);
    const labelContext = buildLabelContext(userThemes);
    const classResult = await classifyText(text, labelContext.labels);
    const { detectedTema, keywords, confidence, method, esCientifico, puntaje, secciones, razonRechazo } = classResult;
    const resolved = resolveLabel(detectedTema, labelContext.subthemeMap);

    console.log("Documento clasificado como:", resolved.tema, `(${method}, score=${confidence})`);
    if (!esCientifico) {
      console.log(`Documento NO es científico (puntaje=${puntaje}, secciones=${secciones})`);
    }

    const metadata = currentMetadata;
    metadata.tema = resolved.tema;
    metadata.subtema = resolved.subtema || null;
    metadata.keywords = Array.from(new Set([...(keywords || []), ...extractedKeywords])).slice(0, defaultMaxKeywords);
    metadata.classification = {
      method,
      confidence
    };
    metadata.status = "processed";
    metadata.processedAt = new Date().toISOString();
    metadata.esCientifico = esCientifico !== undefined ? esCientifico : true;
    if (razonRechazo) {
      metadata.razonRechazo = razonRechazo;
    }
    metadata.evaluacionCientifica = {
      puntaje: puntaje || 0,
      secciones: secciones || 0
    };

    // Guardar metadata actualizada
    await writeJsonObject(metaPath, metadata);
    console.log("Metadata actualizada para documento:", normalizedId);

    // Actualizar índice por usuario sin duplicar documentos
    await updateUserIndex(normalizedUserId, metadata);
    console.log("Documento añadido/actualizado en índice:", normalizedId);

    // Eliminar lock después de completar exitosamente
    try {
      await minioClient.removeObject(bucket, lockPath);
      console.log("Lock eliminado para documento:", normalizedId);
    } catch (err) {
      console.log("No se pudo eliminar lock:", err.message);
    }

  } catch (err) {
    console.error("Error clasificando documento:", normalizedId, err);
    // Intentar eliminar el lock en caso de error
    try {
      await minioClient.removeObject(bucket, lockPath);
    } catch (removeErr) {
      console.log("No se pudo eliminar lock tras error:", removeErr.message);
    }
  } finally {
    inFlightClassifications.delete(processingKey);
  }
}

function scheduleConsumerRestart(delayMs = 3000) {
  if (consumerRestartTimer) {
    return;
  }

  consumerRestartTimer = setTimeout(() => {
    consumerRestartTimer = null;
    startConsumer();
  }, delayMs);
}

// Kafka Consumer
async function startConsumer() {
  if (consumerStarted) {
    return;
  }

  consumerStarted = true;

  try {
    await consumer.connect();
    await consumer.subscribe({ 
      topic: "documents", 
      fromBeginning: false // Solo procesa mensajes nuevos, no los anteriores
    });

    console.log("Kafka consumer conectado a topic 'documents'");

    await consumer.run({
      autoCommit: false, // Controlamos cuando confirmar el mensaje
      eachMessage: async ({ topic, partition, message }) => {
        let shouldCommit = false;
        try {
          const data = JSON.parse(message.value.toString());

          console.log(`Procesando evento Kafka [partition=${partition}, offset=${message.offset}]:`, data);

          const userId = normalizeUserId(data.userId || data.user);
          const documentId = normalizeDocumentId(data.id);

          if (!userId || !documentId) {
            console.error("Evento incompleto en Kafka:", data);
            shouldCommit = true; // Aún así confirmar para no procesarlo de nuevo
            return;
          }

          // Procesar documento (con el lock incluido)
          await classifyDocument(userId, documentId);
          shouldCommit = true;
          
        } catch (err) {
          console.error("Error procesando mensaje Kafka:", err.message);
          // No confirmar para reintentar más tarde
          shouldCommit = false;
        } finally {
          // Confirmar solo si fue procesado exitosamente
          if (shouldCommit) {
            try {
              await consumer.commitOffsets([{
                topic,
                partition,
                offset: (Number(message.offset) + 1).toString()
              }]);
              console.log(`Offset confirmado para partition ${partition}, offset ${message.offset}`);
            } catch (commitErr) {
              console.error("Error confirmando offset:", commitErr.message);
            }
          }
        }
      }
    });

  } catch (err) {
    console.error("Error en consumer:", err);
    consumerStarted = false;

    try {
      await consumer.disconnect();
    } catch (disconnectErr) {
      console.error("Error desconectando consumer:", disconnectErr.message);
    }

    scheduleConsumerRestart();
  }
}

consumer.on(consumer.events.CRASH, async (event) => {
  console.error("Consumer crash detectado:", event.payload.error.message);
  consumerStarted = false;

  try {
    await consumer.disconnect();
  } catch (disconnectErr) {
    console.error("Error desconectando consumer tras crash:", disconnectErr.message);
  }

  scheduleConsumerRestart();
});

// Esperar MinIO (igual que en otros servicios)
function waitForMinio(retries = 10) {
  minioClient.bucketExists(bucket, (err) => {
    if (err) {
      console.log("Esperando MinIO...", err.message);
      if (retries > 0) {
        setTimeout(() => waitForMinio(retries - 1), 2000);
      } else {
        console.log("MinIO no disponible para clasificación");
      }
    } else {
      console.log("MinIO listo para clasificación");
      startConsumer(); // IMPORTANTE: iniciar Kafka después
    }
  });
}

// Iniciar proceso
waitForMinio();

// Endpoint para consultar metadata real en MinIO
app.get("/metadata/:user/:id", async (req, res) => {
  const { user, id } = req.params;
  const metaPath = `${user}/${id}.json`;

  try {
    const metadata = await readJsonObject(metaPath, null);

    if (!metadata) {
      return res.status(404).json({
        message: "Metadata no encontrada",
        path: metaPath
      });
    }

    return res.json(metadata);
  } catch (err) {
    console.error("Error consultando metadata:", err.message);
    return res.status(500).json({ message: "Error consultando metadata" });
  }
});

// Endpoint para consultar el indice completo de un usuario
app.get("/indices/:user", async (req, res) => {
  const { user } = req.params;
  const indexPath = `${user}/indices.json`;

  try {
    const index = await readJsonObject(indexPath, null);

    if (!index) {
      return res.status(404).json({
        message: "Indice no encontrado",
        path: indexPath
      });
    }

    return res.json(index);
  } catch (err) {
    console.error("Error consultando indice:", err.message);
    return res.status(500).json({ message: "Error consultando indice" });
  }
});

function mapIndexDocument(doc) {
  return {
    id: doc.id,
    name: doc.filename || "Sin nombre",
    filename: doc.filename || null,
    tema: doc.tema || "General",
    subtema: doc.subtema || null,
    status: doc.status || "pending",
    keywords: Array.isArray(doc.keywords) ? doc.keywords : [],
    classification: doc.classification || null,
    updatedAt: doc.updatedAt || null,
    esCientifico: doc.esCientifico !== undefined ? doc.esCientifico : true,
    razonRechazo: doc.razonRechazo || null
  };
}

// Endpoint para listar los documentos de un usuario 
app.get("/documents/:user", async (req, res) => {
  const { user } = req.params;
  const indexPath = `${user}/indices.json`;

  try {
    const index = await readJsonObject(indexPath, null);

    if (!index) {
      return res.status(404).json({
        message: "Indice no encontrado",
        path: indexPath
      });
    }

    let documents = Array.isArray(index.documents)
      ? index.documents.map(mapIndexDocument)
      : [];

    // DEDUPLICACIÓN SEGURA: Remover duplicados basado en ID
    const seenIds = new Set();
    documents = documents.filter((doc) => {
      if (seenIds.has(doc.id)) {
        console.log(`Documento duplicado detectado y removido en respuesta: ${doc.id}`);
        return false;
      }
      seenIds.add(doc.id);
      return true;
    });

    return res.json({
      user,
      total: documents.length,
      documents
    });
  } catch (err) {
    console.error("Error consultando documentos del usuario:", err.message);
    return res.status(500).json({ message: "Error consultando documentos del usuario" });
  }
});

// Endpoint para consultar documentos de un tema dentro del indice
app.get("/indices/:user/tema/:tema", async (req, res) => {
  const { user, tema } = req.params;
  const indexPath = `${user}/indices.json`;

  try {
    const index = await readJsonObject(indexPath, null);

    if (!index) {
      return res.status(404).json({
        message: "Indice no encontrado",
        path: indexPath
      });
    }

    const temas = index.byTema || {};
    const ids = Array.isArray(temas[tema]) ? temas[tema] : [];
    const docs = Array.isArray(index.documents)
      ? index.documents.filter((doc) => ids.includes(doc.id)).map(mapIndexDocument)
      : [];

    return res.json({
      user,
      tema,
      total: docs.length,
      documents: docs
    });
  } catch (err) {
    console.error("Error consultando indice por tema:", err.message);
    return res.status(500).json({ message: "Error consultando indice por tema" });
  }
});

// Mantener servicio activo
setInterval(() => {
  console.log("Classification Service activo...");
}, 10000);

// Servidor (opcional, pero útil para debug)
app.listen(3000, () => {
  console.log("Classification Service corriendo");
});

// Endpoint para listar temas disponibles
app.get("/temas", (req, res) => {
  return res.json({
    total: labels.length,
    temas: labels
  });
});

// Endpoint para listar temas del usuario (con subtemas)
app.get("/temas/:userId", async (req, res) => {
  const { userId } = req.params;

  try {
    const themes = await getUserThemes(userId);
    if (!themes) {
      return res.json({
        total: labels.length,
        temas: labels
      });
    }

    return res.json({
      userId,
      total: themes.length,
      themes
    });
  } catch (err) {
    console.error("Error consultando temas del usuario:", err.message);
    return res.status(500).json({ message: "Error consultando temas" });
  }
});

// Endpoint para buscar documentos por keyword dentro del indice
app.get("/indices/:user/keywords/:term", async (req, res) => {
  const { user, term } = req.params;
  const indexPath = `${user}/indices.json`;
  const normalizedTerm = normalizeSearchTerm(term);

  try {
    const index = await readJsonObject(indexPath, null);

    if (!index) {
      return res.status(404).json({
        message: "Indice no encontrado",
        path: indexPath
      });
    }

    if (!normalizedTerm) {
      return res.status(400).json({ message: "keyword es requerido" });
    }

    const docs = Array.isArray(index.documents)
      ? index.documents.filter((doc) => {
        const keywords = Array.isArray(doc.keywords)
          ? doc.keywords.map(normalizeSearchTerm)
          : [];
        const filename = normalizeSearchTerm(doc.filename || "");

        return keywords.includes(normalizedTerm) || filename.includes(normalizedTerm);
      }).map(mapIndexDocument)
      : [];

    return res.json({
      user,
      keyword: term,
      total: docs.length,
      documents: docs
    });
  } catch (err) {
    console.error("Error consultando indice por keyword:", err.message);
    return res.status(500).json({ message: "Error consultando indice por keyword" });
  }
});

// Endpoint para eliminar un documento del indice
app.delete("/indices/:user/document/:id", async (req, res) => {
  const user = normalizeUserId(req.params.user);
  const id = normalizeDocumentId(req.params.id);
  const indexPath = `${user}/indices.json`;

  try {
    const index = await readJsonObject(indexPath, null);

    if (!index) {
      return res.status(404).json({
        message: "Indice no encontrado",
        path: indexPath
      });
    }

    const updatedIndex = removeDocumentFromIndex(index, id);
    await writeJsonObject(indexPath, updatedIndex);

    return res.json({
      message: "Documento removido del indice",
      user,
      id
    });
  } catch (err) {
    console.error("Error eliminando documento del indice:", err.message);
    return res.status(500).json({ message: "Error eliminando documento del indice" });
  }
});

// Endpoint para consultar documentos de un subtema dentro del indice
app.get("/indices/:user/subtema/:subtema", async (req, res) => {
  const { user, subtema } = req.params;
  const indexPath = `${user}/indices.json`;

  try {
    const index = await readJsonObject(indexPath, null);

    if (!index) {
      return res.status(404).json({
        message: "Indice no encontrado",
        path: indexPath
      });
    }

    const bySubtema = index.bySubtema || {};
    const ids = Array.isArray(bySubtema[subtema]) ? bySubtema[subtema] : [];
    const docs = Array.isArray(index.documents)
      ? index.documents.filter((doc) => ids.includes(doc.id)).map(mapIndexDocument)
      : [];

    return res.json({
      user,
      subtema,
      total: docs.length,
      documents: docs
    });
  } catch (err) {
    console.error("Error consultando indice por subtema:", err.message);
    return res.status(500).json({ message: "Error consultando indice por subtema" });
  }
});