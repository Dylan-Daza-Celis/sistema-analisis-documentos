const express = require("express");
const Minio = require("minio");
const pdfParse = require("pdf-parse");
const { Kafka } = require("kafkajs");

const app = express();

// Configuración MinIO
const minioClient = new Minio.Client({
  endPoint: "minio",
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

async function classifyTextWithZeroShot(text, candidateLabels) {
  if (!hfToken) {
    return null;
  }

  const inputText = trimTextForClassifier(text);
  if (!inputText) {
    return null;
  }

  const labelsToUse = Array.isArray(candidateLabels) && candidateLabels.length > 0
    ? candidateLabels
    : labels;

  if (!labelsToUse || labelsToUse.length === 0) {
    return null;
  }

  const inferenceEndpoints = [
    `https://router.huggingface.co/hf-inference/models/${zeroShotModel}`,
    `https://api-inference.huggingface.co/models/${zeroShotModel}`
  ];

  let result = null;
  const failures = [];

  for (const endpoint of inferenceEndpoints) {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${hfToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        inputs: inputText,
        parameters: {
          candidate_labels: labelsToUse,
          multi_label: false
        },
        options: {
          wait_for_model: true
        }
      })
    });

    if (response.ok) {
      result = await response.json();
      break;
    }

    failures.push(`${response.status} en ${endpoint}`);
  }

  if (!result) {
    throw new Error(`Zero-shot no disponible (${failures.join("; ")})`);
  }

  const parsed = parseZeroShotResponse(result);
  if (!parsed) {
    throw new Error("Respuesta inválida de zero-shot");
  }

  const bestLabel = parsed.labels[0];
  const bestScore = Number(parsed.scores[0] || 0);

  if (bestScore < classificationThreshold) {
    return {
      detectedTema: "General",
      keywords: [],
      confidence: bestScore,
      method: "zero-shot"
    };
  }

  return {
    detectedTema: bestLabel,
    keywords: [],
    confidence: bestScore,
    method: "zero-shot"
  };
}

async function classifyText(text, candidateLabels) {
  try {
    const zeroShotResult = await classifyTextWithZeroShot(text, candidateLabels);
    if (zeroShotResult) {
      return zeroShotResult;
    }
  } catch (err) {
    console.log("Zero-shot fallback a keywords:", err.message);
  }

  return classifyTextByKeywords(text);
}

function removeDocumentFromIndex(index, documentId) {
  const documents = Array.isArray(index.documents) ? index.documents : [];
  const filteredDocs = documents.filter((doc) => doc.id !== documentId);
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

async function updateUserIndex(userId, metadata) {
  const indexPath = `${userId}/indices.json`;
  const currentIndex = await readJsonObject(indexPath, {
    user: userId,
    userId,
    updatedAt: null,
    documents: [],
    byTema: {}
  });

  const documentEntry = {
    id: metadata.id,
    filename: metadata.filename,
    tema: metadata.tema || "General",
    subtema: metadata.subtema || null,
    keywords: Array.isArray(metadata.keywords) ? metadata.keywords : [],
    status: metadata.status,
    updatedAt: new Date().toISOString()
  };

  const documents = Array.isArray(currentIndex.documents) ? currentIndex.documents : [];
  const deduplicated = documents.filter((doc) => doc.id !== documentEntry.id);
  deduplicated.push(documentEntry);

  const byTema = {};
  const bySubtema = {};
  deduplicated.forEach((doc) => {
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
    user: currentIndex.user || userId,
    userId,
    updatedAt: new Date().toISOString(),
    documents: deduplicated,
    byTema,
    bySubtema
  };

  await writeJsonObject(indexPath, newIndex);
  console.log("Indice actualizado:", indexPath);
}

// Función de clasificación
async function classifyDocument(userId, id) {
  const pdfPath = `${userId}/${id}.pdf`;
  const metaPath = `${userId}/${id}.json`;

  try {
    // Descargar PDF
    const stream = await minioClient.getObject(bucket, pdfPath);
    const pdfBuffer = await streamToBuffer(stream);
    const pdfData = await pdfParse(pdfBuffer);
    const text = pdfData.text.toLowerCase();
    const extractedKeywords = extractKeywordsFromText(text);

    const userThemes = await getUserThemes(userId);
    const labelContext = buildLabelContext(userThemes);
    const { detectedTema, keywords, confidence, method } = await classifyText(text, labelContext.labels);
    const resolved = resolveLabel(detectedTema, labelContext.subthemeMap);

    console.log("Documento clasificado como:", resolved.tema, `(${method}, score=${confidence})`);

    // Descargar metadata
    const metadata = await readJsonObject(metaPath);
    if (!metadata) {
      throw new Error(`Metadata no encontrada para ${metaPath}`);
    }

    metadata.tema = resolved.tema;
    metadata.subtema = resolved.subtema || null;
    metadata.keywords = Array.from(new Set([...(keywords || []), ...extractedKeywords])).slice(0, defaultMaxKeywords);
    metadata.classification = {
      method,
      confidence
    };
    metadata.status = "processed";

    // Guardar metadata actualizada
    await writeJsonObject(metaPath, metadata);
    console.log("Metadata actualizada");

    // Actualizar índice por usuario sin duplicar documentos
    await updateUserIndex(userId, metadata);

  } catch (err) {
    console.error("Error clasificando:", err);
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
    await consumer.subscribe({ topic: "documents", fromBeginning: true });

    console.log("Kafka consumer conectado");

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const data = JSON.parse(message.value.toString());

          console.log("Evento recibido:", data);

          // Aquí ocurre la magia
          const userId = data.userId || data.user;

          if (!userId || !data.id) {
            console.error("Evento incompleto en Kafka:", data);
            return;
          }

          await classifyDocument(userId, data.id);
        } catch (err) {
          console.error("Error procesando mensaje Kafka:", err.message);
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
    updatedAt: doc.updatedAt || null
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

    const documents = Array.isArray(index.documents)
      ? index.documents.map(mapIndexDocument)
      : [];

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
  const { user, id } = req.params;
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