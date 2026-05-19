const express = require("express");
const bodyParser = require("body-parser");
const Minio = require("minio");

const app = express();
app.use(bodyParser.json());

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

const minioClient = new Minio.Client({
	endPoint: minioEndpoint,
	port: minioPort,
	useSSL: minioUseSSL,
	accessKey: minioAccessKey,
	secretKey: minioSecretKey
});

const bucket = "profiles";
const authServiceUrl = requireEnv("AUTH_SERVICE_URL");
const classificationServiceUrl = requireEnv("CLASSIFICATION_SERVICE_URL");
const retryDelayMs = Number(process.env.RETRY_DELAY_MS || "5000");

const defaultThemes = [
	{ name: "Redes", subthemes: ["Protocolos", "Topologias"] },
	{ name: "Sistemas Operativos", subthemes: ["Procesos", "Memoria"] },
	{ name: "Bases de Datos", subthemes: ["SQL", "Modelado"] }
];

function normalizeName(name) {
	return String(name || "").trim();
}

function normalizeKey(name) {
	return normalizeName(name).toLowerCase();
}

function cloneThemes(themes) {
	return themes.map((theme) => ({
		name: theme.name,
		subthemes: Array.isArray(theme.subthemes) ? [...theme.subthemes] : []
	}));
}

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

function streamToString(stream) {
	return new Promise((resolve, reject) => {
		let data = "";
		stream.on("data", (chunk) => {
			data += chunk.toString("utf8");
		});
		stream.on("end", () => resolve(data));
		stream.on("error", reject);
	});
}

async function readProfile(userId) {
	const objectName = `${userId}.json`;

	try {
		const stream = await minioClient.getObject(bucket, objectName);
		const raw = await streamToString(stream);
		return JSON.parse(raw);
	} catch (err) {
		if (err && (err.code === "NoSuchKey" || err.code === "NotFound")) {
			return null;
		}
		throw err;
	}
}

async function saveProfile(userId, profile) {
	const objectName = `${userId}.json`;
	await minioClient.putObject(bucket, objectName, JSON.stringify(profile, null, 2));
}

async function ensureProfile(userId) {
	let profile = await readProfile(userId);

	if (!profile) {
		profile = {
			userId,
			themes: cloneThemes(defaultThemes),
			createdAt: new Date().toISOString(),
			updatedAt: new Date().toISOString()
		};
		await saveProfile(userId, profile);
	}

	return profile;
}

async function isAdminRequester(requesterEmail) {
	if (!requesterEmail) {
		return false;
	}

	try {
		const response = await fetchWithRetry(`${authServiceUrl}/user/id`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ email: requesterEmail, requesterEmail })
		});

		const data = await response.json();
		const role = String(data.userType || "").trim().toLowerCase();
		return role === "admin" || role === "administrador";
	} catch (err) {
		console.log("Error validando admin:", err.message);
		return false;
	}
}

async function themeHasDocuments(userId, themeName) {
	try {
		const response = await fetchWithRetry(
			`${classificationServiceUrl}/indices/${userId}/tema/${encodeURIComponent(themeName)}`
		);

		const data = await response.json();
		return Number(data.total || 0) > 0;
	} catch (err) {
		console.log("No se pudo validar documentos por tema:", err.message);
		return false;
	}
}

function findThemeIndex(themes, name) {
	const target = normalizeKey(name);
	return themes.findIndex((theme) => normalizeKey(theme.name) === target);
}

function findSubthemeIndex(subthemes, name) {
	const target = normalizeKey(name);
	return subthemes.findIndex((sub) => normalizeKey(sub) === target);
}

ensureMinioBucket();

app.get("/health", (req, res) => {
	res.json({ status: "ok" });
});

// Obtener tematicas del usuario
app.get("/themes/:userId", async (req, res) => {
	const { userId } = req.params;

	try {
		const profile = await ensureProfile(userId);
		return res.json({
			userId,
			total: profile.themes.length,
			themes: profile.themes
		});
	} catch (err) {
		console.log("Error consultando temas:", err.message);
		return res.status(500).json({ message: "Error consultando temas" });
	}
});

// Crear nueva tematica
app.post("/themes/:userId", async (req, res) => {
	const { userId } = req.params;
	const name = normalizeName(req.body.name);

	if (!name) {
		return res.status(400).json({ message: "name es requerido" });
	}

	try {
		const profile = await ensureProfile(userId);
		const exists = findThemeIndex(profile.themes, name) >= 0;

		if (exists) {
			return res.status(409).json({ message: "La tematica ya existe" });
		}

		profile.themes.push({ name, subthemes: [] });
		profile.updatedAt = new Date().toISOString();
		await saveProfile(userId, profile);

		return res.json({ message: "Tematica creada", themes: profile.themes });
	} catch (err) {
		console.log("Error creando tematica:", err.message);
		return res.status(500).json({ message: "Error creando tematica" });
	}
});

// Crear subtematica
app.post("/themes/:userId/:theme/subthemes", async (req, res) => {
	const { userId, theme } = req.params;
	const name = normalizeName(req.body.name);

	if (!name) {
		return res.status(400).json({ message: "name es requerido" });
	}

	try {
		const profile = await ensureProfile(userId);
		const themeIndex = findThemeIndex(profile.themes, theme);

		if (themeIndex < 0) {
			return res.status(404).json({ message: "Tematica no encontrada" });
		}

		const targetTheme = profile.themes[themeIndex];
		if (!Array.isArray(targetTheme.subthemes)) {
			targetTheme.subthemes = [];
		}

		if (findSubthemeIndex(targetTheme.subthemes, name) >= 0) {
			return res.status(409).json({ message: "La subtematica ya existe" });
		}

		targetTheme.subthemes.push(name);
		profile.updatedAt = new Date().toISOString();
		await saveProfile(userId, profile);

		return res.json({ message: "Subtematica creada", themes: profile.themes });
	} catch (err) {
		console.log("Error creando subtematica:", err.message);
		return res.status(500).json({ message: "Error creando subtematica" });
	}
});

// Eliminar tematica
app.delete("/themes/:userId/:theme", async (req, res) => {
	const { userId, theme } = req.params;
	const requesterEmail = req.query.requesterEmail;
	const force = String(req.query.force || "false").toLowerCase() === "true";

	try {
		const profile = await ensureProfile(userId);
		const themeIndex = findThemeIndex(profile.themes, theme);

		if (themeIndex < 0) {
			return res.status(404).json({ message: "Tematica no encontrada" });
		}

		const targetTheme = profile.themes[themeIndex];
		const hasSubthemes = Array.isArray(targetTheme.subthemes) && targetTheme.subthemes.length > 0;
		const hasDocs = await themeHasDocuments(userId, targetTheme.name);

		if ((hasSubthemes || hasDocs) && !force) {
			return res.status(409).json({ message: "Tematica no vacia" });
		}

		if (force) {
			const isAdmin = await isAdminRequester(requesterEmail);
			if (!isAdmin) {
				return res.status(403).json({ message: "Solo admin puede eliminar tematica no vacia" });
			}
		}

		profile.themes.splice(themeIndex, 1);
		profile.updatedAt = new Date().toISOString();
		await saveProfile(userId, profile);

		return res.json({ message: "Tematica eliminada", themes: profile.themes });
	} catch (err) {
		console.log("Error eliminando tematica:", err.message);
		return res.status(500).json({ message: "Error eliminando tematica" });
	}
});

// Eliminar subtematica
app.delete("/themes/:userId/:theme/subthemes/:subtheme", async (req, res) => {
	const { userId, theme, subtheme } = req.params;
	const requesterEmail = req.query.requesterEmail;
	const force = String(req.query.force || "false").toLowerCase() === "true";

	try {
		const profile = await ensureProfile(userId);
		const themeIndex = findThemeIndex(profile.themes, theme);

		if (themeIndex < 0) {
			return res.status(404).json({ message: "Tematica no encontrada" });
		}

		const targetTheme = profile.themes[themeIndex];
		const subthemes = Array.isArray(targetTheme.subthemes) ? targetTheme.subthemes : [];
		const subIndex = findSubthemeIndex(subthemes, subtheme);

		if (subIndex < 0) {
			return res.status(404).json({ message: "Subtematica no encontrada" });
		}

		if (force) {
			const isAdmin = await isAdminRequester(requesterEmail);
			if (!isAdmin) {
				return res.status(403).json({ message: "Solo admin puede eliminar subtematica" });
			}
		}

		subthemes.splice(subIndex, 1);
		targetTheme.subthemes = subthemes;
		profile.updatedAt = new Date().toISOString();
		await saveProfile(userId, profile);

		return res.json({ message: "Subtematica eliminada", themes: profile.themes });
	} catch (err) {
		console.log("Error eliminando subtematica:", err.message);
		return res.status(500).json({ message: "Error eliminando subtematica" });
	}
});

setInterval(() => {
	console.log("User Service activo...");
}, 10000);

app.listen(3000, () => {
	console.log("User Service corriendo en puerto 3000");
});