const express = require("express");
const { createProxyMiddleware } = require("http-proxy-middleware");

const app = express();
const port = process.env.PORT || 8080;

function requireEnv(name) {
	const value = process.env[name];
	if (!value) {
		throw new Error(`${name} es requerido`);
	}
	return value;
}

const services = {
	auth: requireEnv("AUTH_SERVICE_URL"),
	documents: requireEnv("DOCUMENT_SERVICE_URL"),
	classification: requireEnv("CLASSIFICATION_SERVICE_URL"),
	users: requireEnv("USER_SERVICE_URL"),
	citations: requireEnv("CITATION_SERVICE_URL")
};

app.get("/", (req, res) => {
	res.json({
		status: "ok",
		services
	});
});

app.get("/health", (req, res) => {
	res.sendStatus(200);
});

app.use(
	"/auth",
	createProxyMiddleware({
		target: services.auth,
		changeOrigin: true,
		pathRewrite: { "^/auth": "" },
		logLevel: "warn"
	})
);

app.use(
	"/documents",
	createProxyMiddleware({
		target: services.documents,
		changeOrigin: true,
		pathRewrite: { "^/documents": "" },
		logLevel: "warn"
	})
);

app.use(
	"/classification",
	createProxyMiddleware({
		target: services.classification,
		changeOrigin: true,
		pathRewrite: { "^/classification": "" },
		logLevel: "warn"
	})
);

app.use(
	"/users",
	createProxyMiddleware({
		target: services.users,
		changeOrigin: true,
		pathRewrite: { "^/users": "" },
		logLevel: "warn"
	})
);

app.use(
	"/citations",
	createProxyMiddleware({
		target: services.citations,
		changeOrigin: true,
		pathRewrite: { "^/citations": "" },
		logLevel: "warn"
	})
);

app.use((err, req, res, next) => {
	console.error("Gateway error:", err.message);
	res.status(502).json({ message: "Error en gateway" });
});

app.listen(port, () => {
	console.log(`Gateway running on port ${port}`);
});