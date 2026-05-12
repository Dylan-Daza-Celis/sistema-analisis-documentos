const express = require("express");
const { createProxyMiddleware } = require("http-proxy-middleware");

const app = express();
const port = process.env.PORT || 8080;

const services = {
	auth: process.env.AUTH_SERVICE_URL || "http://auth-service:3000",
	documents: process.env.DOCUMENT_SERVICE_URL || "http://document-service:3000",
	classification: process.env.CLASSIFICATION_SERVICE_URL || "http://classification-service:3000",
	users: process.env.USER_SERVICE_URL || "http://user-service:3000",
	citations: process.env.CITATION_SERVICE_URL || "http://citation-service:3000"
};

app.get("/", (req, res) => {
	res.json({
		status: "ok",
		services
	});
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