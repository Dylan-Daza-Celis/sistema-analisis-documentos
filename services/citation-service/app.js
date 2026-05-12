const express = require("express");
const bodyParser = require("body-parser");

const app = express();
app.use(bodyParser.json());

function formatAuthors(authors) {
	if (!authors || authors.length === 0) {
		return "Autor desconocido";
	}

	if (typeof authors === "string") {
		return authors;
	}

	if (authors.length === 1) {
		return authors[0];
	}

	if (authors.length === 2) {
		return `${authors[0]} & ${authors[1]}`;
	}

	return `${authors.slice(0, -1).join(", ")}, & ${authors[authors.length - 1]}`;
}

function formatApa(entry) {
	const authors = formatAuthors(entry.authors || entry.author);
	const year = entry.year || "s.f.";
	const title = entry.title || entry.filename || "Documento sin titulo";
	const source = entry.source || entry.publisher || "";
	const url = entry.url || "";

	const sourcePart = source ? ` ${source}.` : "";
	const urlPart = url ? ` ${url}` : "";

	return `${authors} (${year}). ${title}.${sourcePart}${urlPart}`.trim();
}

function formatInText(entry) {
	const authors = formatAuthors(entry.authors || entry.author);
	const year = entry.year || "s.f.";
	return `(${authors}, ${year})`;
}

app.get("/health", (req, res) => {
	res.json({ status: "ok" });
});

// Generar referencias APA 7
app.post("/apa", (req, res) => {
	const documents = req.body && Array.isArray(req.body.documents)
		? req.body.documents
		: [];

	if (documents.length === 0) {
		return res.status(400).json({ message: "documents es requerido" });
	}

	const references = documents.map(formatApa);
	const citations = documents.map(formatInText);

	return res.json({
		total: documents.length,
		references,
		citations
	});
});

setInterval(() => {
	console.log("Citation Service activo...");
}, 10000);

app.listen(3000, () => {
	console.log("Citation Service corriendo en puerto 3000");
});