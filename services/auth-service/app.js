const express = require("express");
const bodyParser = require("body-parser");
const Minio = require("minio");

const app = express();
app.use(bodyParser.json());

// conexión a MinIO
const minioClient = new Minio.Client({
  endPoint: "minio",
  port: 9000,
  useSSL: false,
  accessKey: "admin",
  secretKey: "password123"
});

const bucket = "users";

// verificar y crear bucket correctamente
minioClient.bucketExists(bucket, (err, exists) => {
  if (err) {
    return console.log("Error verificando bucket:", err);
  }

  if (!exists) {
    minioClient.makeBucket(bucket, "us-east-1", (err) => {
      if (err) return console.log("Error creando bucket:", err);
      console.log("Bucket creado correctamente");
    });
  } else {
    console.log("Bucket ya existe");
  }
});

// REGISTRO
app.post("/register", async (req, res) => {
  const { username, password } = req.body;

  const data = JSON.stringify({ username, password });

  minioClient.putObject(bucket, `${username}.json`, data, (err) => {
    if (err) return res.status(500).send(err);

    res.send("Usuario registrado");
  });
});

// LOGIN
app.post("/login", async (req, res) => {
  const { username, password } = req.body;

  minioClient.getObject(bucket, `${username}.json`, (err, stream) => {
    if (err) return res.status(404).send("Usuario no existe");

    let data = "";
    stream.on("data", chunk => data += chunk);
    stream.on("end", () => {
      const user = JSON.parse(data);

      if (user.password === password) {
        res.send("Login correcto");
      } else {
        res.status(401).send("Credenciales incorrectas");
      }
    });
  });
});

setTimeout(() => {
  app.listen(3000, () => {
    console.log("Auth Service corriendo en puerto 3000");
  });
}, 5000);