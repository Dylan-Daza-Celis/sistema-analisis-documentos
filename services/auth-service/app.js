const express = require("express");
const bodyParser = require("body-parser");
const Minio = require("minio");

const app = express();
app.use(bodyParser.json());

// Configuración MinIO
const minioClient = new Minio.Client({
  endPoint: "minio",
  port: 9000,
  useSSL: false,
  accessKey: "admin",
  secretKey: "password123"
});

const bucket = "users";

// Esperar a MinIO (IMPORTANTE)
function waitForMinio(retries = 10) {
  minioClient.bucketExists(bucket, (err, exists) => {
    if (err) {
      console.log("Esperando MinIO en Auth...");
      if (retries > 0) {
        setTimeout(() => waitForMinio(retries - 1), 2000);
      } else {
        console.log("MinIO no disponible en Auth");
      }
    } else {
      console.log("MinIO conectado en Auth Service");

      if (!exists) {
        minioClient.makeBucket(bucket, "us-east-1", (err) => {
          if (err) console.log("Error creando bucket:", err);
          else console.log("Bucket 'users' creado");
        });
      } else {
        console.log("Bucket ya existe");
      }
    }
  });
}

// Ejecutar conexión
waitForMinio();


// REGISTRO
app.post("/register", async (req, res) => {
  try {
    const { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).send("Faltan datos");
    }

    const data = JSON.stringify({ username, password });

    await minioClient.putObject(bucket, `${username}.json`, data);

    res.send("Usuario registrado correctamente");

  } catch (err) {
    console.log("Error en register:", err);
    res.status(500).send("Error registrando usuario");
  }
});


// LOGIN
app.post("/login", async (req, res) => {
  try {
    const { username, password } = req.body;

    const stream = await minioClient.getObject(bucket, `${username}.json`);

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

  } catch (err) {
    res.status(404).send("Usuario no existe");
  }
});


// Mantener servicio vivo
setInterval(() => {
  console.log("Auth Service activo...");
}, 10000);


// Levantar servidor con delay
setTimeout(() => {
  app.listen(3000, () => {
    console.log("Auth Service corriendo en puerto 3000");
  });
}, 8000);