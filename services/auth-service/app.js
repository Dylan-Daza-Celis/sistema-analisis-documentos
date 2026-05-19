const express = require("express");
const bodyParser = require("body-parser");
const Minio = require("minio");
const { v4: uuidv4 } = require("uuid");
const jwt = require("jsonwebtoken");

const app = express();
app.use(bodyParser.json());

// Configuración MinIO
const minioClient = new Minio.Client({
  endPoint: "minio1",
  port: 9000,
  useSSL: false,
  accessKey: "admin",
  secretKey: "password123"
});

const bucket = "users";
const jwtSecret = process.env.JWT_SECRET || "dev-secret";
const jwtExpiresIn = process.env.JWT_EXPIRES_IN || "8h";

function normalizeEmail(email) {
  return email.trim().toLowerCase();
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function userExists(objectName) {
  return new Promise((resolve, reject) => {
    minioClient.statObject(bucket, objectName, (err) => {
      if (err) {
        if (err.code === "NotFound" || err.code === "NoSuchKey") {
          return resolve(false);
        }

        return reject(err);
      }

      resolve(true);
    });
  });
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

async function getUserByEmail(normalizedEmail) {
  const objectName = `${normalizedEmail}.json`;
  const stream = await minioClient.getObject(bucket, objectName);
  const raw = await streamToString(stream);
  return {
    objectName,
    user: JSON.parse(raw)
  };
}

async function persistUser(objectName, user) {
  await minioClient.putObject(bucket, objectName, JSON.stringify(user));
}

async function ensureUserId(objectName, user) {
  if (user.userId) {
    return user.userId;
  }

  const generatedUserId = uuidv4();
  user.userId = generatedUserId;
  user.updatedAt = new Date().toISOString();
  await persistUser(objectName, user);
  return generatedUserId;
}

async function listUserObjects() {
  return new Promise((resolve, reject) => {
    const objects = [];
    const stream = minioClient.listObjectsV2(bucket, "", true);

    stream.on("data", (obj) => {
      if (obj && obj.name && obj.name.endsWith(".json")) {
        objects.push(obj.name);
      }
    });

    stream.on("end", () => resolve(objects));
    stream.on("error", reject);
  });
}

async function findUserById(userId) {
  const objectNames = await listUserObjects();

  for (const objectName of objectNames) {
    try {
      const stream = await minioClient.getObject(bucket, objectName);
      const raw = await streamToString(stream);
      const user = JSON.parse(raw);

      if (user.userId === userId) {
        return { objectName, user };
      }
    } catch (err) {
      console.log("Error leyendo usuario", objectName, err.message);
    }
  }

  return null;
}

function isAdminUser(user) {
  const role = (user && user.userType ? String(user.userType) : "").trim().toLowerCase();
  return role === "admin" || role === "administrador";
}

function sanitizeUser(user) {
  if (!user) {
    return null;
  }

  const { password, ...rest } = user;
  return rest;
}

async function getRequesterByEmail(requesterEmail) {
  const normalizedRequesterEmail = typeof requesterEmail === "string" ? normalizeEmail(requesterEmail) : "";

  if (!normalizedRequesterEmail) {
    throw new Error("REQUESTER_EMAIL_REQUIRED");
  }

  if (!isValidEmail(normalizedRequesterEmail)) {
    throw new Error("REQUESTER_EMAIL_INVALID");
  }

  const requesterRecord = await getUserByEmail(normalizedRequesterEmail);
  const requesterId = await ensureUserId(requesterRecord.objectName, requesterRecord.user);

  return {
    ...requesterRecord,
    requesterId,
    normalizedRequesterEmail
  };
}

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
    const { name, email, password, userType, masterPassword } = req.body;
    const normalizedEmail = typeof email === "string" ? normalizeEmail(email) : "";
    const MASTER_PASSWORD = "12345";
    console.log("Registro recibido:", { name, email: normalizedEmail, userType });
    
    if (!name || !normalizedEmail || !password) {
      return res.status(400).send("Faltan datos basicos (name, email, password)");
    }

    if (!isValidEmail(normalizedEmail)) {
      return res.status(400).send("Correo invalido");
    }

    const objectName = `${normalizedEmail}.json`;

    if (await userExists(objectName)) {
      return res.status(409).send("El correo ya esta registrado");
    }

    let finalUserType = "usuario";
    
    // Permitir registro como admin si la contraseña maestra es correcta
    if (userType && String(userType).toLowerCase().trim() === "admin") {
      if (masterPassword !== MASTER_PASSWORD) {
        return res.status(403).send("Contraseña maestra incorrecta");
      }
      finalUserType = "admin";
    }

    const userId = uuidv4();
    // For security: always force public registrations to be regular users unless correct master password
    const savedUser = {
      userId,
      name,
      email: normalizedEmail,
      password,
      userType: finalUserType,
      createdAt: new Date().toISOString()
    };

    await minioClient.putObject(bucket, objectName, JSON.stringify(savedUser));

    res.json({ 
      message: "Usuario registrado correctamente", 
      userId,
      userType: savedUser.userType
    });

  } catch (err) {
    console.log("Error en register:", err);
    res.status(500).send("Error registrando usuario");
  }
});

// REGISTRO POR ADMIN
app.post("/admin/register", async (req, res) => {
  try {
    const { requesterEmail, name, email, password, userType, masterPassword } = req.body;
    const normalizedEmail = typeof email === "string" ? normalizeEmail(email) : "";
    const MASTER_PASSWORD = "12345";

    if (!requesterEmail) {
      return res.status(400).send("requesterEmail es requerido");
    }

    const requester = await getRequesterByEmail(requesterEmail);

    if (!isAdminUser(requester.user)) {
      return res.status(403).send("Solo administradores pueden registrar usuarios");
    }

    if (!name || !normalizedEmail || !password) {
      return res.status(400).send("Faltan datos basicos (name, email, password)");
    }

    if (!isValidEmail(normalizedEmail)) {
      return res.status(400).send("Correo invalido");
    }

    // Validar contraseña maestra si se intenta crear un admin
    if (userType && String(userType).toLowerCase().trim() === "admin") {
      if (masterPassword !== MASTER_PASSWORD) {
        return res.status(403).send("Contraseña maestra incorrecta");
      }
    }

    const objectName = `${normalizedEmail}.json`;

    if (await userExists(objectName)) {
      return res.status(409).send("El correo ya esta registrado");
    }

    const userId = uuidv4();
    const data = {
      userId,
      name,
      email: normalizedEmail,
      password,
      userType: userType || "usuario",
      createdAt: new Date().toISOString()
    };

    await minioClient.putObject(bucket, objectName, JSON.stringify(data));

    return res.json({
      message: "Usuario registrado correctamente",
      userId,
      userType: data.userType
    });
  } catch (err) {
    console.log("Error en admin/register:", err);
    return res.status(500).send("Error registrando usuario");
  }
});


// LOGIN
app.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    const normalizedEmail = typeof email === "string" ? normalizeEmail(email) : "";

    if (!normalizedEmail || !password) {
      return res.status(400).send("Faltan datos basicos");
    }

    const { objectName, user } = await getUserByEmail(normalizedEmail);

    if (user.password === password) {
      const userId = await ensureUserId(objectName, user);
      const token = jwt.sign(
        {
          userId,
          email: normalizedEmail,
          userType: user.userType || "usuario",
          name: user.name
        },
        jwtSecret,
        { expiresIn: jwtExpiresIn }
      );

      res.json({
        message: "Login correcto",
        userId,
        userType: user.userType,
        name: user.name,
        token
      });
    } else {
      res.status(401).send("Credenciales incorrectas");
    }

  } catch (err) {
    res.status(404).send("Usuario no existe");
  }
});

// CONSULTAR ID DE USUARIO
app.post("/user/id", async (req, res) => {
  try {
    const { email, requesterEmail } = req.body;
    const normalizedEmail = typeof email === "string" ? normalizeEmail(email) : "";

    if (!normalizedEmail) {
      return res.status(400).send("email es requerido");
    }

    if (!isValidEmail(normalizedEmail)) {
      return res.status(400).send("Correo invalido");
    }

    let requester;

    try {
      requester = await getRequesterByEmail(requesterEmail || email);
    } catch (authErr) {
      if (authErr.message === "REQUESTER_EMAIL_REQUIRED") {
        return res.status(400).send("requesterEmail es requerido");
      }

      if (authErr.message === "REQUESTER_EMAIL_INVALID") {
        return res.status(400).send("Correo de solicitante invalido");
      }

      return res.status(404).send("Solicitante no existe");
    }

    const { objectName, user } = await getUserByEmail(normalizedEmail);
    const targetUserId = await ensureUserId(objectName, user);
    const isSelfRequest = requester.normalizedRequesterEmail === normalizedEmail;

    if (!isSelfRequest && !isAdminUser(requester.user)) {
      return res.status(403).send("No autorizado para consultar este usuario");
    }

    return res.json({
      userId: targetUserId,
      userType: user.userType || null,
      name: user.name,
      email: user.email || normalizedEmail
    });
  } catch (err) {
    return res.status(404).send("Usuario no existe");
  }
});

// VERIFICAR TOKEN
app.post("/token/verify", (req, res) => {
  const { token } = req.body || {};

  if (!token) {
    return res.status(400).send("token es requerido");
  }

  try {
    const payload = jwt.verify(token, jwtSecret);
    return res.json({
      valid: true,
      payload
    });
  } catch (err) {
    return res.status(401).json({
      valid: false,
      message: "Token invalido"
    });
  }
});

// LISTAR USUARIOS (solo admin)
app.get("/users", async (req, res) => {
  try {
    const requesterEmail = req.query.requesterEmail;

    if (!requesterEmail) {
      return res.status(400).send("requesterEmail es requerido");
    }

    const requester = await getRequesterByEmail(requesterEmail);

    if (!isAdminUser(requester.user)) {
      return res.status(403).send("No autorizado");
    }

    const objectNames = await listUserObjects();
    const users = [];

    for (const objectName of objectNames) {
      try {
        const stream = await minioClient.getObject(bucket, objectName);
        const raw = await streamToString(stream);
        const user = JSON.parse(raw);
        users.push(sanitizeUser(user));
      } catch (err) {
        console.log("Error leyendo usuario", objectName, err.message);
      }
    }

    return res.json({
      total: users.length,
      users
    });
  } catch (err) {
    console.log("Error listando usuarios:", err);
    return res.status(500).send("Error listando usuarios");
  }
});

// CONSULTAR USUARIO POR ID (admin o mismo usuario)
app.get("/user/:userId", async (req, res) => {
  try {
    const { userId } = req.params;
    const requesterEmail = req.query.requesterEmail;

    if (!userId) {
      return res.status(400).send("userId es requerido");
    }

    const requester = await getRequesterByEmail(requesterEmail);
    const foundUser = await findUserById(userId);

    if (!foundUser) {
      return res.status(404).send("Usuario no existe");
    }

    const foundUserId = await ensureUserId(foundUser.objectName, foundUser.user);
    const isSelf = requester.requesterId === foundUserId;

    if (!isSelf && !isAdminUser(requester.user)) {
      return res.status(403).send("No autorizado");
    }

    return res.json(sanitizeUser(foundUser.user));
  } catch (err) {
    console.log("Error consultando usuario:", err);
    return res.status(500).send("Error consultando usuario");
  }
});

// ACTUALIZAR USUARIO (admin o mismo usuario)
app.put("/user/:userId", async (req, res) => {
  try {
    const { userId } = req.params;
    const { requesterEmail, name, password, userType } = req.body || {};

    if (!userId) {
      return res.status(400).send("userId es requerido");
    }

    const requester = await getRequesterByEmail(requesterEmail);
    const foundUser = await findUserById(userId);

    if (!foundUser) {
      return res.status(404).send("Usuario no existe");
    }

    const foundUserId = await ensureUserId(foundUser.objectName, foundUser.user);
    const isSelf = requester.requesterId === foundUserId;

    if (!isSelf && !isAdminUser(requester.user)) {
      return res.status(403).send("No autorizado");
    }

    if (name) {
      foundUser.user.name = name;
    }

    if (password) {
      foundUser.user.password = password;
    }

    if (userType && isAdminUser(requester.user)) {
      foundUser.user.userType = userType;
    }

    foundUser.user.updatedAt = new Date().toISOString();
    await persistUser(foundUser.objectName, foundUser.user);

    return res.json({
      message: "Usuario actualizado",
      user: sanitizeUser(foundUser.user)
    });
  } catch (err) {
    console.log("Error actualizando usuario:", err);
    return res.status(500).send("Error actualizando usuario");
  }
});

// ELIMINAR USUARIO
app.delete("/user/:userId", async (req, res) => {
  try {
    const { userId } = req.params;
    const { requesterEmail } = req.body || {};

    if (!userId) {
      return res.status(400).send("userId es requerido");
    }

    let requester;

    try {
      requester = await getRequesterByEmail(requesterEmail);
    } catch (authErr) {
      if (authErr.message === "REQUESTER_EMAIL_REQUIRED") {
        return res.status(400).send("requesterEmail es requerido");
      }

      if (authErr.message === "REQUESTER_EMAIL_INVALID") {
        return res.status(400).send("Correo de solicitante invalido");
      }

      return res.status(404).send("Solicitante no existe");
    }

    const foundUser = await findUserById(userId);

    if (!foundUser) {
      return res.status(404).send("Usuario no existe");
    }

    const foundUserId = await ensureUserId(foundUser.objectName, foundUser.user);
    const isSelfDelete = requester.requesterId === foundUserId;

    // Prevent self-deletion completely
    if (isSelfDelete) {
      return res.status(403).send("No puedes eliminar tu propia cuenta");
    }

    if (!isAdminUser(requester.user)) {
      return res.status(403).send("No autorizado para eliminar este usuario");
    }

    await minioClient.removeObject(bucket, foundUser.objectName);

    return res.json({
      message: "Usuario eliminado correctamente",
      userId: foundUserId
    });
  } catch (err) {
    console.log("Error eliminando usuario:", err);
    return res.status(500).send("Error eliminando usuario");
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