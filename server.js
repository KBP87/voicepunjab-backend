"use strict";

process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:", reason);
});

if (process.env.NODE_ENV !== "production") {
  require("dotenv").config();
}

const express = require("express");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const rateLimit = require("express-rate-limit");
const helmet = require("helmet");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const { Resend } = require("resend");
const textToSpeech = require("@google-cloud/text-to-speech");
const { TranslationServiceClient } = require("@google-cloud/translate").v3;

const app = express();
app.set("trust proxy", 1);

const PORT = Number(process.env.PORT || 8080);
const JWT_SECRET = String(process.env.JWT_SECRET || "change-me-now").trim();

const APP_BASE_URL = String(
  process.env.APP_BASE_URL || "https://voicepunjabai.com"
).trim();

const API_PUBLIC_BASE_URL = String(
  process.env.API_PUBLIC_BASE_URL ||
    "https://voicepunjab-api-777821135954.us-central1.run.app"
).trim();

const RESEND_API_KEY = String(process.env.RESEND_API_KEY || "").trim();
const RESEND_FROM = String(
  process.env.RESEND_FROM || "VoicePunjabAI Support <support@voicepunjabai.com>"
).trim();

const ADMIN_EMAIL = String(process.env.ADMIN_EMAIL || "").trim().toLowerCase();
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "").trim();
const ADMIN_NAME = String(process.env.ADMIN_NAME || "VoicePunjab Admin").trim();

const resend = RESEND_API_KEY ? new Resend(RESEND_API_KEY) : null;

const GOOGLE_CLOUD_PROJECT =
  process.env.GOOGLE_CLOUD_PROJECT ||
  process.env.GCLOUD_PROJECT ||
  process.env.GOOGLE_PROJECT_ID ||
  "";

const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "";

const GOOGLE_CREDENTIALS_JSON =
  process.env.GOOGLE_CREDENTIALS_JSON || "";

const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();

if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL missing");
  process.exit(1);
}

const clientConfig = {
  projectId: GOOGLE_CLOUD_PROJECT
};

if (GOOGLE_CREDENTIALS_JSON) {
  try {
    const creds = JSON.parse(GOOGLE_CREDENTIALS_JSON);
    if (creds.private_key) {
      creds.private_key = creds.private_key.replace(/\\n/g, "\n");
    }
    clientConfig.credentials = creds;
  } catch (err) {
    console.error("Failed to parse GOOGLE_CREDENTIALS_JSON:", err.message);
  }
} else if (GOOGLE_APPLICATION_CREDENTIALS) {
  clientConfig.keyFilename = GOOGLE_APPLICATION_CREDENTIALS;
}

const ttsClient = new textToSpeech.TextToSpeechClient(clientConfig);
const translateClient = new TranslationServiceClient(clientConfig);

const ALLOWED_ORIGIN_RAW = (
  process.env.ALLOWED_ORIGIN ||
  "https://voicepunjabai.com,https://www.voicepunjabai.com,http://localhost:8080,http://127.0.0.1:8080"
).trim();

const ALLOWED_ORIGINS =
  ALLOWED_ORIGIN_RAW === "*"
    ? "*"
    : ALLOWED_ORIGIN_RAW.split(",").map((s) => s.trim()).filter(Boolean);

const ALLOWED_VOICES = ["pa-IN-Standard-A", "pa-IN-Standard-B"];
const MAX_TTS_TEXT_LENGTH = 1000;
const MAX_CONVERT_TEXT_LENGTH = 500;
const MIN_SPEED = 0.75;
const MAX_SPEED = 1.25;
const MIN_PITCH = -5;
const MAX_PITCH = 5;

const CACHE_DIR = path.join(__dirname, "cache");

if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}

console.log("Using DB:", DATABASE_URL ? "Neon ✅" : "Missing ❌");

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

async function dbGet(sql, params = []) {
  const result = await pool.query(sql, params);
  return result.rows[0] || null;
}

async function dbAll(sql, params = []) {
  const result = await pool.query(sql, params);
  return result.rows;
}

async function dbRun(sql, params = []) {
  return pool.query(sql, params);
}

async function initDatabase() {
  await dbRun(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      plan TEXT NOT NULL DEFAULT 'free',
      is_verified BOOLEAN NOT NULL DEFAULT FALSE,
      is_admin BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS email_verification_tokens (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token TEXT NOT NULL UNIQUE,
      expires_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS password_reset_tokens (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token TEXT NOT NULL UNIQUE,
      expires_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS translation_cache (
      id SERIAL PRIMARY KEY,
      source_text TEXT NOT NULL UNIQUE,
      translated_text TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS tts_cache (
      id SERIAL PRIMARY KEY,
      cache_key TEXT NOT NULL UNIQUE,
      text TEXT NOT NULL,
      voice TEXT NOT NULL,
      speed REAL NOT NULL,
      pitch REAL NOT NULL,
      file_name TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS usage_logs (
      id SERIAL PRIMARY KEY,
      client_id TEXT NOT NULL,
      endpoint TEXT NOT NULL,
      char_count INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS request_logs (
      id SERIAL PRIMARY KEY,
      endpoint TEXT NOT NULL,
      cache_status TEXT NOT NULL,
      char_count INTEGER NOT NULL,
      client_id TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await dbRun(`
    CREATE TABLE IF NOT EXISTS audio_history (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      original_text TEXT NOT NULL,
      voice TEXT NOT NULL,
      speed REAL NOT NULL,
      pitch REAL NOT NULL,
      audio_url TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

function containsGurmukhi(text) {
  return /[\u0A00-\u0A7F]/.test(String(text || ""));
}

function normalizeText(value) {
  return String(value || "").trim();
}

function normalizeWhitespace(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function normalizeEnglishForCache(text) {
  return normalizeWhitespace(text)
    .toLowerCase()
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/[.,!?;:]+$/g, "");
}

function normalizePunjabiForCache(text) {
  return normalizeWhitespace(text)
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/[.,!?;:]+$/g, "");
}

function isValidVoice(voice) {
  return ALLOWED_VOICES.includes(voice);
}

function isValidSpeed(speed) {
  return Number.isFinite(speed) && speed >= MIN_SPEED && speed <= MAX_SPEED;
}

function isValidPitch(pitch) {
  return Number.isFinite(pitch) && pitch >= MIN_PITCH && pitch <= MAX_PITCH;
}

function makeTtsCacheKey({ text, voice, speed, pitch }) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify({ text, voice, speed, pitch }))
    .digest("hex");
}

function getClientIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.trim()) {
    return forwarded.split(",")[0].trim();
  }
  return req.ip || "unknown";
}

function createAuthToken(user) {
  return jwt.sign(
    {
      id: user.id,
      email: user.email,
      plan: user.plan,
      is_verified: !!user.is_verified,
      is_admin: !!user.is_admin
    },
    JWT_SECRET,
    { expiresIn: "30d" }
  );
}

function sanitizeUser(user) {
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    plan: user.plan,
    is_verified: !!user.is_verified,
    is_admin: !!user.is_admin
  };
}

function makeVerificationToken() {
  return crypto.randomBytes(32).toString("hex");
}

async function createVerificationToken(userId) {
  const token = makeVerificationToken();

  await dbRun(`DELETE FROM email_verification_tokens WHERE user_id = $1`, [userId]);

  await dbRun(
    `INSERT INTO email_verification_tokens (user_id, token, expires_at)
     VALUES ($1, $2, NOW() + INTERVAL '24 hours')`,
    [userId, token]
  );

  return token;
}

async function ensureAdminUser() {
  if (!ADMIN_EMAIL || !ADMIN_PASSWORD) {
    console.log("Admin bootstrap skipped: ADMIN_EMAIL or ADMIN_PASSWORD missing.");
    return;
  }

  const existing = await dbGet(
    `SELECT id, email, is_admin FROM users WHERE email = $1`,
    [ADMIN_EMAIL]
  );

  if (existing) {
    await dbRun(
      `UPDATE users
       SET is_admin = TRUE, is_verified = TRUE
       WHERE email = $1`,
      [ADMIN_EMAIL]
    );
    console.log("Admin user ensured for existing account:", ADMIN_EMAIL);
    return;
  }

  const passwordHash = await bcrypt.hash(ADMIN_PASSWORD, 10);

  await dbRun(
    `INSERT INTO users (name, email, password_hash, plan, is_verified, is_admin)
     VALUES ($1, $2, $3, 'free', TRUE, TRUE)`,
    [ADMIN_NAME, ADMIN_EMAIL, passwordHash]
  );

  console.log("Admin user created:", ADMIN_EMAIL);
}

async function sendVerificationEmail(user, token) {
  const verifyUrl = `${API_PUBLIC_BASE_URL}/api/verify-email?token=${encodeURIComponent(token)}`;

  console.log("Verification URL for", user.email, ":", verifyUrl);

  if (!resend) {
    console.log("Verification email not sent because Resend is not configured.");
    throw new Error("Resend is not configured");
  }

  const response = await resend.emails.send({
    from: RESEND_FROM,
    to: user.email,
    subject: "Verify your VoicePunjabAI email",
    text: `Hello,

Thank you for signing up for VoicePunjabAI.

Please verify your email by clicking the link below:

${verifyUrl}

If you did not create this account, you can ignore this email.

VoicePunjabAI Team`,
    html: `
      <div style="font-family:Arial,sans-serif;line-height:1.7;color:#222;max-width:600px;">
        <p>Hello,</p>
        <p>Thank you for signing up for <strong>VoicePunjabAI</strong>.</p>
        <p>Please verify your email by clicking the button below:</p>
        <p style="margin:24px 0;">
          <a
            href="${verifyUrl}"
            style="
              display:inline-block;
              padding:12px 18px;
              background:#4f46e5;
              color:#ffffff;
              text-decoration:none;
              border-radius:8px;
              font-weight:700;
            "
          >
            Verify Email
          </a>
        </p>
        <p>If the button does not work, open this link:</p>
        <p style="word-break:break-word;">
          <a href="${verifyUrl}">${verifyUrl}</a>
        </p>
        <p>If you did not create this account, you can ignore this email.</p>
        <p>VoicePunjabAI Team</p>
      </div>
    `
  });

  if (response?.error) {
    console.error("Verification email send failed:", response.error);
    throw new Error(response.error.message || "Verification email failed");
  }

  console.log(
    "Verification email sent successfully to",
    user.email,
    response?.data?.id || ""
  );
}

async function createPasswordResetToken(userId) {
  const token = makeVerificationToken();

  await dbRun(`DELETE FROM password_reset_tokens WHERE user_id = $1`, [userId]);

  await dbRun(
    `INSERT INTO password_reset_tokens (user_id, token, expires_at)
     VALUES ($1, $2, NOW() + INTERVAL '1 hour')`,
    [userId, token]
  );

  return token;
}

async function sendPasswordResetEmail(user, token) {
  const resetUrl = `${APP_BASE_URL}/reset-password.html?token=${encodeURIComponent(token)}`;

  if (!resend) {
    console.log("Password reset email not sent because Resend is not configured.");
    throw new Error("Resend is not configured");
  }

  const response = await resend.emails.send({
    from: RESEND_FROM,
    to: user.email,
    subject: "Reset your VoicePunjabAI password",
    text: `Hello ${user.name},

Open this link to reset your password:
${resetUrl}

If you did not request this, you can ignore this email.

VoicePunjabAI Team`,
    html: `
      <div style="font-family:Arial,sans-serif;line-height:1.7;color:#222;max-width:600px;">
        <p>Hello ${user.name},</p>
        <p>Click the button below to reset your password:</p>
        <p style="margin:24px 0;">
          <a
            href="${resetUrl}"
            style="
              display:inline-block;
              padding:12px 18px;
              background:#4f46e5;
              color:#ffffff;
              text-decoration:none;
              border-radius:8px;
              font-weight:700;
            "
          >
            Reset Password
          </a>
        </p>
        <p>If the button does not work, open this link:</p>
        <p style="word-break:break-word;">
          <a href="${resetUrl}">${resetUrl}</a>
        </p>
        <p>If you did not request this, you can ignore this email.</p>
        <p>VoicePunjabAI Team</p>
      </div>
    `
  });

  if (response?.error) {
    console.error("Password reset email send failed:", response.error);
    throw new Error(response.error.message || "Password reset email failed");
  }

  console.log(
    "Password reset email sent successfully to",
    user.email,
    response?.data?.id || ""
  );
}

async function loadUserFromToken(req, res, next) {
  try {
    const authHeader = String(req.headers.authorization || "").trim();

    if (!authHeader.startsWith("Bearer ")) {
      req.user = null;
      return next();
    }

    const token = authHeader.slice(7).trim();
    if (!token) {
      req.user = null;
      return next();
    }

    const decoded = jwt.verify(token, JWT_SECRET);

    const user = await dbGet(
      `SELECT id, name, email, plan, is_verified, is_admin FROM users WHERE id = $1`,
      [decoded.id]
    );

    req.user = user || null;
    next();
  } catch (err) {
    req.user = null;
    next();
  }
}

function requireAuth(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ error: "Login required." });
  }
  next();
}

function requireAdmin(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ error: "Login required." });
  }
  if (!req.user.is_admin) {
    return res.status(403).json({ error: "Admin access required." });
  }
  next();
}

function getPlanLimits(plan) {
  switch (String(plan || "").toLowerCase()) {
    case "starter":
      return { translate: 5000, tts: 8000 };
    case "pro":
      return { translate: 20000, tts: 30000 };
    case "business":
      return { translate: 100000, tts: 150000 };
    case "free":
      return { translate: 1000, tts: 2000 };
    default:
      return { translate: 300, tts: 500 };
  }
}

function getTrackingId(req) {
  if (req.user?.id) return `user:${req.user.id}`;
  return getClientIp(req);
}

async function getTodayUsage(clientId, endpoint) {
  const row = await dbGet(
    `SELECT COALESCE(SUM(char_count), 0) AS total
     FROM usage_logs
     WHERE client_id = $1
       AND endpoint = $2
       AND created_at::date = CURRENT_DATE`,
    [clientId, endpoint]
  );

  return Number(row?.total || 0);
}

async function logUsage(clientId, endpoint, charCount) {
  await dbRun(
    `INSERT INTO usage_logs (client_id, endpoint, char_count)
     VALUES ($1, $2, $3)`,
    [clientId, endpoint, charCount]
  );
}

async function logRequest(endpoint, cacheStatus, charCount, clientId) {
  await dbRun(
    `INSERT INTO request_logs (endpoint, cache_status, char_count, client_id)
     VALUES ($1, $2, $3, $4)`,
    [endpoint, cacheStatus, charCount, clientId]
  );
}

async function saveAudioHistory(userId, originalText, voice, speed, pitch, audioUrl) {
  await dbRun(
    `INSERT INTO audio_history (user_id, original_text, voice, speed, pitch, audio_url)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [userId, originalText, voice, speed, pitch, audioUrl]
  );
}

async function translateEnglishToPunjabi(text) {
  if (!GOOGLE_CLOUD_PROJECT) {
    throw new Error("Missing GOOGLE_CLOUD_PROJECT");
  }

  const cacheText = normalizeEnglishForCache(text);

  const existing = await dbGet(
    `SELECT translated_text FROM translation_cache WHERE source_text = $1`,
    [cacheText]
  );

  if (existing) {
    return { translatedText: existing.translated_text, cached: true };
  }

  const [response] = await translateClient.translateText({
    parent: `projects/${GOOGLE_CLOUD_PROJECT}/locations/global`,
    contents: [text],
    mimeType: "text/plain",
    sourceLanguageCode: "en",
    targetLanguageCode: "pa"
  });

  const translatedText = response.translations?.[0]?.translatedText || text;

  await dbRun(
    `INSERT INTO translation_cache (source_text, translated_text)
     VALUES ($1, $2)
     ON CONFLICT (source_text) DO NOTHING`,
    [cacheText, translatedText]
  );

  return { translatedText, cached: false };
}

app.use(
  helmet({
    contentSecurityPolicy: false,
    crossOriginResourcePolicy: false
  })
);

app.use(express.json({ limit: "1mb" }));

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (ALLOWED_ORIGINS === "*") {
    res.setHeader("Access-Control-Allow-Origin", "*");
  } else if (origin && ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Expose-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

app.use(loadUserFromToken);

app.use("/cache", express.static(CACHE_DIR));
app.use(express.static(path.join(__dirname, "public")));

const rateLimitOptions = {
  standardHeaders: true,
  legacyHeaders: false
};

const signupLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 5,
  ...rateLimitOptions
});

const resendLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 5,
  ...rateLimitOptions
});

const forgotPasswordLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 5,
  ...rateLimitOptions
});

const ttsLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 10,
  ...rateLimitOptions
});

const convertLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  ...rateLimitOptions
});

const adminLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  ...rateLimitOptions
});

app.use("/api/signup", signupLimiter);
app.use("/api/resend-verification", resendLimiter);
app.use("/api/forgot-password", forgotPasswordLimiter);
app.use("/api/tts", ttsLimiter);
app.use("/api/convert", convertLimiter);
app.use("/api/admin", adminLimiter);

app.get("/health", async (req, res) => {
  try {
    await dbGet("SELECT 1 AS ok");
    res.json({
      status: "ok",
      db: "connected",
      service: "voicepunjab-api"
    });
  } catch (err) {
    console.error("health error:", err.message);
    res.status(500).json({
      status: "error",
      db: "not ready"
    });
  }
});

app.post("/api/signup", async (req, res) => {
  try {
    const name = normalizeText(req.body?.name);
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || "");

    if (!name || !email || !password) {
      return res.status(400).json({ error: "Name, email, and password are required." });
    }

    if (password.length < 6) {
      return res.status(400).json({ error: "Password must be at least 6 characters long." });
    }

    const existing = await dbGet(`SELECT id FROM users WHERE email = $1`, [email]);

    if (existing) {
      return res.status(409).json({ error: "An account with this email already exists." });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const insertResult = await dbRun(
      `INSERT INTO users (name, email, password_hash, plan, is_verified, is_admin)
       VALUES ($1, $2, $3, 'free', FALSE, FALSE)
       RETURNING id`,
      [name, email, passwordHash]
    );

    const user = await dbGet(
      `SELECT id, name, email, plan, is_verified, is_admin FROM users WHERE id = $1`,
      [insertResult.rows[0].id]
    );

    const token = await createVerificationToken(user.id);

    try {
      await sendVerificationEmail(user, token);
      return res.json({
        message: "Account created. Please verify your email before logging in."
      });
    } catch (mailErr) {
      console.error("verification email send failed:", mailErr);
      return res.status(500).json({
        error:
          "Account created, but verification email could not be delivered right now. Please use Resend Verification from the login page."
      });
    }
  } catch (err) {
    console.error("signup error:", err);
    return res.status(500).json({ error: "Signup failed." });
  }
});

app.post("/api/login", async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return res.status(400).json({ error: "Email and password are required." });
    }

    const userRow = await dbGet(`SELECT * FROM users WHERE email = $1`, [email]);

    if (!userRow) {
      return res.status(401).json({ error: "Invalid email or password." });
    }

    const passwordOk = await bcrypt.compare(password, userRow.password_hash);

    if (!passwordOk) {
      return res.status(401).json({ error: "Invalid email or password." });
    }

    if (!userRow.is_verified) {
      return res.status(403).json({ error: "Please verify your email before logging in." });
    }

    const user = {
      id: userRow.id,
      name: userRow.name,
      email: userRow.email,
      plan: userRow.plan,
      is_verified: userRow.is_verified,
      is_admin: userRow.is_admin
    };

    const token = createAuthToken(user);

    return res.json({
      message: "Login successful.",
      token,
      user: sanitizeUser(user)
    });
  } catch (err) {
    console.error("login error:", err);
    return res.status(500).json({ error: "Login failed." });
  }
});

app.post("/api/resend-verification", async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);

    if (!email) {
      return res.status(400).json({ error: "Email is required." });
    }

    const user = await dbGet(
      `SELECT id, name, email, plan, is_verified, is_admin FROM users WHERE email = $1`,
      [email]
    );

    if (!user) {
      return res.json({ message: "If an account exists, a verification email has been sent." });
    }

    if (user.is_verified) {
      return res.json({ message: "This email is already verified." });
    }

    const token = await createVerificationToken(user.id);

    try {
      await sendVerificationEmail(user, token);
      return res.json({ message: "Verification email sent." });
    } catch (mailErr) {
      console.error("resend verification email failed:", mailErr);
      return res.status(500).json({
        error: "Could not send verification email right now. Please try again later."
      });
    }
  } catch (err) {
    console.error("resend verification error:", err);
    return res.status(500).json({ error: "Could not resend verification email." });
  }
});

app.get("/api/verify-email", async (req, res) => {
  try {
    const token = normalizeText(req.query?.token);

    if (!token) {
      return res.status(400).send(`
        <html>
          <head>
            <title>Verification Failed</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
          </head>
          <body style="font-family:Arial,sans-serif;background:#f6f7fb;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;">
            <div style="background:#fff;padding:32px;border-radius:16px;max-width:520px;width:90%;box-shadow:0 10px 30px rgba(0,0,0,.08);text-align:center;">
              <h1 style="margin:0 0 12px;color:#dc2626;">Verification Failed</h1>
              <p style="color:#555;line-height:1.7;">This verification link is missing or invalid.</p>
              <a href="${APP_BASE_URL}/login.html" style="display:inline-block;margin-top:18px;padding:12px 18px;background:#4f46e5;color:#fff;text-decoration:none;border-radius:8px;font-weight:700;">
                Go to Login
              </a>
            </div>
          </body>
        </html>
      `);
    }

    const row = await dbGet(
      `SELECT id, user_id, expires_at
       FROM email_verification_tokens
       WHERE token = $1`,
      [token]
    );

    if (!row) {
      return res.status(400).send(`
        <html>
          <head>
            <title>Verification Failed</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
          </head>
          <body style="font-family:Arial,sans-serif;background:#f6f7fb;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;">
            <div style="background:#fff;padding:32px;border-radius:16px;max-width:520px;width:90%;box-shadow:0 10px 30px rgba(0,0,0,.08);text-align:center;">
              <h1 style="margin:0 0 12px;color:#dc2626;">Verification Failed</h1>
              <p style="color:#555;line-height:1.7;">This verification link is invalid or has already been used.</p>
              <a href="${APP_BASE_URL}/login.html" style="display:inline-block;margin-top:18px;padding:12px 18px;background:#4f46e5;color:#fff;text-decoration:none;border-radius:8px;font-weight:700;">
                Go to Login
              </a>
            </div>
          </body>
        </html>
      `);
    }

    const expired = new Date(row.expires_at).getTime() < Date.now();

    if (expired) {
      await dbRun(`DELETE FROM email_verification_tokens WHERE id = $1`, [row.id]);

      return res.status(400).send(`
        <html>
          <head>
            <title>Link Expired</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
          </head>
          <body style="font-family:Arial,sans-serif;background:#f6f7fb;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;">
            <div style="background:#fff;padding:32px;border-radius:16px;max-width:520px;width:90%;box-shadow:0 10px 30px rgba(0,0,0,.08);text-align:center;">
              <h1 style="margin:0 0 12px;color:#dc2626;">Link Expired</h1>
              <p style="color:#555;line-height:1.7;">Your verification link has expired. Please go back to login and resend the verification email.</p>
              <a href="${APP_BASE_URL}/login.html" style="display:inline-block;margin-top:18px;padding:12px 18px;background:#4f46e5;color:#fff;text-decoration:none;border-radius:8px;font-weight:700;">
                Go to Login
              </a>
            </div>
          </body>
        </html>
      `);
    }

    await dbRun(`UPDATE users SET is_verified = TRUE WHERE id = $1`, [row.user_id]);
    await dbRun(`DELETE FROM email_verification_tokens WHERE user_id = $1`, [row.user_id]);

    return res.send(`
      <html>
        <head>
          <title>Email Verified</title>
          <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        </head>
        <body style="font-family:Arial,sans-serif;background:#f6f7fb;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;">
          <div style="background:#fff;padding:32px;border-radius:16px;max-width:520px;width:90%;box-shadow:0 10px 30px rgba(0,0,0,.08);text-align:center;">
            <div style="font-size:46px;margin-bottom:8px;">✅</div>
            <h1 style="margin:0 0 12px;color:#111827;">Email verified successfully</h1>
            <p style="color:#555;line-height:1.7;">
              Your VoicePunjabAI account is now verified. You can log in and start using Punjabi text-to-speech.
            </p>
            <a href="${APP_BASE_URL}/login.html?verified=1" style="display:inline-block;margin-top:18px;padding:12px 18px;background:#4f46e5;color:#fff;text-decoration:none;border-radius:8px;font-weight:700;">
              Go to Login
            </a>
          </div>
        </body>
      </html>
    `);
  } catch (err) {
    console.error("verify email error:", err);
    return res.status(500).send(`
      <html>
        <head>
          <title>Verification Error</title>
          <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        </head>
        <body style="font-family:Arial,sans-serif;background:#f6f7fb;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;">
          <div style="background:#fff;padding:32px;border-radius:16px;max-width:520px;width:90%;box-shadow:0 10px 30px rgba(0,0,0,.08);text-align:center;">
            <h1 style="margin:0 0 12px;color:#dc2626;">Verification Error</h1>
            <p style="color:#555;line-height:1.7;">Something went wrong while verifying your email. Please try again later.</p>
            <a href="${APP_BASE_URL}/login.html" style="display:inline-block;margin-top:18px;padding:12px 18px;background:#4f46e5;color:#fff;text-decoration:none;border-radius:8px;font-weight:700;">
              Go to Login
            </a>
          </div>
        </body>
      </html>
    `);
  }
});

app.post("/api/forgot-password", async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);

    if (!email) {
      return res.status(400).json({ error: "Email is required." });
    }

    const user = await dbGet(`SELECT id, name, email FROM users WHERE email = $1`, [email]);

    if (!user) {
      return res.json({
        message: "If an account exists for this email, a reset link has been sent."
      });
    }

    const token = await createPasswordResetToken(user.id);

    try {
      await sendPasswordResetEmail(user, token);
      return res.json({
        message: "If an account exists for this email, a reset link has been sent."
      });
    } catch (mailErr) {
      console.error("password reset email failed:", mailErr);
      return res.status(500).json({
        error: "Could not send reset email right now. Please try again later."
      });
    }
  } catch (err) {
    console.error("forgot password error:", err);
    return res.status(500).json({ error: "Could not process password reset request." });
  }
});

app.post("/api/reset-password", async (req, res) => {
  try {
    const token = normalizeText(req.body?.token);
    const password = String(req.body?.password || "");

    if (!token || !password) {
      return res.status(400).json({ error: "Token and password are required." });
    }

    if (password.length < 6) {
      return res.status(400).json({ error: "Password must be at least 6 characters long." });
    }

    const row = await dbGet(
      `SELECT id, user_id, expires_at
       FROM password_reset_tokens
       WHERE token = $1`,
      [token]
    );

    if (!row) {
      return res.status(400).json({ error: "Invalid or expired reset link." });
    }

    const expired = new Date(row.expires_at).getTime() < Date.now();

    if (expired) {
      await dbRun(`DELETE FROM password_reset_tokens WHERE id = $1`, [row.id]);
      return res.status(400).json({ error: "Invalid or expired reset link." });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    await dbRun(`UPDATE users SET password_hash = $1 WHERE id = $2`, [
      passwordHash,
      row.user_id
    ]);

    await dbRun(`DELETE FROM password_reset_tokens WHERE user_id = $1`, [row.user_id]);

    return res.json({
      message: "Password updated successfully. You can now log in."
    });
  } catch (err) {
    console.error("reset password error:", err);
    return res.status(500).json({ error: "Could not reset password." });
  }
});

app.get("/api/me", (req, res) => {
  if (!req.user) {
    return res.json({ loggedIn: false, user: null });
  }

  return res.json({ loggedIn: true, user: sanitizeUser(req.user) });
});

app.get("/api/history", requireAuth, async (req, res) => {
  try {
    const rows = await dbAll(
      `SELECT id, original_text, voice, speed, pitch, audio_url, created_at
       FROM audio_history
       WHERE user_id = $1
       ORDER BY created_at DESC
       LIMIT 50`,
      [req.user.id]
    );

    return res.json({ items: rows || [] });
  } catch (err) {
    console.error("history error:", err);
    return res.status(500).json({ error: "Failed to load audio history." });
  }
});

app.delete("/api/history/:id", requireAuth, async (req, res) => {
  try {
    const historyId = Number(req.params.id);

    if (!Number.isInteger(historyId) || historyId <= 0) {
      return res.status(400).json({ error: "Invalid history item id." });
    }

    const existing = await dbGet(
      `SELECT id FROM audio_history WHERE id = $1 AND user_id = $2`,
      [historyId, req.user.id]
    );

    if (!existing) {
      return res.status(404).json({ error: "History item not found." });
    }

    await dbRun(`DELETE FROM audio_history WHERE id = $1 AND user_id = $2`, [
      historyId,
      req.user.id
    ]);

    return res.json({ message: "History item deleted successfully." });
  } catch (err) {
    console.error("delete history error:", err);
    return res.status(500).json({ error: "Failed to delete history item." });
  }
});

app.get("/api/usage", async (req, res) => {
  try {
    const trackingId = getTrackingId(req);
    const plan = req.user?.plan || "guest";
    const limits = getPlanLimits(plan);

    const translateUsed = await getTodayUsage(trackingId, "convert");
    const ttsUsed = await getTodayUsage(trackingId, "tts");

    return res.json({
      plan,
      translate: {
        used: translateUsed,
        limit: limits.translate,
        remaining: Math.max(0, limits.translate - translateUsed)
      },
      tts: {
        used: ttsUsed,
        limit: limits.tts,
        remaining: Math.max(0, limits.tts - ttsUsed)
      }
    });
  } catch (err) {
    console.error("usage error:", err);
    return res.status(500).json({ error: "Failed to load usage stats." });
  }
});

app.get("/api/admin/stats", requireAdmin, async (req, res) => {
  try {
    const freeLimits = getPlanLimits("free");

    const [
      translateToday,
      ttsToday,
      translateCharsToday,
      ttsCharsToday,
      translateCacheHitsToday,
      translateCacheMissesToday,
      ttsCacheHitsToday,
      ttsCacheMissesToday,
      translationCacheCount,
      ttsCacheCount,
      usageLogCount,
      requestLogCount,
      topClientsToday
    ] = await Promise.all([
      dbGet(`SELECT COUNT(*) AS total FROM request_logs WHERE endpoint = 'convert' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COUNT(*) AS total FROM request_logs WHERE endpoint = 'tts' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COALESCE(SUM(char_count), 0) AS total FROM usage_logs WHERE endpoint = 'convert' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COALESCE(SUM(char_count), 0) AS total FROM usage_logs WHERE endpoint = 'tts' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COUNT(*) AS total FROM request_logs WHERE endpoint = 'convert' AND cache_status = 'hit' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COUNT(*) AS total FROM request_logs WHERE endpoint = 'convert' AND cache_status = 'miss' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COUNT(*) AS total FROM request_logs WHERE endpoint = 'tts' AND cache_status = 'hit' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COUNT(*) AS total FROM request_logs WHERE endpoint = 'tts' AND cache_status = 'miss' AND created_at::date = CURRENT_DATE`),
      dbGet(`SELECT COUNT(*) AS total FROM translation_cache`),
      dbGet(`SELECT COUNT(*) AS total FROM tts_cache`),
      dbGet(`SELECT COUNT(*) AS total FROM usage_logs`),
      dbGet(`SELECT COUNT(*) AS total FROM request_logs`),
      dbAll(`SELECT client_id, COUNT(*) AS requests, COALESCE(SUM(char_count), 0) AS chars
             FROM request_logs
             WHERE created_at::date = CURRENT_DATE
             GROUP BY client_id
             ORDER BY requests DESC, chars DESC
             LIMIT 5`)
    ]);

    const translateHits = Number(translateCacheHitsToday?.total || 0);
    const translateMisses = Number(translateCacheMissesToday?.total || 0);
    const ttsHits = Number(ttsCacheHitsToday?.total || 0);
    const ttsMisses = Number(ttsCacheMissesToday?.total || 0);

    return res.json({
      today: {
        translate_requests: Number(translateToday?.total || 0),
        tts_requests: Number(ttsToday?.total || 0),
        translate_characters: Number(translateCharsToday?.total || 0),
        tts_characters: Number(ttsCharsToday?.total || 0),
        translate_cache_hit_rate: translateHits + translateMisses
          ? `${Math.round((translateHits / (translateHits + translateMisses)) * 100)}%`
          : "0%",
        tts_cache_hit_rate: ttsHits + ttsMisses
          ? `${Math.round((ttsHits / (ttsHits + ttsMisses)) * 100)}%`
          : "0%"
      },
      totals: {
        translation_cache_records: Number(translationCacheCount?.total || 0),
        tts_cache_records: Number(ttsCacheCount?.total || 0),
        usage_log_rows: Number(usageLogCount?.total || 0),
        request_log_rows: Number(requestLogCount?.total || 0)
      },
      limits: {
        daily_translate_char_limit: freeLimits.translate,
        daily_tts_char_limit: freeLimits.tts
      },
      top_clients_today: topClientsToday || []
    });
  } catch (err) {
    console.error("admin stats error:", err);
    return res.status(500).json({ error: "Failed to load admin stats." });
  }
});

app.post("/api/convert", async (req, res) => {
  const rawText = normalizeText(req.body?.text);
  const mode = normalizeText(req.body?.mode || "english").toLowerCase();

  if (!rawText) {
    return res.json({ gurmukhi: "", note: "Nothing to convert." });
  }

  if (rawText.length > MAX_CONVERT_TEXT_LENGTH) {
    return res.status(400).json({
      gurmukhi: "",
      note: `Text is too long. Please keep it under ${MAX_CONVERT_TEXT_LENGTH} characters.`
    });
  }

  const trackingId = getTrackingId(req);
  const plan = req.user?.plan || "guest";
  const limits = getPlanLimits(plan);
  const todaysUsage = await getTodayUsage(trackingId, "convert");

  if (todaysUsage + rawText.length > limits.translate) {
    return res.status(429).json({
      gurmukhi: "",
      note: "Daily translation limit reached. Please try again tomorrow."
    });
  }

  try {
    if (containsGurmukhi(rawText)) {
      return res.json({ gurmukhi: rawText, note: "Text is already in Punjabi." });
    }

    if (mode !== "english") {
      return res.json({
        gurmukhi: rawText,
        note: "Only English to Punjabi conversion is enabled."
      });
    }

    const result = await translateEnglishToPunjabi(rawText);

    await logUsage(trackingId, "convert", rawText.length);
    await logRequest("convert", result.cached ? "hit" : "miss", rawText.length, trackingId);

    return res.json({
      gurmukhi: result.translatedText,
      note: result.cached ? "Translated from cache." : "Translated successfully."
    });
  } catch (err) {
    console.error("convert error:", err);
    return res.status(500).json({
      gurmukhi: rawText,
      error: "convert failed",
      note: err.message || "Conversion failed."
    });
  }
});

app.post("/api/tts", async (req, res) => {
  try {
    const rawText = normalizeText(req.body?.text);
    const voice = normalizeText(req.body?.voice || "pa-IN-Standard-A");
    const speed = Number(req.body?.speed ?? 1);
    const pitch = Number(req.body?.pitch ?? 0);

    if (!rawText) {
      return res.status(400).json({ error: "Text required." });
    }

    if (rawText.length > MAX_TTS_TEXT_LENGTH) {
      return res.status(400).json({
        error: `Text too long. Maximum ${MAX_TTS_TEXT_LENGTH} characters.`
      });
    }

    if (!containsGurmukhi(rawText)) {
      return res.status(400).json({
        error: "Please enter Punjabi text in Gurmukhi before generating speech."
      });
    }

    if (!isValidVoice(voice) || !isValidSpeed(speed) || !isValidPitch(pitch)) {
      return res.status(400).json({ error: "Invalid voice settings." });
    }

    const normalizedPunjabi = normalizePunjabiForCache(rawText);
    const trackingId = getTrackingId(req);
    const plan = req.user?.plan || "guest";
    const limits = getPlanLimits(plan);
    const todaysUsage = await getTodayUsage(trackingId, "tts");

    if (todaysUsage + rawText.length > limits.tts) {
      return res.status(429).json({
        error: "Daily TTS limit reached. Please try again tomorrow."
      });
    }

    const cacheKey = makeTtsCacheKey({
      text: normalizedPunjabi,
      voice,
      speed,
      pitch
    });

    const existing = await dbGet(
      `SELECT file_name FROM tts_cache WHERE cache_key = $1`,
      [cacheKey]
    );

    let audioBase64 = "";
    let wasCached = false;

    if (existing) {
      const cachedFilePath = path.join(CACHE_DIR, existing.file_name);
      if (fs.existsSync(cachedFilePath)) {
        const buffer = fs.readFileSync(cachedFilePath);
        audioBase64 = buffer.toString("base64");
        wasCached = true;
      }
    }

    if (!audioBase64) {
      const [response] = await ttsClient.synthesizeSpeech({
        input: { text: rawText },
        voice: { languageCode: "pa-IN", name: voice },
        audioConfig: {
          audioEncoding: "MP3",
          speakingRate: speed,
          pitch
        }
      });

      const fileName = `${cacheKey}.mp3`;
      const filePath = path.join(CACHE_DIR, fileName);

      const buffer = Buffer.isBuffer(response.audioContent)
        ? response.audioContent
        : Buffer.from(response.audioContent, "binary");

      fs.writeFileSync(filePath, buffer);

      await dbRun(
        `INSERT INTO tts_cache (cache_key, text, voice, speed, pitch, file_name)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (cache_key) DO NOTHING`,
        [cacheKey, normalizedPunjabi, voice, speed, pitch, fileName]
      );

      audioBase64 = buffer.toString("base64");
    }

    await logUsage(trackingId, "tts", rawText.length);
    await logRequest("tts", wasCached ? "hit" : "miss", rawText.length, trackingId);

    if (req.user?.id) {
      await saveAudioHistory(req.user.id, rawText, voice, speed, pitch, "");
    }

    return res.json({
      audioBase64,
      mimeType: "audio/mpeg",
      cached: wasCached
    });
  } catch (err) {
    console.error("tts error:", err);
    return res.status(500).json({
      error: "TTS failed",
      details: err.message || "Unknown TTS error"
    });
  }
});

app.get("/api/test-email", async (req, res) => {
  try {
    if (!resend) {
      return res.status(500).json({ error: "Resend not configured." });
    }

    const to = String(req.query.to || "").trim();

    if (!to) {
      return res.status(400).json({ error: "Add ?to=youremail@example.com" });
    }

    const response = await resend.emails.send({
      from: RESEND_FROM,
      to,
      subject: "VoicePunjabAI test email",
      text: `Hello,

This is a test email from VoicePunjabAI.

If you received this message, your email delivery is working.

VoicePunjabAI Team`,
      html: `
        <div style="font-family:Arial,sans-serif;line-height:1.7;color:#222;max-width:600px;">
          <p>Hello,</p>
          <p>This is a test email from <strong>VoicePunjabAI</strong>.</p>
          <p>If you received this message, your email delivery is working.</p>
          <p>VoicePunjabAI Team</p>
        </div>
      `
    });

    if (response?.error) {
      console.error("test email error:", response.error);
      return res.status(500).json({
        error: "Test email failed.",
        details: response.error.message || "Unknown email error"
      });
    }

    return res.json({
      message: "Test email sent."
    });
  } catch (err) {
    console.error("test email error:", err);
    return res.status(500).json({
      error: "Test email failed.",
      details: err.message
    });
  }
});

async function initializeApp() {
  try {
    await dbGet("SELECT 1 AS ok");
    console.log("Database connected successfully.");

    await initDatabase();
    console.log("initDatabase finished successfully.");

    await ensureAdminUser();

    const tables = await dbAll(`
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
      ORDER BY table_schema, table_name
    `);

    console.log("Tables after init:", tables);
  } catch (err) {
    console.error("Startup DB init failed:", err);
  }
}

function startServer() {
  console.log("Starting VoicePunjab API...");
  console.log("PORT =", PORT);
  console.log("DATABASE_URL =", DATABASE_URL ? "(set)" : "(missing)");
  console.log("RESEND_API_KEY =", RESEND_API_KEY ? "(set)" : "(missing)");
  console.log("ADMIN_EMAIL =", ADMIN_EMAIL || "(missing)");

  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`VoicePunjab API running on port ${PORT}`);
    initializeApp();
  });

  server.on("error", (err) => {
    console.error("Server listen error:", err);
  });
}

app.get("/", (req, res) => {
  res.json({
    message: "VoicePunjab API is running.",
    health: "/health"
  });
});

app.use((req, res) => {
  res.status(404).json({
    error: "Route not found"
  });
});

startServer();

{}