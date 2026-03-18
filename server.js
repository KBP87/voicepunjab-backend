"use strict";

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
const nodemailer = require("nodemailer");
const { Pool } = require("pg");
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

const SMTP_HOST = String(process.env.SMTP_HOST || "").trim();
const SMTP_PORT = Number(process.env.SMTP_PORT || 465);
const SMTP_SECURE = String(process.env.SMTP_SECURE || "true").trim() === "true";
const SMTP_USER = String(process.env.SMTP_USER || "").trim();
const SMTP_PASS = String(process.env.SMTP_PASS || "").trim();
const SMTP_FROM = String(
  process.env.SMTP_FROM || "VoicePunjabAI <support@voicepunjabai.com>"
).trim();

const GOOGLE_CLOUD_PROJECT =
  process.env.GOOGLE_CLOUD_PROJECT ||
  process.env.GCLOUD_PROJECT ||
  process.env.GOOGLE_PROJECT_ID ||
  "";

const GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS || "";

const GOOGLE_CREDENTIALS_JSON =
  process.env.GOOGLE_CREDENTIALS_JSON || "";

const DB_USER = String(process.env.DB_USER || "").trim();
const DB_PASS = String(process.env.DB_PASS || "").trim();
const DB_NAME = String(process.env.DB_NAME || "").trim();
const INSTANCE_CONNECTION_NAME = String(
  process.env.INSTANCE_CONNECTION_NAME || ""
).trim();
const DB_HOST = String(process.env.DB_HOST || "").trim();
const DB_PORT = Number(process.env.DB_PORT || 5432);

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

function getPoolConfig() {
  const baseConfig = {
    user: DB_USER,
    password: DB_PASS,
    database: DB_NAME,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  };

  if (INSTANCE_CONNECTION_NAME) {
    return {
      ...baseConfig,
      host: `/cloudsql/${INSTANCE_CONNECTION_NAME}`,
      port: 5432
    };
  }

  return {
    ...baseConfig,
    host: DB_HOST || "127.0.0.1",
    port: DB_PORT
  };
}

const pool = new Pool(getPoolConfig());

let mailer = null;

if (SMTP_HOST && SMTP_USER && SMTP_PASS) {
  mailer = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: SMTP_SECURE,
    auth: {
      user: SMTP_USER,
      pass: SMTP_PASS
    }
  });
}

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

async function sendVerificationEmail(user, token) {
  const verifyUrl = `${API_PUBLIC_BASE_URL}/api/verify-email?token=${encodeURIComponent(token)}`;

  console.log("Verification URL for", user.email, ":", verifyUrl);

  if (!mailer) {
    console.log("Verification email not sent because SMTP is not configured.");
    return;
  }

  const info = await mailer.sendMail({
    from: SMTP_FROM,
    to: user.email,
    replyTo: "support@voicepunjabai.com",
    subject: "Verify your VoicePunjabAI email",
    text: `Hello ${user.name},

Please verify your email by opening this link:
${verifyUrl}

This link expires in 24 hours.

VoicePunjabAI`,
    html: `<div style="font-family:Arial,sans-serif;line-height:1.6;color:#222;">
      <p>Hello ${user.name},</p>
      <p>Please verify your email by clicking the button below:</p>
      <p>
        <a href="${verifyUrl}" style="display:inline-block;padding:10px 16px;background:#4f46e5;color:#ffffff;text-decoration:none;border-radius:8px;">
          Verify Email
        </a>
      </p>
      <p>If the button does not work, open this link:</p>
      <p><a href="${verifyUrl}">${verifyUrl}</a></p>
      <p>This link expires in 24 hours.</p>
      <p>VoicePunjabAI</p>
    </div>`
  });

  console.log("Verification email sent:", info.messageId);
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

  if (!mailer) {
    console.log("Password reset email not sent because SMTP is not configured.");
    console.log("Password reset URL:", resetUrl);
    return;
  }

  const info = await mailer.sendMail({
    from: SMTP_FROM,
    to: user.email,
    replyTo: "support@voicepunjabai.com",
    subject: "Reset your VoicePunjabAI password",
    text: `Hello ${user.name},

Open this link to reset your password:
${resetUrl}

If you did not request this, you can ignore this email.

VoicePunjabAI`,
    html: `<div style="font-family:Arial,sans-serif;line-height:1.6;color:#222;">
      <p>Hello ${user.name},</p>
      <p>Click below to reset your password:</p>
      <p>
        <a href="${resetUrl}" style="display:inline-block;padding:10px 16px;background:#4f46e5;color:#ffffff;text-decoration:none;border-radius:8px;">
          Reset Password
        </a>
      </p>
      <p>If the button does not work, open this link:</p>
      <p><a href="${resetUrl}">${resetUrl}</a></p>
      <p>If you did not request this, you can ignore this email.</p>
      <p>VoicePunjabAI</p>
    </div>`
  });

  console.log("Password reset email sent:", info.messageId);
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
    res.send("ok");
  } catch (err) {
    console.error("health error:", err.message);
    res.status(500).send("db not ready");
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

      return res.json({
        message:
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
      return res.redirect(`${APP_BASE_URL}/login.html?verified=0`);
    }

    const row = await dbGet(
      `SELECT id, user_id, expires_at
       FROM email_verification_tokens
       WHERE token = $1`,
      [token]
    );

    if (!row) {
      return res.redirect(`${APP_BASE_URL}/login.html?verified=0`);
    }

    const expired = new Date(row.expires_at).getTime() < Date.now();

    if (expired) {
      await dbRun(`DELETE FROM email_verification_tokens WHERE id = $1`, [row.id]);
      return res.redirect(`${APP_BASE_URL}/login.html?verified=0`);
    }

    await dbRun(`UPDATE users SET is_verified = TRUE WHERE id = $1`, [row.user_id]);
    await dbRun(`DELETE FROM email_verification_tokens WHERE user_id = $1`, [row.user_id]);

    return res.redirect(`${APP_BASE_URL}/login.html?verified=1`);
  } catch (err) {
    console.error("verify email error:", err);
    return res.redirect(`${APP_BASE_URL}/login.html?verified=0`);
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

    let audioUrl = "";
    let wasCached = false;

    if (existing) {
      const cachedFilePath = path.join(CACHE_DIR, existing.file_name);
      if (fs.existsSync(cachedFilePath)) {
        audioUrl = `/cache/${existing.file_name}`;
        wasCached = true;
      }
    }

    if (!audioUrl) {
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

      fs.writeFileSync(filePath, response.audioContent, "binary");

      await dbRun(
        `INSERT INTO tts_cache (cache_key, text, voice, speed, pitch, file_name)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (cache_key) DO NOTHING`,
        [cacheKey, normalizedPunjabi, voice, speed, pitch, fileName]
      );

      audioUrl = `/cache/${fileName}`;
    }

    await logUsage(trackingId, "tts", rawText.length);
    await logRequest("tts", wasCached ? "hit" : "miss", rawText.length, trackingId);

    if (req.user?.id) {
      await saveAudioHistory(req.user.id, rawText, voice, speed, pitch, audioUrl);
    }

    return res.json({ audioUrl, cached: wasCached });
  } catch (err) {
    console.error("tts error:", err);
    return res.status(500).json({
      error: "TTS failed",
      details: err.message || "Unknown TTS error"
    });
  }
});

app.get(/^(?!\/api\/|\/health|\/cache\/).*/, (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

async function startServer() {
  try {
    console.log("Starting server...");
    console.log("Connected DB_NAME =", DB_NAME || "(missing)");
    console.log("Connected DB_USER =", DB_USER || "(missing)");
    console.log("INSTANCE_CONNECTION_NAME =", INSTANCE_CONNECTION_NAME || "(missing)");
    console.log("DB_HOST =", DB_HOST || "(not set)");

    await dbGet("SELECT 1 AS ok");
    console.log("Database connected successfully.");

    await initDatabase();
    console.log("initDatabase finished successfully.");

    const tables = await dbAll(`
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
      ORDER BY table_schema, table_name
    `);

    console.log("Tables after init:", tables);

    app.listen(PORT, "0.0.0.0", () => {
      console.log(`VoicePunjab API running on port ${PORT}`);
      console.log("SMTP_HOST =", SMTP_HOST || "(missing)");
      console.log("SMTP_USER =", SMTP_USER || "(missing)");
    });
  } catch (err) {
    console.error("Server startup failed:", err);
    process.exit(1);
  }
}

startServer();
