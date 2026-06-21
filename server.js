const express = require("express");
const bodyParser = require("body-parser");
const Redis = require("ioredis");
const crypto = require("crypto");

const app = express();
app.use(bodyParser.json({ limit: "1mb" }));

if (!process.env.REDIS_URL) {
    console.warn("[WARN] REDIS_URL belum diset di Vercel Environment Variables");
}

const redis = new Redis(process.env.REDIS_URL);

const ALLOWED_UNIVERSES = (process.env.ALLOWED_UNIVERSES || "")
    .split(",")
    .map(v => v.trim())
    .filter(Boolean);

const ROBLOX_TOPIC = process.env.ROBLOX_TOPIC || "DonationV1";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";

function isAllowedUniverse(universeId) {
    return ALLOWED_UNIVERSES.includes(String(universeId));
}

function getRobloxApiKey(universeId) {
    return process.env[`ROBLOX_API_KEY_${universeId}`] || process.env.ROBLOX_API_KEY || "";
}

function verifyWebhookSecret(req) {
    if (!WEBHOOK_SECRET) return true;

    const fromQuery = req.query.secret;
    const fromHeader = req.headers["x-webhook-secret"];

    return fromQuery === WEBHOOK_SECRET || fromHeader === WEBHOOK_SECRET;
}

function createDonationHash(donorName, amount, message) {
    const timeBucket = Math.floor(Date.now() / 5000);

    return crypto
        .createHash("md5")
        .update(
            String(donorName) +
            String(amount) +
            String(message || "") +
            String(timeBucket)
        )
        .digest("hex");
}

function parseAmount(val) {
    if (!val) return 0;
    return Number(String(val).replace(/[^\d]/g, ""));
}

function compactDonation(donation) {
    return {
        type: "donation",
        donation: {
            id: String(donation.id || ""),
            timestamp: Number(donation.timestamp || Date.now()),
            source: String(donation.source || "unknown").slice(0, 20),
            donorName: String(donation.donorName || "Anonymous").slice(0, 40),
            amount: Number(donation.amount || 0),
            currency: String(donation.currency || "IDR").slice(0, 8),
            message: String(donation.message || "").slice(0, 220),
        },
    };
}

async function publishDonationToRoblox(universeId, donation) {
    const apiKey = getRobloxApiKey(universeId);

    if (!apiKey) {
        console.warn(`[ROBLOX PUBLISH SKIPPED] Missing API key for universe ${universeId}`);
        return {
            ok: false,
            reason: "MISSING_ROBLOX_API_KEY",
        };
    }

    let payload = compactDonation(donation);
    let message = JSON.stringify(payload);

    if (Buffer.byteLength(message, "utf8") > 950) {
        payload.donation.message = String(payload.donation.message || "").slice(0, 80);
        message = JSON.stringify(payload);
    }

    const url = `https://apis.roblox.com/cloud/v2/universes/${universeId}:publishMessage`;

    const response = await fetch(url, {
        method: "POST",
        headers: {
            "x-api-key": apiKey,
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            topic: ROBLOX_TOPIC,
            message,
        }),
    });

    if (!response.ok) {
        const text = await response.text().catch(() => "");
        console.error("[ROBLOX PUBLISH FAILED]", response.status, text);

        return {
            ok: false,
            status: response.status,
            body: text,
        };
    }

    return {
        ok: true,
    };
}

async function saveAndPublishDonation(universeId, donation) {
    await redis.zadd(
        `donations:${universeId}`,
        donation.timestamp,
        JSON.stringify(donation)
    );

    await redis.set(
        `lastDonationId:${universeId}`,
        String(donation.timestamp)
    );

    return await publishDonationToRoblox(universeId, donation);
}

function validateWebhookBase(req, res) {
    const universeId = String(req.params.universeId || "");

    if (!universeId) {
        res.status(400).json({
            error: "MISSING_UNIVERSE_ID",
        });
        return null;
    }

    if (!isAllowedUniverse(universeId)) {
        res.status(403).json({
            error: "UNAUTHORIZED_UNIVERSE",
            universeId,
        });
        return null;
    }

    if (!verifyWebhookSecret(req)) {
        res.status(401).json({
            error: "INVALID_WEBHOOK_SECRET",
        });
        return null;
    }

    return universeId;
}

// ================================
// LEGACY SESSION / POLLING ENDPOINTS
// Ini tetap disimpan untuk fallback.
// Tapi nanti polling Roblox lama harus dimatikan.
// ================================

app.post("/api/session", async (req, res) => {
    try {
        const universeId = String(req.body.universeId || "");

        if (!isAllowedUniverse(universeId)) {
            return res.json({
                ok: false,
                reason: "UNAUTHORIZED_UNIVERSE",
            });
        }

        const token = crypto.randomUUID();

        await redis.set(
            `session:${token}`,
            universeId,
            "EX",
            86400
        );

        res.json({
            ok: true,
            token,
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({
            ok: false,
            error: "INTERNAL_ERROR",
        });
    }
});

async function validateSession(req) {
    const token = req.headers["x-session"];
    if (!token) return null;

    return await redis.get(`session:${token}`);
}

app.get("/api/tail", async (req, res) => {
    try {
        const universeId = await validateSession(req);

        if (!universeId) {
            return res.json({
                ok: false,
                reason: "INVALID_SESSION",
            });
        }

        const lastId = await redis.get(`lastDonationId:${universeId}`);

        res.json({
            id: lastId || "0",
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({
            ok: false,
            error: "INTERNAL_ERROR",
        });
    }
});

app.get("/api/donations", async (req, res) => {
    try {
        const universeId = await validateSession(req);

        if (!universeId) {
            return res.json({
                ok: false,
                reason: "INVALID_SESSION",
            });
        }

        const after = req.query.after || "0";

        const items = await redis.zrangebyscore(
            `donations:${universeId}`,
            `(${after}`,
            "+inf"
        );

        const parsed = items.map(item => JSON.parse(item));

        res.json({
            items: parsed,
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({
            ok: false,
            error: "INTERNAL_ERROR",
        });
    }
});

// ================================
// WEBHOOK SAWERIA
// URL:
// /webhook/saweria/:universeId?secret=WEBHOOK_SECRET
// ================================

app.post("/webhook/saweria/:universeId", async (req, res) => {
    try {
        const universeId = validateWebhookBase(req, res);
        if (!universeId) return;

        const raw = req.body || {};

        if (raw.type && raw.type !== "donation") {
            return res.json({
                ok: true,
                ignored: true,
            });
        }

        const amount =
            parseAmount(raw.amount_raw) ||
            parseAmount(raw.amount) ||
            parseAmount(raw?.etc?.amount_to_display);

        if (!amount || amount <= 0) {
            return res.json({
                ok: true,
                ignored: true,
                reason: "INVALID_AMOUNT",
            });
        }

        const donorName =
            raw.donator_name ||
            raw.name ||
            raw.supporter ||
            "Anonymous";

        const message = String(raw.message || "");
        const hash = createDonationHash(donorName, amount, message);

        const exists = await redis.get(`donationHash:${hash}`);
        if (exists) {
            return res.json({
                ok: true,
                duplicate: true,
            });
        }

        await redis.set(`donationHash:${hash}`, "1", "EX", 300);

        const timestamp = Date.now();

        const donation = {
            id: hash,
            timestamp,
            source: "saweria",
            donorName: String(donorName),
            amount: Number(amount),
            currency: "IDR",
            message,
        };

        const publishResult = await saveAndPublishDonation(universeId, donation);

        res.json({
            ok: true,
            pushedToRoblox: publishResult.ok === true,
            publishResult,
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({
            error: "INTERNAL_ERROR",
        });
    }
});

// ================================
// WEBHOOK BAGIBAGI
// URL:
// /webhook/bagibagi/:universeId?secret=WEBHOOK_SECRET
// ================================

app.post("/webhook/bagibagi/:universeId", async (req, res) => {
    try {
        const universeId = validateWebhookBase(req, res);
        if (!universeId) return;

        const raw = req.body || {};

        const amount =
            parseAmount(raw.amount) ||
            parseAmount(raw.nominal) ||
            parseAmount(raw.value);

        if (!amount || amount <= 0) {
            return res.json({
                ok: true,
                ignored: true,
                reason: "INVALID_AMOUNT",
            });
        }

        const donorName =
            raw.name ||
            raw.username ||
            raw.donator ||
            "Anonymous";

        const message =
            raw.message ||
            raw.note ||
            raw.pesan ||
            "";

        const hash = createDonationHash(donorName, amount, message);

        const exists = await redis.get(`donationHash:${hash}`);
        if (exists) {
            return res.json({
                ok: true,
                duplicate: true,
            });
        }

        await redis.set(`donationHash:${hash}`, "1", "EX", 300);

        const timestamp = Date.now();

        const donation = {
            id: hash,
            timestamp,
            source: "bagibagi",
            donorName: String(donorName),
            amount: Number(amount),
            currency: "IDR",
            message: String(message),
        };

        const publishResult = await saveAndPublishDonation(universeId, donation);

        res.json({
            ok: true,
            pushedToRoblox: publishResult.ok === true,
            publishResult,
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({
            error: "INTERNAL_ERROR",
        });
    }
});

// ================================
// TEST OPEN CLOUD PUBLISH
// Pakai ini untuk test tanpa provider donation.
// URL:
// /api/test/publish/:universeId?secret=WEBHOOK_SECRET
// ================================

app.post("/api/test/publish/:universeId", async (req, res) => {
    try {
        const universeId = validateWebhookBase(req, res);
        if (!universeId) return;

        const timestamp = Date.now();

        const donation = {
            id: `test_${timestamp}`,
            timestamp,
            source: "manual",
            donorName: String(req.body?.donorName || "Test Donor"),
            amount: Number(req.body?.amount || 50000),
            currency: "IDR",
            message: String(req.body?.message || "Test Open Cloud donation"),
        };

        const publishResult = await saveAndPublishDonation(universeId, donation);

        res.json({
            ok: true,
            donation,
            pushedToRoblox: publishResult.ok === true,
            publishResult,
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({
            error: "INTERNAL_ERROR",
        });
    }
});

app.get("/", (req, res) => {
    res.json({
        ok: true,
        service: "roblox-donation-bridge",
        topic: ROBLOX_TOPIC,
        allowedUniverses: ALLOWED_UNIVERSES,
    });
});

module.exports = app;
