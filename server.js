const express = require("express");
const bodyParser = require("body-parser");
const Redis = require("ioredis");
const crypto = require("crypto");

const app = express();

app.use(bodyParser.json({ limit: "1mb" }));
app.use(bodyParser.urlencoded({ extended: true, limit: "1mb" }));

// ============================================================
// ENV
// ============================================================

const REDIS_URL = process.env.REDIS_URL || "";
const ALLOWED_UNIVERSES = (process.env.ALLOWED_UNIVERSES || "")
    .split(",")
    .map(v => v.trim())
    .filter(Boolean);

const ROBLOX_TOPIC = process.env.ROBLOX_TOPIC || "DonationV1";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";
const WEBHOOK_DEBUG = process.env.WEBHOOK_DEBUG === "true";

let redis = null;

if (REDIS_URL) {
    redis = new Redis(REDIS_URL, {
        maxRetriesPerRequest: 1,
        enableReadyCheck: false,
        lazyConnect: false,
    });

    redis.on("error", (err) => {
        console.warn("[REDIS ERROR]", err?.message || err);
    });
} else {
    console.warn("[WARN] REDIS_URL belum diset. Redis history/dedupe akan dilewati.");
}

// ============================================================
// BASIC HELPERS
// ============================================================

function isAllowedUniverse(universeId) {
    return ALLOWED_UNIVERSES.includes(String(universeId));
}

function getRobloxApiKey(universeId) {
    return process.env[`ROBLOX_API_KEY_${universeId}`] || process.env.ROBLOX_API_KEY || "";
}

function verifyWebhookSecret(req) {
    // Kalau WEBHOOK_SECRET kosong, secret check dimatikan.
    // Untuk production sebaiknya tetap isi WEBHOOK_SECRET di Vercel.
    if (!WEBHOOK_SECRET) return true;

    const fromQuery = req.query.secret;
    const fromHeader = req.headers["x-webhook-secret"];

    return fromQuery === WEBHOOK_SECRET || fromHeader === WEBHOOK_SECRET;
}

function parseAmount(val) {
    if (!val) return 0;
    return Number(String(val).replace(/[^\d]/g, ""));
}

function getPayloadViews(raw) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return [];

    // Provider webhook kadang mengirim field langsung, kadang dibungkus.
    return [
        raw,
        raw.data,
        raw.donation,
        raw.transaction,
        raw.payload,
        raw.result,
    ].filter(value => value && typeof value === "object" && !Array.isArray(value));
}

function firstWebhookValue(raw, fieldNames) {
    for (const view of getPayloadViews(raw)) {
        for (const fieldName of fieldNames) {
            const value = view[fieldName];

            if (value !== undefined && value !== null && value !== "") {
                return value;
            }
        }
    }

    return "";
}

function listWebhookFields(raw) {
    const fields = [];

    for (const [containerIndex, view] of getPayloadViews(raw).entries()) {
        const prefix = containerIndex === 0 ? "root" : `nested${containerIndex}`;
        fields.push(...Object.keys(view).slice(0, 40).map(key => `${prefix}.${key}`));
    }

    return [...new Set(fields)].slice(0, 100);
}

function createDonationHash(source, donorName, amount, message, providerId) {
    if (providerId) {
        return crypto
            .createHash("sha256")
            .update(`${source}:${providerId}`)
            .digest("hex");
    }

    // Bucket 5 detik untuk mencegah duplicate webhook double-send.
    const timeBucket = Math.floor(Date.now() / 5000);

    return crypto
        .createHash("sha256")
        .update(
            `${source}:${String(donorName)}:${String(amount)}:${String(message || "")}:${String(timeBucket)}`
        )
        .digest("hex");
}

function validateWebhookBase(req, res) {
    const universeId = String(req.params.universeId || "");

    if (!universeId) {
        res.status(400).json({
            ok: false,
            error: "MISSING_UNIVERSE_ID",
        });
        return null;
    }

    if (!isAllowedUniverse(universeId)) {
        res.status(403).json({
            ok: false,
            error: "UNAUTHORIZED_UNIVERSE",
            universeId,
        });
        return null;
    }

    if (!verifyWebhookSecret(req)) {
        res.status(401).json({
            ok: false,
            error: "INVALID_WEBHOOK_SECRET",
        });
        return null;
    }

    return universeId;
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

// ============================================================
// ROBLOX OPEN CLOUD PUSH
// ============================================================

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

    // MessagingService payload kecil, jadi dipotong aman.
    if (Buffer.byteLength(message, "utf8") > 950) {
        payload.donation.message = String(payload.donation.message || "").slice(0, 80);
        message = JSON.stringify(payload);
    }

    const url = `https://apis.roblox.com/cloud/v2/universes/${universeId}:publishMessage`;

    try {
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
    } catch (err) {
        console.error("[ROBLOX PUBLISH ERROR]", err?.message || err);

        return {
            ok: false,
            reason: "ROBLOX_PUBLISH_ERROR",
            message: err?.message || String(err),
        };
    }
}

// ============================================================
// REDIS BEST-EFFORT
// Redis tidak wajib. Kalau Upstash limit/error, donation tetap push.
// ============================================================

async function markDuplicateBestEffort(hash) {
    if (!redis) {
        return {
            checked: false,
            duplicate: false,
            reason: "REDIS_DISABLED",
        };
    }

    try {
        const inserted = await redis.set(
            `donationHash:${hash}`,
            "1",
            "EX",
            300,
            "NX"
        );

        if (!inserted) {
            return {
                checked: true,
                duplicate: true,
            };
        }

        return {
            checked: true,
            duplicate: false,
        };
    } catch (err) {
        console.warn("[REDIS DEDUPE SKIPPED]", err?.message || err);

        return {
            checked: false,
            duplicate: false,
            reason: "REDIS_ERROR",
        };
    }
}

async function saveDonationBestEffort(universeId, donation) {
    if (!redis) {
        return {
            ok: false,
            reason: "REDIS_DISABLED",
        };
    }

    try {
        await redis.zadd(
            `donations:${universeId}`,
            donation.timestamp,
            JSON.stringify(donation)
        );

        await redis.set(
            `lastDonationId:${universeId}`,
            String(donation.timestamp)
        );

        return {
            ok: true,
        };
    } catch (err) {
        console.warn("[REDIS SAVE SKIPPED]", err?.message || err);

        return {
            ok: false,
            reason: "REDIS_ERROR",
            message: err?.message || String(err),
        };
    }
}

async function saveAndPublishDonation(universeId, donation) {
    // PENTING:
    // Push ke Roblox dulu. Jadi kalau Redis/Upstash limit, donation tetap masuk realtime.
    const publishResult = await publishDonationToRoblox(universeId, donation);

    // Redis hanya untuk history/dedupe/fallback. Tidak boleh menggagalkan donation.
    const saveResult = await saveDonationBestEffort(universeId, donation);

    return {
        publishResult,
        saveResult,
    };
}

// ============================================================
// LEGACY SESSION/POLLING DISABLED
// Tidak ada session Redis lagi.
// ============================================================

app.post("/api/session", (req, res) => {
    res.status(410).json({
        ok: false,
        reason: "LEGACY_SESSION_DISABLED",
        message: "Donation bridge now uses OpenCloudPush only. Roblox server must not call /api/session.",
    });
});

app.get("/api/tail", (req, res) => {
    res.status(410).json({
        ok: false,
        reason: "LEGACY_POLLING_DISABLED",
        message: "Donation bridge now uses OpenCloudPush only. Roblox server must not call /api/tail.",
    });
});

app.get("/api/donations", (req, res) => {
    res.status(410).json({
        ok: false,
        reason: "LEGACY_POLLING_DISABLED",
        message: "Donation bridge now uses OpenCloudPush only. Roblox server must not call /api/donations.",
    });
});

// ============================================================
// WEBHOOK SAWERIA
// URL:
// POST /webhook/saweria/:universeId?secret=WEBHOOK_SECRET
// ============================================================

app.post("/webhook/saweria/:universeId", async (req, res) => {
    try {
        const universeId = validateWebhookBase(req, res);
        if (!universeId) return;

        const raw = req.body || {};

        if (raw.type && raw.type !== "donation") {
            return res.json({
                ok: true,
                ignored: true,
                reason: "NOT_DONATION_EVENT",
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

        const providerId =
            raw.id ||
            raw.transaction_id ||
            raw.invoice_id ||
            raw.payment_id ||
            "";

        const hash = createDonationHash(
            "saweria",
            donorName,
            amount,
            message,
            providerId
        );

        const dedupe = await markDuplicateBestEffort(hash);

        if (dedupe.duplicate) {
            return res.json({
                ok: true,
                duplicate: true,
            });
        }

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

        const result = await saveAndPublishDonation(universeId, donation);

        res.json({
            ok: true,
            donationId: donation.id,
            pushedToRoblox: result.publishResult.ok === true,
            publishResult: result.publishResult,
            savedToRedis: result.saveResult.ok === true,
            saveResult: result.saveResult,
            dedupe,
        });
    } catch (err) {
        console.error("[WEBHOOK SAWERIA ERROR]", err);
        res.status(500).json({
            ok: false,
            error: "INTERNAL_ERROR",
        });
    }
});

// ============================================================
// WEBHOOK BAGIBAGI
// URL:
// POST /webhook/bagibagi/:universeId?secret=WEBHOOK_SECRET
// ============================================================

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
            raw.donator_name ||
            "Anonymous";

        const message =
            raw.message ||
            raw.note ||
            raw.pesan ||
            "";

        const providerId =
            raw.id ||
            raw.transaction_id ||
            raw.order_id ||
            raw.invoice_id ||
            "";

        const hash = createDonationHash(
            "bagibagi",
            donorName,
            amount,
            message,
            providerId
        );

        const dedupe = await markDuplicateBestEffort(hash);

        if (dedupe.duplicate) {
            return res.json({
                ok: true,
                duplicate: true,
            });
        }

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

        const result = await saveAndPublishDonation(universeId, donation);

        res.json({
            ok: true,
            donationId: donation.id,
            pushedToRoblox: result.publishResult.ok === true,
            publishResult: result.publishResult,
            savedToRedis: result.saveResult.ok === true,
            saveResult: result.saveResult,
            dedupe,
        });
    } catch (err) {
        console.error("[WEBHOOK BAGIBAGI ERROR]", err);
        res.status(500).json({
            ok: false,
            error: "INTERNAL_ERROR",
        });
    }
});

// ============================================================
// WEBHOOK SOCIABUZZ
// URL:
// POST /webhook/sociabuzz/:universeId?secret=WEBHOOK_SECRET
//
// Sociabuzz dapat mengirim application/json atau form-urlencoded.
// Field dibuat toleran terhadap payload langsung maupun nested karena format
// integrasi dapat berbeda antar-produk/versi (TRIBE, support, transaction).
// ============================================================

app.post("/webhook/sociabuzz/:universeId", async (req, res) => {
    try {
        const universeId = validateWebhookBase(req, res);
        if (!universeId) return;

        const raw = req.body || {};

        if (WEBHOOK_DEBUG) {
            // Hanya nama field, tidak mencetak nama donor/pesan/nilai sensitif.
            console.log("[WEBHOOK SOCIABUZZ FIELDS]", listWebhookFields(raw));
        }

        const amount = parseAmount(firstWebhookValue(raw, [
            "amount",
            "amount_raw",
            "nominal",
            "value",
            "total",
            "total_amount",
            "donation_amount",
            "support_amount",
            "gross_amount",
        ]));

        if (!amount || amount <= 0) {
            return res.json({
                ok: true,
                ignored: true,
                reason: "INVALID_AMOUNT",
            });
        }

        const donorName = firstWebhookValue(raw, [
            "name",
            "supporter_name",
            "supporter",
            "donor_name",
            "donator_name",
            "donator",
            "username",
            "from_name",
            "customer_name",
        ]) || "Anonymous";

        const message = firstWebhookValue(raw, [
            "message",
            "supporter_message",
            "support_message",
            "note",
            "comment",
            "pesan",
        ]);

        const providerId = firstWebhookValue(raw, [
            "id",
            "transaction_id",
            "transactionId",
            "order_id",
            "invoice_id",
            "payment_id",
            "reference_id",
            "ref_id",
            "uuid",
        ]);

        const hash = createDonationHash(
            "sociabuzz",
            donorName,
            amount,
            message,
            providerId
        );

        const dedupe = await markDuplicateBestEffort(hash);

        if (dedupe.duplicate) {
            return res.json({
                ok: true,
                duplicate: true,
            });
        }

        const timestamp = Date.now();

        const donation = {
            id: hash,
            timestamp,
            source: "sociabuzz",
            donorName: String(donorName),
            amount: Number(amount),
            currency: String(firstWebhookValue(raw, ["currency", "currency_code"]) || "IDR"),
            message: String(message || ""),
        };

        const result = await saveAndPublishDonation(universeId, donation);

        res.json({
            ok: true,
            donationId: donation.id,
            pushedToRoblox: result.publishResult.ok === true,
            publishResult: result.publishResult,
            savedToRedis: result.saveResult.ok === true,
            saveResult: result.saveResult,
            dedupe,
        });
    } catch (err) {
        console.error("[WEBHOOK SOCIABUZZ ERROR]", err);
        res.status(500).json({
            ok: false,
            error: "INTERNAL_ERROR",
        });
    }
});

// ============================================================
// TEST OPEN CLOUD PUBLISH
// URL:
// POST /api/test/publish/:universeId?secret=WEBHOOK_SECRET
// ============================================================

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

        const result = await saveAndPublishDonation(universeId, donation);

        res.json({
            ok: true,
            donation,
            pushedToRoblox: result.publishResult.ok === true,
            publishResult: result.publishResult,
            savedToRedis: result.saveResult.ok === true,
            saveResult: result.saveResult,
        });
    } catch (err) {
        console.error("[TEST PUBLISH ERROR]", err);
        res.status(500).json({
            ok: false,
            error: "INTERNAL_ERROR",
        });
    }
});

// ============================================================
// HEALTH CHECK
// ============================================================

app.get("/", (req, res) => {
    res.json({
        ok: true,
        service: "roblox-donation-bridge",
        mode: "OpenCloudPush",
        legacySession: false,
        legacyPolling: false,
        topic: ROBLOX_TOPIC,
        allowedUniverses: ALLOWED_UNIVERSES,
        redisEnabled: Boolean(redis),
        webhookDebug: WEBHOOK_DEBUG,
        supportedProviders: ["saweria", "bagibagi", "sociabuzz"],
    });
});

app.get("/favicon.ico", (req, res) => {
    res.status(204).end();
});

app.get("/favicon.png", (req, res) => {
    res.status(204).end();
});

module.exports = app;
