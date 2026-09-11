"use strict";

const express = require("express");
const bodyParser = require("body-parser");
const Redis = require("ioredis");
const crypto = require("crypto");

const app = express();

app.use(bodyParser.json({ limit: "1mb" }));
app.use(bodyParser.urlencoded({ extended: true, limit: "1mb" }));

// Environment
const REDIS_URL = process.env.REDIS_URL || "";
const ALLOWED_UNIVERSES = new Set(
    (process.env.ALLOWED_UNIVERSES || "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean)
);
const ROBLOX_TOPIC = process.env.ROBLOX_TOPIC || "DonationV1";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";
const WEBHOOK_DEBUG = process.env.WEBHOOK_DEBUG === "true";
const ENABLE_TEST_ENDPOINT = process.env.ENABLE_TEST_ENDPOINT === "true";
const EXCHANGE_RATE_API_KEY = process.env.EXCHANGE_RATE_API_KEY || "";
const FX_CACHE_SECONDS = Math.max(Number(process.env.FX_CACHE_SECONDS) || 21600, 300);
const MIN_IDR_AMOUNT = Math.max(Number(process.env.MIN_IDR_AMOUNT) || 1000, 1);

let manualIdrRates = {};
try {
    manualIdrRates = JSON.parse(process.env.FX_RATES_JSON || "{}");
} catch (error) {
    console.warn("[FX] FX_RATES_JSON tidak valid:", error?.message || error);
}

let redis = null;
if (REDIS_URL) {
    redis = new Redis(REDIS_URL, {
        maxRetriesPerRequest: 1,
        enableReadyCheck: false,
        lazyConnect: false,
    });
    redis.on("error", (error) => {
        console.warn("[REDIS ERROR]", error?.message || error);
    });
} else {
    console.warn("[WARN] REDIS_URL belum diset. History, cache FX, dan dedupe Redis dilewati.");
}

// General helpers
function isAllowedUniverse(universeId) {
    return ALLOWED_UNIVERSES.has(String(universeId));
}

function getRobloxApiKey(universeId) {
    return process.env[`ROBLOX_API_KEY_${universeId}`] || process.env.ROBLOX_API_KEY || "";
}

function safeSecretEqual(actual, expected) {
    if (typeof actual !== "string" || typeof expected !== "string") return false;
    const actualBuffer = Buffer.from(actual);
    const expectedBuffer = Buffer.from(expected);
    return actualBuffer.length === expectedBuffer.length
        && crypto.timingSafeEqual(actualBuffer, expectedBuffer);
}

function verifyWebhookSecret(req) {
    if (!WEBHOOK_SECRET) return true;
    const supplied = req.headers["x-webhook-secret"] || req.query.secret || "";
    return safeSecretEqual(String(supplied), WEBHOOK_SECRET);
}

function validateWebhookBase(req, res) {
    const universeId = String(req.params.universeId || "");
    if (!universeId) {
        res.status(400).json({ ok: false, error: "MISSING_UNIVERSE_ID" });
        return null;
    }
    if (!isAllowedUniverse(universeId)) {
        res.status(403).json({ ok: false, error: "UNAUTHORIZED_UNIVERSE", universeId });
        return null;
    }
    if (!verifyWebhookSecret(req)) {
        res.status(401).json({ ok: false, error: "INVALID_WEBHOOK_SECRET" });
        return null;
    }
    return universeId;
}

function getPayloadViews(raw) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return [];
    return [raw, raw.data, raw.donation, raw.transaction, raw.payload, raw.result]
        .filter((value) => value && typeof value === "object" && !Array.isArray(value));
}

function firstWebhookValue(raw, fieldNames) {
    for (const view of getPayloadViews(raw)) {
        for (const fieldName of fieldNames) {
            const value = view[fieldName];
            if (value !== undefined && value !== null && value !== "") return value;
        }
    }
    return "";
}

function listWebhookFields(raw) {
    const fields = [];
    for (const [index, view] of getPayloadViews(raw).entries()) {
        const prefix = index === 0 ? "root" : `nested${index}`;
        fields.push(...Object.keys(view).slice(0, 40).map((key) => `${prefix}.${key}`));
    }
    return [...new Set(fields)].slice(0, 100);
}

const ZERO_DECIMAL_CURRENCIES = new Set([
    "BIF", "CLP", "DJF", "GNF", "IDR", "ISK", "JPY", "KMF", "KRW", "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF",
]);

function parseMoney(value, currency = "IDR") {
    if (typeof value === "number") return Number.isFinite(value) ? value : 0;

    let text = String(value || "").trim().replace(/[^0-9,.-]/g, "");
    if (!text) return 0;

    const code = String(currency || "IDR").toUpperCase();
    const comma = text.lastIndexOf(",");
    const dot = text.lastIndexOf(".");

    if (comma >= 0 && dot >= 0) {
        const decimalMark = comma > dot ? "," : ".";
        const groupingMark = decimalMark === "," ? "." : ",";
        text = text.split(groupingMark).join("").replace(decimalMark, ".");
    } else if (comma >= 0 || dot >= 0) {
        const mark = comma >= 0 ? "," : ".";
        const pieces = text.split(mark);
        if (ZERO_DECIMAL_CURRENCIES.has(code) || pieces.length > 2) {
            text = pieces.join("");
        } else {
            text = pieces.join(".");
        }
    }

    const amount = Number(text);
    return Number.isFinite(amount) && amount > 0 ? amount : 0;
}

function createDonationHash(source, donorName, amount, currency, message, providerId) {
    const identity = providerId
        ? `${source}:${providerId}`
        : `${source}:${donorName}:${amount}:${currency}:${message}:${Math.floor(Date.now() / 5000)}`;
    return crypto.createHash("sha256").update(identity).digest("hex");
}

// Currency conversion. API response uses IDR as base, so rates[USD] means
// how many USD equal one IDR. Foreign amount / rates[code] produces IDR.
let memoryFxCache = null;

function normalizedManualRates() {
    const result = { IDR: 1 };
    for (const [currency, rate] of Object.entries(manualIdrRates)) {
        const numericRate = Number(rate);
        if (Number.isFinite(numericRate) && numericRate > 0) {
            result[String(currency).toUpperCase()] = numericRate;
        }
    }
    return result;
}

async function readCachedFxRates() {
    const now = Date.now();
    if (memoryFxCache && memoryFxCache.expiresAt > now) return memoryFxCache.rates;
    if (!redis) return null;

    try {
        const cached = await redis.get("fxRates:IDR");
        if (!cached) return null;
        const rates = JSON.parse(cached);
        memoryFxCache = { rates, expiresAt: now + FX_CACHE_SECONDS * 1000 };
        return rates;
    } catch (error) {
        console.warn("[FX CACHE READ SKIPPED]", error?.message || error);
        return null;
    }
}

async function fetchFxRates() {
    if (!EXCHANGE_RATE_API_KEY) return null;
    const response = await fetch(
        `https://v6.exchangerate-api.com/v6/${encodeURIComponent(EXCHANGE_RATE_API_KEY)}/latest/IDR`,
        { headers: { Accept: "application/json" } }
    );
    if (!response.ok) throw new Error(`FX_HTTP_${response.status}`);
    const data = await response.json();
    if (data.result !== "success" || !data.conversion_rates) {
        throw new Error(`FX_API_${data["error-type"] || "INVALID_RESPONSE"}`);
    }
    return data.conversion_rates;
}

async function getFxRates() {
    const cached = await readCachedFxRates();
    if (cached) return cached;

    let rates = null;
    try {
        rates = await fetchFxRates();
    } catch (error) {
        console.error("[FX FETCH ERROR]", error?.message || error);
    }

    if (!rates) {
        const manual = normalizedManualRates();
        if (Object.keys(manual).length > 1) return { mode: "IDR_PER_UNIT", values: manual };
        throw new Error("FX_RATES_UNAVAILABLE");
    }

    const cacheValue = { mode: "BASE_IDR", values: rates };
    memoryFxCache = { rates: cacheValue, expiresAt: Date.now() + FX_CACHE_SECONDS * 1000 };
    if (redis) {
        redis.set("fxRates:IDR", JSON.stringify(cacheValue), "EX", FX_CACHE_SECONDS)
            .catch((error) => console.warn("[FX CACHE SAVE SKIPPED]", error?.message || error));
    }
    return cacheValue;
}

async function convertToIdr(amount, currency) {
    const code = String(currency || "IDR").trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(code)) throw new Error(`INVALID_CURRENCY:${code}`);
    if (code === "IDR") return Math.round(amount);

    const table = await getFxRates();
    const rate = Number(table.values[code]);
    if (!Number.isFinite(rate) || rate <= 0) throw new Error(`UNSUPPORTED_CURRENCY:${code}`);

    const idrAmount = table.mode === "BASE_IDR" ? amount / rate : amount * rate;
    if (!Number.isFinite(idrAmount) || idrAmount <= 0) throw new Error("INVALID_CONVERSION_RESULT");
    return Math.round(idrAmount);
}

// Roblox Open Cloud
function compactDonation(donation) {
    return {
        type: "donation",
        donation: {
            id: String(donation.id || ""),
            timestamp: Number(donation.timestamp || Math.floor(Date.now() / 1000)),
            source: String(donation.source || "unknown").slice(0, 20),
            donorName: String(donation.donorName || "Anonymous").slice(0, 40),
            amount: Number(donation.amount || 0),
            currency: "IDR",
            message: String(donation.message || "").slice(0, 220),
            originalAmount: Number(donation.originalAmount || donation.amount || 0),
            originalCurrency: String(donation.originalCurrency || "IDR").slice(0, 8),
        },
    };
}

async function publishDonationToRoblox(universeId, donation) {
    const apiKey = getRobloxApiKey(universeId);
    if (!apiKey) return { ok: false, reason: "MISSING_ROBLOX_API_KEY" };

    let payload = compactDonation(donation);
    let message = JSON.stringify(payload);
    if (Buffer.byteLength(message, "utf8") > 950) {
        payload.donation.message = payload.donation.message.slice(0, 80);
        message = JSON.stringify(payload);
    }

    try {
        const response = await fetch(
            `https://apis.roblox.com/cloud/v2/universes/${universeId}:publishMessage`,
            {
                method: "POST",
                headers: { "x-api-key": apiKey, "Content-Type": "application/json" },
                body: JSON.stringify({ topic: ROBLOX_TOPIC, message }),
            }
        );
        if (!response.ok) {
            return { ok: false, status: response.status, body: await response.text().catch(() => "") };
        }
        return { ok: true };
    } catch (error) {
        return { ok: false, reason: "ROBLOX_PUBLISH_ERROR", message: error?.message || String(error) };
    }
}

// Redis dedupe and history
async function acquireDedupe(hash) {
    if (!redis) return { checked: false, duplicate: false, token: null };
    const token = crypto.randomUUID();
    try {
        const inserted = await redis.set(`donationHash:${hash}`, token, "EX", 300, "NX");
        return { checked: true, duplicate: !inserted, token: inserted ? token : null };
    } catch (error) {
        console.warn("[REDIS DEDUPE SKIPPED]", error?.message || error);
        return { checked: false, duplicate: false, token: null };
    }
}

async function releaseDedupe(hash, token) {
    if (!redis || !token) return;
    try {
        await redis.eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            1,
            `donationHash:${hash}`,
            token
        );
    } catch (error) {
        console.warn("[REDIS DEDUPE RELEASE SKIPPED]", error?.message || error);
    }
}

async function saveDonationBestEffort(universeId, donation) {
    if (!redis) return { ok: false, reason: "REDIS_DISABLED" };
    try {
        await redis.zadd(`donations:${universeId}`, donation.timestamp, JSON.stringify(donation));
        await redis.set(`lastDonationId:${universeId}`, String(donation.timestamp));
        return { ok: true };
    } catch (error) {
        return { ok: false, reason: "REDIS_ERROR", message: error?.message || String(error) };
    }
}

async function deliverDonation(universeId, donation, dedupe) {
    const publishResult = await publishDonationToRoblox(universeId, donation);
    if (!publishResult.ok) {
        await releaseDedupe(donation.id, dedupe.token);
        return { publishResult, saveResult: { ok: false, reason: "PUBLISH_FAILED" } };
    }
    return {
        publishResult,
        saveResult: await saveDonationBestEffort(universeId, donation),
    };
}

function webhookResponse(res, donation, result, dedupe) {
    const body = {
        ok: result.publishResult.ok === true,
        donationId: donation.id,
        pushedToRoblox: result.publishResult.ok === true,
        publishResult: result.publishResult,
        savedToRedis: result.saveResult.ok === true,
        saveResult: result.saveResult,
        dedupe: {
            checked: dedupe.checked,
            duplicate: dedupe.duplicate,
        },
        conversion: {
            originalAmount: donation.originalAmount,
            originalCurrency: donation.originalCurrency,
            idrAmount: donation.amount,
        },
    };
    return res.status(result.publishResult.ok ? 200 : 502).json(body);
}

// Legacy endpoints
for (const path of ["/api/session", "/api/tail", "/api/donations"]) {
    app.all(path, (_req, res) => res.status(410).json({
        ok: false,
        reason: "LEGACY_OPEN_CLOUD_PUSH_ONLY",
    }));
}

// Generic handler builder for Saweria and BagiBagi (amount already treated as IDR).
function registerIdrWebhook(provider, fields) {
    app.post(`/webhook/${provider}/:universeId`, async (req, res) => {
        try {
            const universeId = validateWebhookBase(req, res);
            if (!universeId) return;
            const raw = req.body || {};
            const amount = parseMoney(firstWebhookValue(raw, fields.amount), "IDR");
            if (!amount) return res.json({ ok: true, ignored: true, reason: "INVALID_AMOUNT" });
            if (amount < MIN_IDR_AMOUNT) {
                return res.json({ ok: true, ignored: true, reason: "AMOUNT_BELOW_MINIMUM" });
            }
            const donorName = firstWebhookValue(raw, fields.name) || "Anonymous";
            const message = String(firstWebhookValue(raw, fields.message) || "");
            const providerId = firstWebhookValue(raw, fields.id);
            const hash = createDonationHash(provider, donorName, amount, "IDR", message, providerId);
            const dedupe = await acquireDedupe(hash);
            if (dedupe.duplicate) return res.json({ ok: true, duplicate: true });
            const donation = {
                id: hash,
                timestamp: Math.floor(Date.now() / 1000),
                source: provider,
                donorName: String(donorName),
                amount: Math.round(amount),
                currency: "IDR",
                originalAmount: amount,
                originalCurrency: "IDR",
                message,
            };
            return webhookResponse(res, donation, await deliverDonation(universeId, donation, dedupe), dedupe);
        } catch (error) {
            console.error(`[WEBHOOK ${provider.toUpperCase()} ERROR]`, error);
            return res.status(500).json({ ok: false, error: "INTERNAL_ERROR" });
        }
    });
}

registerIdrWebhook("saweria", {
    amount: ["amount_raw", "amount", "nominal", "value"],
    name: ["donator_name", "name", "supporter"],
    message: ["message", "note", "pesan"],
    id: ["id", "transaction_id", "invoice_id", "payment_id"],
});

registerIdrWebhook("bagibagi", {
    amount: ["amount", "nominal", "value"],
    name: ["name", "username", "donator", "donator_name"],
    message: ["message", "note", "pesan"],
    id: ["id", "transaction_id", "order_id", "invoice_id"],
});

// SociaBuzz: accepts any three-letter currency for which an FX rate is available.
app.post("/webhook/sociabuzz/:universeId", async (req, res) => {
    try {
        const universeId = validateWebhookBase(req, res);
        if (!universeId) return;
        const raw = req.body || {};
        if (WEBHOOK_DEBUG) console.log("[WEBHOOK SOCIABUZZ FIELDS]", listWebhookFields(raw));

        const originalCurrency = String(firstWebhookValue(raw, ["currency", "currency_code"]) || "IDR")
            .trim()
            .toUpperCase();
        const originalAmount = parseMoney(firstWebhookValue(raw, [
            "amount", "amount_raw", "nominal", "value", "total", "total_amount",
            "donation_amount", "support_amount", "gross_amount",
        ]), originalCurrency);
        if (!originalAmount) return res.json({ ok: true, ignored: true, reason: "INVALID_AMOUNT" });

        let amount;
        try {
            amount = await convertToIdr(originalAmount, originalCurrency);
        } catch (error) {
            return res.status(422).json({
                ok: false,
                error: "CURRENCY_CONVERSION_FAILED",
                currency: originalCurrency,
                detail: error?.message || String(error),
            });
        }
        if (amount < MIN_IDR_AMOUNT) {
            return res.json({
                ok: true,
                ignored: true,
                reason: "AMOUNT_BELOW_MINIMUM_AFTER_CONVERSION",
                conversion: { originalAmount, originalCurrency, idrAmount: amount },
            });
        }

        const donorName = firstWebhookValue(raw, [
            "name", "supporter_name", "supporter", "donor_name", "donator_name",
            "donator", "username", "from_name", "customer_name",
        ]) || "Anonymous";
        const message = String(firstWebhookValue(raw, [
            "message", "supporter_message", "support_message", "note", "comment", "pesan",
        ]) || "");
        const providerId = firstWebhookValue(raw, [
            "id", "transaction_id", "transactionId", "order_id", "invoice_id",
            "payment_id", "reference_id", "ref_id", "uuid",
        ]);
        const hash = createDonationHash(
            "sociabuzz", donorName, originalAmount, originalCurrency, message, providerId
        );
        const dedupe = await acquireDedupe(hash);
        if (dedupe.duplicate) return res.json({ ok: true, duplicate: true });

        const donation = {
            id: hash,
            timestamp: Math.floor(Date.now() / 1000),
            source: "sociabuzz",
            donorName: String(donorName),
            amount,
            currency: "IDR",
            originalAmount,
            originalCurrency,
            message,
        };
        return webhookResponse(res, donation, await deliverDonation(universeId, donation, dedupe), dedupe);
    } catch (error) {
        console.error("[WEBHOOK SOCIABUZZ ERROR]", error);
        return res.status(500).json({ ok: false, error: "INTERNAL_ERROR" });
    }
});

app.post("/api/test/publish/:universeId", async (req, res) => {
    if (!ENABLE_TEST_ENDPOINT) return res.status(404).json({ ok: false, error: "NOT_FOUND" });
    try {
        const universeId = validateWebhookBase(req, res);
        if (!universeId) return;
        const amount = Math.round(Number(req.body?.amount || 50000));
        const timestamp = Math.floor(Date.now() / 1000);
        const donation = {
            id: `test_${timestamp}_${crypto.randomUUID()}`,
            timestamp,
            source: "manual",
            donorName: String(req.body?.donorName || "Test Donor"),
            amount,
            currency: "IDR",
            originalAmount: amount,
            originalCurrency: "IDR",
            message: String(req.body?.message || "Test Open Cloud donation"),
        };
        const result = await publishDonationToRoblox(universeId, donation);
        return res.status(result.ok ? 200 : 502).json({ ok: result.ok, donation, publishResult: result });
    } catch (error) {
        console.error("[TEST PUBLISH ERROR]", error);
        return res.status(500).json({ ok: false, error: "INTERNAL_ERROR" });
    }
});

app.get("/", (_req, res) => {
    res.json({
        ok: true,
        service: "roblox-donation-bridge",
        mode: "OpenCloudPush",
        topic: ROBLOX_TOPIC,
        allowedUniverses: [...ALLOWED_UNIVERSES],
        redisEnabled: Boolean(redis),
        webhookDebug: WEBHOOK_DEBUG,
        fxProvider: EXCHANGE_RATE_API_KEY ? "ExchangeRate-API" : "FX_RATES_JSON only",
        supportedProviders: ["saweria", "bagibagi", "sociabuzz"],
    });
});

app.get(["/favicon.ico", "/favicon.png"], (_req, res) => res.status(204).end());

module.exports = app;
