const express = require("express");
const bodyParser = require("body-parser");
const Redis = require("ioredis");
const crypto = require("crypto");

const app = express();
app.use(bodyParser.json());

const redis = new Redis(process.env.REDIS_URL);

const ALLOWED_UNIVERSES = [
    "9494346835",
    "9571322329",
    "9708552870",
    "9929798598",
    "9930679278",
    "9958532531",
    "9978804792",
    "10046314551",
    "10214313758",
    "10226076955"
];

function createDonationHash(donorName, amount, message) {
    const timeBucket = Math.floor(Date.now() / 5000); // 5 detik window
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

// SESSION
app.post("/api/session", async (req, res) => {

    const universeId = req.body.universeId;

    if (!ALLOWED_UNIVERSES.includes(universeId)) {
        return res.json({
            ok: false,
            reason: "UNAUTHORIZED_UNIVERSE"
        });
    }

    const token = crypto.randomUUID();

    await redis.set(
        `session:${token}`,
        universeId,
        "EX",
        86400
    );

    res.json({ ok: true, token });
});

async function validateSession(req) {

    const token = req.headers["x-session"];
    if (!token) return null;

    const universeId = await redis.get(`session:${token}`);
    return universeId;
}

// TAIL
app.get("/api/tail", async (req, res) => {

    const universeId = await validateSession(req);
    if (!universeId) {
        return res.json({ ok:false, reason:"INVALID_SESSION" });
    }

    const lastId = await redis.get(`lastDonationId:${universeId}`);

    res.json({ id: lastId || "0" });
});

// DONATIONS
app.get("/api/donations", async (req, res) => {

    const universeId = await validateSession(req);
    if (!universeId) {
        return res.json({ ok:false, reason:"INVALID_SESSION" });
    }

    const after = req.query.after || "0";

    const items = await redis.zrangebyscore(
        `donations:${universeId}`,
        `(${after}`,
        "+inf"
    );

    const parsed = items.map(item => JSON.parse(item));

    res.json({ items: parsed });
});

// WEBHOOK
app.post("/webhook/saweria/:universeId", async (req, res) => {
    try {
        const universeId = req.params.universeId;

        if (!ALLOWED_UNIVERSES.includes(universeId)) {
            return res.status(403).json({ error: "UNAUTHORIZED_UNIVERSE" });
        }

        const raw = req.body || {};

        if (raw.type && raw.type !== "donation") {
            return res.json({ ok: true });
        }

        const parseAmount = (val) => {
            if (!val) return 0;
            return Number(String(val).replace(/[^\d]/g, ""));
        };

        const amount =
            parseAmount(raw.amount_raw) ||
            parseAmount(raw.amount) ||
            parseAmount(raw?.etc?.amount_to_display);

        if (!amount || amount <= 0) {
            return res.json({ ok: true });
        }

        const donorName =
            raw.donator_name ||
            raw.name ||
            raw.supporter ||
            "Anonymous";

        const message = String(raw.message || "");

        const hash = createDonationHash(donorName, amount, message);

        // 🔥 ANTI DUPLICATE
        const exists = await redis.get(`donationHash:${hash}`);
        if (exists) {
            return res.json({ ok: true });
        }

        await redis.set(`donationHash:${hash}`, "1", "EX", 300);

        const timestamp = Date.now();

        const donation = {
            id: hash,
            timestamp: timestamp,
            source: "saweria",
            donorName: String(donorName),
            amount: Number(amount),
            currency: "IDR",
            message: message,
        };

        await redis.zadd(
            `donations:${universeId}`,
            timestamp,
            JSON.stringify(donation)
        );

        await redis.set(
            `lastDonationId:${universeId}`,
            timestamp.toString()
        );

        res.json({ ok: true });

    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "INTERNAL_ERROR" });
    }
});

app.post("/webhook/bagibagi/:universeId", async (req, res) => {
    try {
        const universeId = req.params.universeId;

        if (!ALLOWED_UNIVERSES.includes(universeId)) {
            return res.status(403).json({ error: "UNAUTHORIZED_UNIVERSE" });
        }

        const raw = req.body || {};

        const parseAmount = (val) => {
            if (!val) return 0;
            return Number(String(val).replace(/[^\d]/g, ""));
        };

        const amount =
            parseAmount(raw.amount) ||
            parseAmount(raw.nominal) ||
            parseAmount(raw.value);

        if (!amount || amount <= 0) {
            return res.json({ ok: true });
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
            return res.json({ ok: true });
        }

        await redis.set(`donationHash:${hash}`, "1", "EX", 300);

        const timestamp = Date.now();

        const donation = {
            id: hash,
            timestamp: timestamp,
            source: "bagibagi",
            donorName: String(donorName),
            amount: Number(amount),
            currency: "IDR",
            message: String(message),
        };

        await redis.zadd(
            `donations:${universeId}`,
            timestamp,
            JSON.stringify(donation)
        );

        await redis.set(
            `lastDonationId:${universeId}`,
            timestamp.toString()
        );

        res.json({ ok: true });

    } catch (err) {
        console.error(err);
        res.status(500).json({ error: "INTERNAL_ERROR" });
    }
});

module.exports = app;