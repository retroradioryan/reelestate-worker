// worker.js (STABLE VERSION — Callback + Debug + Production Safe)

import { createClient } from "@supabase/supabase-js";

/* ==============================
   ENV VALIDATION
============================== */

function mustEnv(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`❌ Missing environment variable: ${name}`);
    process.exit(1);
  }
  return value;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");

const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_AVATAR_ID = mustEnv("HEYGEN_AVATAR_ID");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

const POLL_MS = 5000;

/* ==============================
   CLIENTS
============================== */

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

console.log("🚀 WORKER LIVE");
console.log("Callback Base:", HEYGEN_CALLBACK_BASE_URL);
console.log("Webhook Secret:", HEYGEN_WEBHOOK_SECRET);

/* ==============================
   UTIL
============================== */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text().catch(() => "");

  if (!response.ok) {
    console.error("❌ HTTP ERROR:", response.status, text);
    throw new Error(text || "Request failed");
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON response");
  }
}

/* ==============================
   CREATE HEYGEN VIDEO
============================== */

async function createHeygenVideo({ scriptText, jobId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("=================================");
  console.log("🎬 Creating HeyGen Video");
  console.log("Job ID:", jobId);
  console.log("Callback URL:", callbackUrl);
  console.log("=================================");

  const payload = {
    video_inputs: [
      {
        character: {
          type: "avatar",
          avatar_id: HEYGEN_AVATAR_ID,
        },
        voice: {
          type: "text",
          voice_id: HEYGEN_VOICE_ID,
          input_text: scriptText,
        },
        background: {
          type: "color",
          value: "#00FF00",
        },
      },
    ],
    dimension: {
      width: 1080,
      height: 1920,
    },
    callback_url: callbackUrl, // must be ROOT LEVEL
  };

  console.log("Sending payload to HeyGen...");
  console.log(JSON.stringify(payload, null, 2));

  const json = await fetchJSON(
    "https://api.heygen.com/v2/video/generate",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key": HEYGEN_API_KEY,
      },
      body: JSON.stringify(payload),
    }
  );

  console.log("HeyGen response:", JSON.stringify(json, null, 2));

  const videoId = json?.data?.video_id;

  if (!videoId) {
    throw new Error("HeyGen did not return video_id");
  }

  console.log("✅ HEYGEN VIDEO ID:", videoId);

  return videoId;
}

/* ==============================
   PROCESS QUEUED JOB
============================== */

async function processQueuedJob(job) {
  const jobId = job.id;

  console.log("📦 Processing job:", jobId);

  // Minimal script for stability
  const script =
    "Welcome to this beautiful new listing. Contact us today to arrange your private viewing.";

  const videoId = await createHeygenVideo({
    scriptText: script,
    jobId,
  });

  await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: videoId,
    })
    .eq("id", jobId);

  console.log("⏳ Waiting for webhook...");
}

/* ==============================
   MAIN LOOP
============================== */

async function loop() {
  while (true) {
    try {
      const { data, error } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "queued")
        .limit(1);

      if (error) {
        console.error("Supabase error:", error.message);
      }

      if (data && data.length > 0) {
        await processQueuedJob(data[0]);
      }
    } catch (err) {
      console.error("Worker error:", err.message);
    }

    await sleep(POLL_MS);
  }
}

loop();
