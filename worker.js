// worker.js (FULL PRODUCTION VERSION — QUEUED + RENDERING SUPPORT)

import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";

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
   CLIENT
============================== */

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

console.log("🚀 WORKER LIVE");

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

async function downloadFile(url, outputPath) {
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Download failed: ${response.status}`);
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  fs.writeFileSync(outputPath, buffer);
}

/* ==============================
   CREATE HEYGEN VIDEO
============================== */

async function createHeygenVideo({ scriptText, jobId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("🎬 Creating HeyGen video for job:", jobId);
  console.log("Callback URL:", callbackUrl);

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
    dimension: { width: 1080, height: 1920 },
    callback_url: callbackUrl,
  };

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

  const videoId = json?.data?.video_id;

  if (!videoId) {
    throw new Error("HeyGen did not return video_id");
  }

  console.log("✅ HEYGEN VIDEO ID:", videoId);

  return videoId;
}

/* ==============================
   PROCESS QUEUED
============================== */

async function processQueued(job) {
  const jobId = job.id;

  console.log("📦 Processing QUEUED job:", jobId);

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

  console.log("⏳ Waiting for webhook to switch to rendering...");
}

/* ==============================
   PROCESS RENDERING
============================== */

async function processRendering(job) {
  const jobId = job.id;

  console.log("🎞 Processing RENDERING job:", jobId);

  if (!job.heygen_video_url) {
    console.log("⚠️ No video URL yet.");
    return;
  }

  const tmpPath = path.join("/tmp", `heygen-${jobId}.mp4`);

  await downloadFile(job.heygen_video_url, tmpPath);

  // For now: just mark completed
  await supabase
    .from("render_jobs")
    .update({
      status: "completed",
      final_public_url: job.heygen_video_url,
    })
    .eq("id", jobId);

  console.log("✅ Job completed:", jobId);
}

/* ==============================
   MAIN LOOP
============================== */

async function loop() {
  while (true) {
    try {
      // 1️⃣ Check queued
      const { data: queued } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "queued")
        .limit(1);

      if (queued?.length) {
        await processQueued(queued[0]);
        await sleep(POLL_MS);
        continue;
      }

      // 2️⃣ Check rendering
      const { data: rendering } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "rendering")
        .limit(1);

      if (rendering?.length) {
        await processRendering(rendering[0]);
      }
    } catch (err) {
      console.error("Worker error:", err.message);
    }

    await sleep(POLL_MS);
  }
}

loop();
