// worker.js (RESTORED WORKING VERSION + HARD DEBUG)

import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import OpenAI from "openai";

/* ==============================
   ENV + CONFIG
============================== */

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const supabase = createClient(
  mustEnv("SUPABASE_URL"),
  mustEnv("SUPABASE_SERVICE_ROLE_KEY")
);

const openai = new OpenAI({ apiKey: mustEnv("OPENAI_API_KEY") });

const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_AVATAR_ID = mustEnv("HEYGEN_AVATAR_ID");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

const POLL_MS = 5000;

console.log("🚀 WORKER LIVE");
console.log("HEYGEN_CALLBACK_BASE_URL:", HEYGEN_CALLBACK_BASE_URL);
console.log("HEYGEN_WEBHOOK_SECRET:", HEYGEN_WEBHOOK_SECRET);

/* ==============================
   UTILS
============================== */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchRetry(url, opts = {}) {
  const resp = await fetch(url, opts);
  const text = await resp.text().catch(() => "");

  if (!resp.ok) {
    console.error("❌ HTTP ERROR:", resp.status, text);
    throw new Error(text);
  }

  return JSON.parse(text);
}

/* ==============================
   HEYGEN CREATE (WORKING STRUCTURE)
============================== */

async function createHeygenVideoFromText({ scriptText, jobId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("=================================");
  console.log("CREATING HEYGEN VIDEO");
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
    callback_url: callbackUrl, // ROOT LEVEL (THIS WAS CORRECT)
  };

  console.log("Sending payload to HeyGen...");
  console.log(JSON.stringify(payload, null, 2));

  const json = await fetchRetry(
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

async function processQueued(job) {
  const jobId = job.id;

  console.log("📦 Processing job:", jobId);

  // Simpler test script so we remove OpenAI from equation for now
  const script = "Welcome to this beautiful new listing. Contact us today to arrange a viewing.";

  const videoId = await createHeygenVideoFromText({
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

  console.log("Waiting for webhook...");
}

/* ==============================
   MAIN LOOP
============================== */

async function loop() {
  while (true) {
    try {
      const { data } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "queued")
        .limit(1);

      if (data?.length) {
        await processQueued(data[0]);
      }
    } catch (err) {
      console.error("Worker error:", err.message);
    }

    await sleep(POLL_MS);
  }
}

loop();
