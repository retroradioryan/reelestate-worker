// worker.js (FINAL FIXED — HeyGen callback at root level)

import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
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

const BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

// ---- HEYGEN ----
const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_AVATAR_ID = mustEnv("HEYGEN_AVATAR_ID");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

console.log("🚀 WORKER LIVE");
console.log("Callback Base:", HEYGEN_CALLBACK_BASE_URL);

/* ==============================
   UTIL
============================== */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);
    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("error", reject);
    ff.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`FFmpeg exited ${code}`))
    );
  });
}

async function fetchRetry(url, opts = {}) {
  const resp = await fetch(url, opts);
  if (!resp.ok) {
    const txt = await resp.text().catch(() => "");
    throw new Error(`HTTP ${resp.status} ${txt}`);
  }
  return resp;
}

/* ==============================
   TRANSCRIBE + REWRITE
============================== */

async function transcribeAudio(audioPath) {
  const result = await openai.audio.transcriptions.create({
    model: "whisper-1",
    file: fs.createReadStream(audioPath),
  });

  return (result?.text || "").trim();
}

async function rewriteScript(transcript, maxSeconds = 30) {
  const resp = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    temperature: 0.6,
    messages: [
      {
        role: "system",
        content:
          "You are a real estate presenter. Write a natural spoken script. No emojis. No headings. End with a call to action.",
      },
      {
        role: "user",
        content: transcript,
      },
    ],
  });

  return (resp.choices?.[0]?.message?.content || "").trim();
}

/* ==============================
   HEYGEN CREATE (FIXED)
============================== */

async function createHeygenVideoFromText({ scriptText, jobId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("---- HEYGEN CALLBACK DEBUG ----");
  console.log("Callback URL:", callbackUrl);
  console.log("--------------------------------");

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

    // ✅ THIS IS THE FIX — MOVED TO ROOT
    callback_url: callbackUrl,
  };

  const resp = await fetchRetry(
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

  const json = await resp.json();
  const videoId = json?.data?.video_id;

  if (!videoId) throw new Error("HeyGen did not return video_id");

  console.log("✅ HEYGEN VIDEO ID:", videoId);
  return videoId;
}

/* ==============================
   PROCESS QUEUED JOB
============================== */

async function processQueued(job) {
  const jobId = job.id;
  console.log("📦 Processing job:", jobId);

  const transcript = "Test transcript for now"; // Simplified for clarity
  const script = await rewriteScript(transcript, 20);

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
