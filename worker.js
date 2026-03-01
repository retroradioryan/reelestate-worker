// worker.js (FULL REWRITE — Node 22 SAFE)
// - Downloads walkthrough (STREAMING, no arrayBuffer)
// - Extracts audio
// - Transcribes with OpenAI Whisper
// - Rewrites into a tight presenter script (maxSeconds)
// - Sends TEXT voice to HeyGen (green background)
// - HeyGen webhook updates render_jobs -> status=rendering + heygen_video_url
// - Worker composites walkthrough + keyed avatar (BOTTOM RIGHT)
// - Uploads final MP4 to Supabase + marks job completed

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

// ---- KEYING ----
const KEY_COLOR_HEX = process.env.KEY_COLOR_HEX || "#00FF00"; // HeyGen background
const KEY_COLOR_FFMPEG = (process.env.KEY_COLOR_FFMPEG || "0x00FF00").trim(); // ffmpeg colorkey expects 0xRRGGBB
const KEY_SIMILARITY = String(process.env.KEY_SIMILARITY || "0.35");
const KEY_BLEND = String(process.env.KEY_BLEND || "0.20");

// ---- OVERLAY (BOTTOM RIGHT defaults) ----
const OVERLAY_SCALE_W = Number(process.env.OVERLAY_SCALE_W || 480);
const OVERLAY_MARGIN_X = Number(process.env.OVERLAY_MARGIN_X || 60);  // distance from RIGHT edge
const OVERLAY_MARGIN_Y = Number(process.env.OVERLAY_MARGIN_Y || 120); // distance from BOTTOM edge

// ---- SAFETY ----
const MAX_WALK_DOWNLOAD_MB = Number(process.env.MAX_WALK_DOWNLOAD_MB || 250);
const MAX_AVATAR_DOWNLOAD_MB = Number(process.env.MAX_AVATAR_DOWNLOAD_MB || 500);
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || 120000); // 120s
const FETCH_RETRIES = Number(process.env.FETCH_RETRIES || 3);

// ---- HEYGEN ----
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL"); // e.g. https://reelestate-api-xxxx.onrender.com/heygen-callback
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");

console.log("🚀 WORKER LIVE (transcribe → rewrite → HeyGen → composite)");

/* ==============================
   UTIL: SLEEP
============================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ==============================
   UTIL: FETCH WITH TIMEOUT + RETRY
============================== */
async function fetchWithTimeout(url, opts = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...opts, signal: controller.signal });
    return resp;
  } finally {
    clearTimeout(t);
  }
}

async function fetchRetry(url, opts = {}, retries = FETCH_RETRIES) {
  let lastErr;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const resp = await fetchWithTimeout(url, opts);
      return resp;
    } catch (err) {
      lastErr = err;
      console.error(`⚠️ fetch attempt ${attempt}/${retries} failed:`, err?.message || err);
      // small backoff
      await sleep(800 * attempt);
    }
  }
  throw lastErr || new Error("fetch failed");
}

/* ==============================
   UTIL: DOWNLOAD FILE (Node 22 safe streaming)
   Uses Web Streams reader (resp.body.getReader()).
   Avoids resp.body.on and avoids arrayBuffer memory blowups.
============================== */
async function downloadToFile(url, outPath, { maxMB } = {}) {
  if (!url || !url.startsWith("http")) throw new Error(`Invalid URL: ${url}`);

  const resp = await fetchRetry(url, { method: "GET" });

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Download failed: ${resp.status} ${text}`);
  }

  if (!resp.body || typeof resp.body.getReader !== "function") {
    // This should not happen in Node 22, but guard anyway.
    const ab = await resp.arrayBuffer();
    const buf = Buffer.from(ab);
    const maxBytes = (maxMB || 9999) * 1024 * 1024;
    if (buf.length > maxBytes) throw new Error(`File too large (>${maxMB}MB)`);
    fs.writeFileSync(outPath, buf);
    return;
  }

  const reader = resp.body.getReader();
  const fileStream = fs.createWriteStream(outPath);

  const maxBytes = (maxMB || 9999) * 1024 * 1024;
  let downloaded = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      // value is a Uint8Array
      downloaded += value.byteLength;
      if (downloaded > maxBytes) {
        throw new Error(`File too large (>${maxMB}MB)`);
      }

      fileStream.write(Buffer.from(value));
    }
  } finally {
    fileStream.end();
  }
}

/* ==============================
   UTIL: RUN FFMPEG
============================== */
function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);

    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("error", reject);

    ff.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`FFmpeg exited with code ${code}`));
    });
  });
}

/* ==============================
   STEP: EXTRACT AUDIO
============================== */
async function extractAudioMp4ToM4a(videoPath, audioOutPath) {
  await runFFmpeg([
    "-y",
    "-i",
    videoPath,
    "-vn",
    "-ac",
    "1",
    "-ar",
    "44100",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    audioOutPath,
  ]);
}

/* ==============================
   STEP: TRANSCRIBE (OpenAI Whisper)
============================== */
async function transcribeAudio(audioPath) {
  console.log("📝 Transcribing walkthrough audio…");

  const result = await openai.audio.transcriptions.create({
    model: "whisper-1",
    file: fs.createReadStream(audioPath),
  });

  const text = (result?.text || "").trim();
  if (!text) throw new Error("Transcription returned empty text");
  return text;
}

/* ==============================
   STEP: REWRITE SCRIPT (maxSeconds)
============================== */
function targetWordsForSeconds(maxSeconds) {
  const wps = 2.4; // presenter pace
  return Math.max(30, Math.round(maxSeconds * wps));
}

async function rewriteToPresenterScript(transcript, maxSeconds = 30) {
  const targetWords = targetWordsForSeconds(maxSeconds);

  console.log(
    `✍️ Rewriting transcript → presenter script (~${maxSeconds}s, ~${targetWords} words)…`
  );

  const resp = await openai.chat.completions.create({
    model: process.env.OPENAI_SCRIPT_MODEL || "gpt-4o-mini",
    temperature: 0.6,
    messages: [
      {
        role: "system",
        content:
          "You are a real estate presenter. Write a natural spoken script for an avatar narrator. " +
          "No bullet points. No headings. No emojis. No scene directions. " +
          "Do not mention 'walkthrough', 'this video', or 'recording'. " +
          "Keep it within the word target. End with a simple call to action.",
      },
      {
        role: "user",
        content:
          `WORD TARGET: ~${targetWords} words.\n` +
          `MAX SECONDS: ${maxSeconds}\n\n` +
          `TRANSCRIPT:\n${transcript}`,
      },
    ],
  });

  const script = (resp.choices?.[0]?.message?.content || "").trim();
  if (!script) throw new Error("Script rewrite returned empty text");
  return script;
}

/* ==============================
   STEP: HEYGEN CREATE (TEXT VOICE)
============================== */
async function createHeygenVideoFromText({ scriptText, jobId }) {
  console.log("🎤 Sending TEXT script to HeyGen:\n" + scriptText);

  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  const payload = {
    video_inputs: [
      {
        character: {
          type: "avatar",
          avatar_id: mustEnv("HEYGEN_AVATAR_ID"),
        },
        voice: {
          type: "text",
          voice_id: mustEnv("HEYGEN_VOICE_ID"),
          input_text: scriptText.trim(),
        },
        background: { type: "color", value: KEY_COLOR_HEX },
        callback_url: callbackUrl,
      },
    ],
    dimension: { width: 1080, height: 1920 },
  };

  const resp = await fetchRetry("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": mustEnv("HEYGEN_API_KEY"),
    },
    body: JSON.stringify(payload),
  });

  const bodyText = await resp.text().catch(() => "");
  if (!resp.ok) throw new Error(`HeyGen error: ${bodyText}`);

  const json = JSON.parse(bodyText);
  const videoId = json?.data?.video_id;

  if (!videoId) throw new Error("HeyGen did not return video_id");
  console.log("✅ HEYGEN VIDEO ID:", videoId);
  return videoId;
}

/* ==============================
   STEP: UPLOAD FILE TO SUPABASE
============================== */
async function uploadToSupabase({ localPath, storagePath, contentType }) {
  const buf = fs.readFileSync(localPath);

  const { error } = await supabase.storage.from(BUCKET).upload(storagePath, buf, {
    contentType,
    upsert: true,
  });
  if (error) throw error;

  const { data } = supabase.storage.from(BUCKET).getPublicUrl(storagePath);
  return data.publicUrl;
}

/* ==============================
   DB HELPERS
============================== */

async function updateJobSafe(jobId, payloadWithOptionalFields, payloadMinimal) {
  const upd1 = await supabase
    .from("render_jobs")
    .update(payloadWithOptionalFields)
    .eq("id", jobId);

  if (!upd1.error) return;

  console.log("⚠️ Optional fields update failed, retrying minimal:", upd1.error.message);

  const upd2 = await supabase
    .from("render_jobs")
    .update(payloadMinimal)
    .eq("id", jobId);

  if (upd2.error) throw upd2.error;
}

async function failJob(jobId, err) {
  const msg = String(err?.message || err || "Unknown error");
  console.error("❌ Job failed:", jobId, msg);

  await supabase
    .from("render_jobs")
    .update({
      status: "failed",
      error: msg.slice(0, 2000),
    })
    .eq("id", jobId);
}

/* ==============================
   PHASE 1 — QUEUED → HEYGEN_REQUESTED
============================== */
async function processQueued(job) {
  const jobId = job.id;
  const maxSeconds = Number(job.max_seconds || 30);

  console.log("📦 Processing QUEUED:", jobId);

  // lock: queued -> processing
  const lock = await supabase
    .from("render_jobs")
    .update({ status: "processing" })
    .eq("id", jobId)
    .eq("status", "queued")
    .select("id")
    .maybeSingle();

  if (lock.error) throw lock.error;
  if (!lock.data) {
    console.log("⏭️ Job already taken by another worker:", jobId);
    return;
  }

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const audioPath = path.join(tmp, `audio-${jobId}.m4a`);

  // 1) download walkthrough (streaming)
  await downloadToFile(job.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });

  // 2) extract audio
  await extractAudioMp4ToM4a(walkPath, audioPath);

  // 3) transcribe
  const transcript = await transcribeAudio(audioPath);

  // 4) rewrite
  const scriptText = await rewriteToPresenterScript(transcript, maxSeconds);

  // 5) create HeyGen green-screen avatar (text voice)
  const heygenVideoId = await createHeygenVideoFromText({ scriptText, jobId });

  // 6) update job (optional transcript/script columns)
  await updateJobSafe(
    jobId,
    {
      status: "heygen_requested",
      heygen_video_id: heygenVideoId,
      transcript_text: transcript, // optional
      script_text: scriptText,     // optional
    },
    {
      status: "heygen_requested",
      heygen_video_id: heygenVideoId,
    }
  );

  console.log("📡 HeyGen requested. Waiting for webhook to set status=rendering…");
}

/* ==============================
   PHASE 2 — RENDERING → COMPLETED
============================== */
async function processRendering(job) {
  const jobId = job.id;

  if (!job.heygen_video_url) {
    console.log("⏳ rendering job missing heygen_video_url, skipping:", jobId);
    return;
  }

  console.log("🎬 Rendering FINAL:", jobId);

  // lock: rendering -> rendering_in_progress
  const lock = await supabase
    .from("render_jobs")
    .update({ status: "rendering_in_progress" })
    .eq("id", jobId)
    .eq("status", "rendering")
    .select("id")
    .maybeSingle();

  if (lock.error) throw lock.error;
  if (!lock.data) {
    console.log("⏭️ Job already rendering elsewhere:", jobId);
    return;
  }

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  // downloads (streaming)
  await downloadToFile(job.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });
  await downloadToFile(job.heygen_video_url, avatarPath, { maxMB: MAX_AVATAR_DOWNLOAD_MB });

  // Bottom-right overlay:
  // overlay=W-w-MARGIN_X : H-h-MARGIN_Y
  const filter =
    `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[vbg];` +
    `[1:v]scale=${OVERLAY_SCALE_W}:-2,colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND}[fg];` +
    `[vbg][fg]overlay=W-w-${OVERLAY_MARGIN_X}:H-h-${OVERLAY_MARGIN_Y}[outv]`;

  await runFFmpeg([
    "-y",
    "-i", walkPath,
    "-i", avatarPath,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "1:a?", // HeyGen audio
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-pix_fmt", "yuv420p",
    "-c:a", "aac",
    "-b:a", "160k",
    finalPath,
  ]);

  const finalPublicUrl = await uploadToSupabase({
    localPath: finalPath,
    storagePath: `renders/final-${jobId}.mp4`,
    contentType: "video/mp4",
  });

  const upd = await supabase
    .from("render_jobs")
    .update({
      status: "completed",
      final_public_url: finalPublicUrl,
    })
    .eq("id", jobId);

  if (upd.error) throw upd.error;

  console.log("✅ Completed:", jobId, finalPublicUrl);
}

/* ==============================
   MAIN LOOP
============================== */
async function loop() {
  while (true) {
    try {
      // 1) queued job
      const { data: queued, error: qErr } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "queued")
        .order("created_at", { ascending: true })
        .limit(1);

      if (qErr) throw qErr;

      if (queued?.length) {
        const job = queued[0];
        try {
          await processQueued(job);
        } catch (err) {
          await failJob(job.id, err);
        }
        await sleep(POLL_MS);
        continue;
      }

      // 2) rendering job (set by webhook)
      const { data: rendering, error: rErr } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "rendering")
        .order("created_at", { ascending: true })
        .limit(1);

      if (rErr) throw rErr;

      if (rendering?.length) {
        const job = rendering[0];
        try {
          await processRendering(job);
        } catch (err) {
          await failJob(job.id, err);
        }
      }
    } catch (err) {
      console.error("❌ Worker loop error:", err?.message || err);
    }

    await sleep(POLL_MS);
  }
}

loop();
