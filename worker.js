// worker.js (FULL REWRITE)
// - Downloads walkthrough
// - Extracts audio
// - Transcribes with OpenAI Whisper
// - Rewrites into a tight presenter script (maxSeconds)
// - Sends TEXT voice to HeyGen (with green background)
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
const KEY_COLOR_HEX = process.env.KEY_COLOR_HEX || "#00FF00";
// ffmpeg colorkey expects 0xRRGGBB (no #)
const KEY_COLOR_FFMPEG = "0x00FF00";
const KEY_SIMILARITY = process.env.KEY_SIMILARITY || "0.35";
const KEY_BLEND = process.env.KEY_BLEND || "0.20";

// ---- OVERLAY (BOTTOM RIGHT defaults) ----
// width of avatar on a 1080-wide canvas
const OVERLAY_SCALE_W = Number(process.env.OVERLAY_SCALE_W || 480);
// distance from RIGHT edge
const OVERLAY_MARGIN_X = Number(process.env.OVERLAY_MARGIN_X || 90);
// distance from BOTTOM edge (higher = more up)
const OVERLAY_MARGIN_Y = Number(process.env.OVERLAY_MARGIN_Y || 260);

// ---- SAFETY ----
const MAX_WALK_DOWNLOAD_MB = Number(process.env.MAX_WALK_DOWNLOAD_MB || 250);

// ---- HEYGEN ----
// Base callback URL should be your API endpoint WITHOUT query params, e.g.
// https://reelestate-api-9oob.onrender.com/heygen-callback
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");

console.log("🚀 WORKER LIVE (transcribe → rewrite → HeyGen → composite)");

/* ==============================
   UTIL: SLEEP
============================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ==============================
   UTIL: FETCH
   Node 18+ has global fetch. Render uses Node 22, so this is fine.
============================== */
async function httpGet(url) {
  const resp = await fetch(url);
  return resp;
}

/* ==============================
   UTIL: DOWNLOAD FILE (stream)
============================== */
async function downloadToFile(url, outPath, { maxMB } = {}) {
  if (!url || !url.startsWith("http")) throw new Error(`Invalid URL: ${url}`);

  const resp = await httpGet(url);
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Download failed: ${resp.status} ${text}`);
  }

  const maxBytes = (maxMB || 9999) * 1024 * 1024;
  let downloaded = 0;

  await new Promise((resolve, reject) => {
    const fileStream = fs.createWriteStream(outPath);
    fileStream.on("error", reject);

    resp.body.on("data", (chunk) => {
      downloaded += chunk.length;
      if (downloaded > maxBytes) {
        resp.body.destroy(new Error(`File too large (>${maxMB}MB)`));
      }
    });

    resp.body.on("error", reject);
    fileStream.on("finish", resolve);

    resp.body.pipe(fileStream);
  });
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
  // presenter pace ~2.3–2.6 words/sec
  const wps = 2.4;
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

  const resp = await fetch("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": mustEnv("HEYGEN_API_KEY"),
    },
    body: JSON.stringify(payload),
  });

  const bodyText = await resp.text();
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

// Best-effort: update optional debug columns if they exist, otherwise retry without them.
async function updateJobSafe(jobId, payloadWithOptionalFields, payloadMinimal) {
  const upd1 = await supabase.from("render_jobs").update(payloadWithOptionalFields).eq("id", jobId);
  if (!upd1.error) return;
  console.log("⚠️ Optional fields update failed, retrying minimal:", upd1.error.message);

  const upd2 = await supabase.from("render_jobs").update(payloadMinimal).eq("id", jobId);
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

  // Lock it: queued -> processing (prevents double-processing)
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

  // 1) download walkthrough
  await downloadToFile(job.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });

  // 2) extract audio
  await extractAudioMp4ToM4a(walkPath, audioPath);

  // 3) transcribe audio
  const transcript = await transcribeAudio(audioPath);

  // 4) rewrite to presenter script
  const scriptText = await rewriteToPresenterScript(transcript, maxSeconds);

  // 5) create HeyGen video (text voice on green background)
  const heygenVideoId = await createHeygenVideoFromText({ scriptText, jobId });

  // 6) save
  await updateJobSafe(
    jobId,
    {
      status: "heygen_requested",
      heygen_video_id: heygenVideoId,
      transcript_text: transcript, // optional column
      script_text: scriptText, // optional column
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
    console.log("⏳ Rendering job missing heygen_video_url yet, skipping:", jobId);
    return;
  }

  console.log("🎬 Rendering FINAL:", jobId);

  // Lock it: rendering -> rendering_in_progress
  const lock = await supabase
    .from("render_jobs")
    .update({ status: "rendering_in_progress" })
    .eq("id", jobId)
    .eq("status", "rendering")
    .select("id")
    .maybeSingle();

  if (lock.error) throw lock.error;
  if (!lock.data) {
    console.log("⏭️ Job already being rendered elsewhere:", jobId);
    return;
  }

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  await downloadToFile(job.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });
  await downloadToFile(job.heygen_video_url, avatarPath, { maxMB: 500 });

  // Bottom-right placement (defaults):
  // overlay=W-w-90 : H-h-260
  const filter =
    `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[vbg];` +
    `[1:v]scale=${OVERLAY_SCALE_W}:-2,colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND}[fg];` +
    `[vbg][fg]overlay=W-w-${OVERLAY_MARGIN_X}:H-h-${OVERLAY_MARGIN_Y}[outv]`;

  await runFFmpeg([
    "-y",
    "-i",
    walkPath,
    "-i",
    avatarPath,
    "-filter_complex",
    filter,
    "-map",
    "[outv]",
    "-map",
    "1:a?", // keep HeyGen audio (presenter voice)
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "23",
    "-pix_fmt",
    "yuv420p",
    "-c:a",
    "aac",
    "-b:a",
    "160k",
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
      // 1) Take one queued job
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

      // 2) Take one rendering job
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
      console.error("❌ Worker loop error:", err);
    }

    await sleep(POLL_MS);
  }
}

loop();
