// worker.js (Node 22 SAFE + Lower-Third System + DEBUG CALLBACK URL)
//
// Pipeline:
// queued -> processing -> heygen_requested -> (webhook sets rendering + heygen_video_url) -> rendering_in_progress -> completed

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

// Supabase
const supabase = createClient(
  mustEnv("SUPABASE_URL"),
  mustEnv("SUPABASE_SERVICE_ROLE_KEY")
);

// OpenAI
const openai = new OpenAI({ apiKey: mustEnv("OPENAI_API_KEY") });

// Storage / polling
const BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

// ---- KEYING ----
const KEY_COLOR_HEX = process.env.KEY_COLOR_HEX || "#00FF00";
const KEY_COLOR_FFMPEG = (process.env.KEY_COLOR_FFMPEG || "0x00FF00").trim();
const KEY_SIMILARITY = String(process.env.KEY_SIMILARITY || "0.35");
const KEY_BLEND = String(process.env.KEY_BLEND || "0.20");

// ---- AVATAR OVERLAY (BOTTOM RIGHT) ----
const AVATAR_SCALE_W = Number(process.env.AVATAR_SCALE_W || 460);
const AVATAR_MARGIN_X = Number(process.env.AVATAR_MARGIN_X || 60);
const AVATAR_MARGIN_Y = Number(process.env.AVATAR_MARGIN_Y || 180);

// ---- LOWER THIRD ----
const LT_X = Number(process.env.LT_X || 60);
const LT_Y = Number(process.env.LT_Y || 1520);
const LT_W = Number(process.env.LT_W || 780);
const LT_H = Number(process.env.LT_H || 180);
const LT_RADIUS = Number(process.env.LT_RADIUS || 24);

const BRAND_BG = process.env.BRAND_BG || "#0E1A2B";
const BRAND_ACCENT = process.env.BRAND_ACCENT || "#2D8CFF";
const BRAND_TEXT = process.env.BRAND_TEXT || "#FFFFFF";
const BRAND_TEXT_SUB = process.env.BRAND_TEXT_SUB || "rgba(255,255,255,0.88)";

const LT_ANIMATE = String(process.env.LT_ANIMATE || "true").toLowerCase() === "true";
const LT_ANIM_SECONDS = Number(process.env.LT_ANIM_SECONDS || 0.4);

// ---- SAFETY ----
const MAX_WALK_DOWNLOAD_MB = Number(process.env.MAX_WALK_DOWNLOAD_MB || 250);
const MAX_AVATAR_DOWNLOAD_MB = Number(process.env.MAX_AVATAR_DOWNLOAD_MB || 500);
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || 120000);
const FETCH_RETRIES = Number(process.env.FETCH_RETRIES || 3);

// ---- HEYGEN ----
// IMPORTANT: Render env key is HEYGEN_API_KEY (NOT HEYGEN_API_KEY)
const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_AVATAR_ID = mustEnv("HEYGEN_AVATAR_ID");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

console.log("🚀 WORKER LIVE (transcribe → rewrite → HeyGen → branded composite)");
console.log("---- ENV CHECK ----");
console.log("SUPABASE_URL present?", !!process.env.SUPABASE_URL);
console.log("OPENAI_API_KEY present?", !!process.env.OPENAI_API_KEY);
console.log("HEYGEN_API_KEY present?", !!process.env.HEYGEN_API_KEY);
console.log("HEYGEN_CALLBACK_BASE_URL:", HEYGEN_CALLBACK_BASE_URL);
console.log("HEYGEN_WEBHOOK_SECRET present?", !!process.env.HEYGEN_WEBHOOK_SECRET);
console.log("HEYGEN_AVATAR_ID present?", !!process.env.HEYGEN_AVATAR_ID);
console.log("HEYGEN_VOICE_ID present?", !!process.env.HEYGEN_VOICE_ID);
console.log("-------------------");

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
    return await fetch(url, { ...opts, signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
}

async function fetchRetry(url, opts = {}, retries = FETCH_RETRIES) {
  let lastErr;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fetchWithTimeout(url, opts);
    } catch (err) {
      lastErr = err;
      console.error(`⚠️ fetch attempt ${attempt}/${retries} failed:`, err?.message || err);
      await sleep(800 * attempt);
    }
  }
  throw lastErr || new Error("fetch failed");
}

/* ==============================
   UTIL: DOWNLOAD FILE
============================== */
async function downloadToFile(url, outPath, { maxMB } = {}) {
  if (!url || !url.startsWith("http")) throw new Error(`Invalid URL: ${url}`);

  const resp = await fetchRetry(url, { method: "GET" });
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Download failed: ${resp.status} ${text}`);
  }

  if (!resp.body || typeof resp.body.getReader !== "function") {
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

      downloaded += value.byteLength;
      if (downloaded > maxBytes) throw new Error(`File too large (>${maxMB}MB)`);

      fileStream.write(Buffer.from(value));
    }
  } finally {
    fileStream.end();
  }
}

/* ==============================
   UTIL: CLEANUP
============================== */
function safeUnlink(p) {
  try {
    if (p && fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
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
   LOWER THIRD PNG
============================== */
async function generateLowerThirdPng({ outPath, headline, subline, tagLine }) {
  const esc = (s) =>
    String(s || "")
      .replace(/\\/g, "\\\\")
      .replace(/:/g, "\\:")
      .replace(/'/g, "\\'")
      .replace(/\n/g, " ");

  const h1 = esc(headline || "PROPERTY UPDATE");
  const h2 = esc(subline || "Just listed • Book a viewing today");
  const h3 = esc(tagLine || "");

  const padL = 26;
  const accentW = 8;
  const accentGap = 18;

  const font = process.env.LT_FONTFILE || "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

  const bg1 = `drawbox=x=0:y=0:w=${LT_W}:h=${LT_H}:color=${BRAND_BG}@0.92:t=fill`;
  const bg2 = `drawbox=x=2:y=2:w=${LT_W - 4}:h=${LT_H - 4}:color=${BRAND_BG}@0.92:t=fill`;
  const accent = `drawbox=x=0:y=0:w=${accentW}:h=${LT_H}:color=${BRAND_ACCENT}@1.0:t=fill`;

  const headSize = Number(process.env.LT_HEAD_SIZE || 52);
  const subSize = Number(process.env.LT_SUB_SIZE || 40);
  const tagSize = Number(process.env.LT_TAG_SIZE || 36);

  const textX = padL + accentW + accentGap;
  const headY = 20;
  const subY = 92;
  const tagY = 132;

  const dt1 = `drawtext=fontfile='${font}':text='${h1}':x=${textX}:y=${headY}:fontsize=${headSize}:fontcolor=${BRAND_TEXT}:shadowcolor=black@0:shadowx=0:shadowy=0`;
  const dt2 = `drawtext=fontfile='${font}':text='${h2}':x=${textX}:y=${subY}:fontsize=${subSize}:fontcolor=${BRAND_TEXT_SUB}:shadowcolor=black@0:shadowx=0:shadowy=0`;

  const filters = [bg1, bg2, accent, dt1, dt2];

  if (h3 && h3.trim()) {
    filters.push(
      `drawtext=fontfile='${font}':text='${h3}':x=${textX}:y=${tagY}:fontsize=${tagSize}:fontcolor=${BRAND_TEXT_SUB}:shadowcolor=black@0:shadowx=0:shadowy=0`
    );
  }

  await runFFmpeg([
    "-y",
    "-f",
    "lavfi",
    "-i",
    `color=c=black@0.0:s=${LT_W}x${LT_H}:r=30`,
    "-vf",
    filters.join(","),
    "-frames:v",
    "1",
    outPath,
  ]);
}

/* ==============================
   AUDIO EXTRACT
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
   WHISPER TRANSCRIBE
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
   SCRIPT REWRITE
============================== */
function targetWordsForSeconds(maxSeconds) {
  const wps = 2.4;
  return Math.max(30, Math.round(maxSeconds * wps));
}

async function rewriteToPresenterScript(transcript, maxSeconds = 30) {
  const targetWords = targetWordsForSeconds(maxSeconds);
  console.log(`✍️ Rewriting transcript → presenter script (~${maxSeconds}s, ~${targetWords} words)…`);

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
        content: `WORD TARGET: ~${targetWords} words.\nMAX SECONDS: ${maxSeconds}\n\nTRANSCRIPT:\n${transcript}`,
      },
    ],
  });

  const script = (resp.choices?.[0]?.message?.content || "").trim();
  if (!script) throw new Error("Script rewrite returned empty text");
  return script;
}

/* ==============================
   HEYGEN CREATE
============================== */
async function createHeygenVideoFromText({ scriptText, jobId }) {
  console.log("🎤 Sending TEXT script to HeyGen:\n" + scriptText);

  // Build the callback URL *and log it*
  const callbackUrl = `${HEYGEN_CALLBACK_BASE_URL}?token=${encodeURIComponent(
    HEYGEN_WEBHOOK_SECRET
  )}&job_id=${encodeURIComponent(jobId)}`;

  console.log("---- HEYGEN CALLBACK DEBUG ----");
  console.log("Callback URL being sent to HeyGen:");
  console.log(callbackUrl);
  console.log("Secret present?", !!HEYGEN_WEBHOOK_SECRET);
  console.log("--------------------------------");

  const payload = {
    video_inputs: [
      {
        character: { type: "avatar", avatar_id: HEYGEN_AVATAR_ID },
        voice: { type: "text", voice_id: HEYGEN_VOICE_ID, input_text: scriptText.trim() },
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
      "X-Api-Key": HEYGEN_API_KEY,
    },
    body: JSON.stringify(payload),
  });

  const bodyText = await resp.text().catch(() => "");
  if (!resp.ok) {
    console.error("❌ HeyGen generate failed:", resp.status, bodyText?.slice(0, 400));
    throw new Error(`HeyGen error: ${bodyText}`);
  }

  const json = JSON.parse(bodyText);
  const videoId = json?.data?.video_id;
  if (!videoId) throw new Error("HeyGen did not return video_id");

  console.log("✅ HEYGEN VIDEO ID:", videoId);
  return videoId;
}

/* ==============================
   UPLOAD TO SUPABASE
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
  const upd1 = await supabase.from("render_jobs").update(payloadWithOptionalFields).eq("id", jobId);
  if (!upd1.error) return;

  console.log("⚠️ Optional fields update failed, retrying minimal:", upd1.error.message);

  const upd2 = await supabase.from("render_jobs").update(payloadMinimal).eq("id", jobId);
  if (upd2.error) throw upd2.error;
}

async function failJob(jobId, err) {
  const msg = String(err?.message || err || "Unknown error");
  console.error("❌ Job failed:", jobId, msg);

  await supabase.from("render_jobs").update({ status: "failed", error: msg.slice(0, 2000) }).eq("id", jobId);
}

/* ==============================
   PHASE 1 — QUEUED → HEYGEN_REQUESTED
============================== */
async function processQueued(job) {
  const jobId = job.id;
  const maxSeconds = Number(job.max_seconds || 30);

  console.log("📦 Processing QUEUED:", jobId);

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

  try {
    await downloadToFile(job.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });
    await extractAudioMp4ToM4a(walkPath, audioPath);

    const transcript = await transcribeAudio(audioPath);
    const scriptText = await rewriteToPresenterScript(transcript, maxSeconds);

    const heygenVideoId = await createHeygenVideoFromText({ scriptText, jobId });

    await updateJobSafe(
      jobId,
      {
        status: "heygen_requested",
        heygen_video_id: heygenVideoId,
        transcript_text: transcript,
        script_text: scriptText,
      },
      {
        status: "heygen_requested",
        heygen_video_id: heygenVideoId,
      }
    );

    console.log("📡 HeyGen requested. Waiting for webhook to set status=rendering…");
  } finally {
    safeUnlink(walkPath);
    safeUnlink(audioPath);
  }
}

/* ==============================
   LOWER THIRD COPY
============================== */
function buildLowerThirdCopy(job) {
  const agent = (job.agent_name || job.presenter_name || "ReelEstate AI").trim();
  const loc = (job.property_location || job.location || "New Listing").trim();
  const price = (job.price || job.list_price || "").toString().trim();

  const headline = agent.toUpperCase();
  const subline = price ? `${loc} — ${price}` : `${loc}`;
  const tagLine = (job.badge || job.tagline || "Just Listed").trim();

  return { headline, subline, tagLine };
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

  console.log("🎬 Rendering FINAL (branded):", jobId);

  const lock = await supabase
    .from("render_jobs")
    .update({ status: "rendering_in_progress" })
    .eq("id", jobId)
    .eq("status", "rendering")
    .select("*")
    .maybeSingle();

  if (lock.error) throw lock.error;
  if (!lock.data) {
    console.log("⏭️ Job already rendering elsewhere:", jobId);
    return;
  }

  const lockedJob = lock.data;

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const lowerThirdPath = path.join(tmp, `lowerthird-${jobId}.png`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  try {
    await downloadToFile(lockedJob.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });
    await downloadToFile(lockedJob.heygen_video_url, avatarPath, { maxMB: MAX_AVATAR_DOWNLOAD_MB });

    const { headline, subline, tagLine } = buildLowerThirdCopy(lockedJob);
    await generateLowerThirdPng({ outPath: lowerThirdPath, headline, subline, tagLine });

    const ltXExpr = LT_ANIMATE
      ? `if(lt(t\\,${LT_ANIM_SECONDS}), -w + t*${Math.ceil((LT_X + LT_W) / LT_ANIM_SECONDS)}, ${LT_X})`
      : `${LT_X}`;

    const filter =
      `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
      `pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1[vbg];` +
      `[1:v]format=rgba[lt];` +
      `[vbg][lt]overlay=x='${ltXExpr}':y=${LT_Y}[v1];` +
      `[2:v]scale=${AVATAR_SCALE_W}:-2,colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND}[av];` +
      `[v1][av]overlay=x=W-w-${AVATAR_MARGIN_X}:y=H-h-${AVATAR_MARGIN_Y}[outv]`;

    await runFFmpeg([
      "-y",
      "-i",
      walkPath,
      "-i",
      lowerThirdPath,
      "-i",
      avatarPath,
      "-filter_complex",
      filter,
      "-map",
      "[outv]",
      "-map",
      "2:a?",
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
      "-movflags",
      "+faststart",
      finalPath,
    ]);

    const finalStoragePath = `renders/final-${jobId}.mp4`;
    const finalPublicUrl = await uploadToSupabase({
      localPath: finalPath,
      storagePath: finalStoragePath,
      contentType: "video/mp4",
    });

    const upd = await supabase
      .from("render_jobs")
      .update({ status: "completed", final_public_url: finalPublicUrl })
      .eq("id", jobId);

    if (upd.error) throw upd.error;

    console.log("✅ Completed:", jobId, finalPublicUrl);
  } finally {
    safeUnlink(walkPath);
    safeUnlink(avatarPath);
    safeUnlink(lowerThirdPath);
    safeUnlink(finalPath);
  }
}

/* ==============================
   MAIN LOOP
============================== */
async function loop() {
  while (true) {
    try {
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
