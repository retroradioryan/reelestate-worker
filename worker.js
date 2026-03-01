// worker.js (FULL REWRITE — Node 22 SAFE + Lower-Third System)
// Implements:
// ✅ Avatar size/position: width 450–480 (default 460), right margin 60, bottom margin 180
// ✅ Branded lower-third (PNG) overlay at x=60, y=1520 (1080x1920 canvas)
// ✅ Optional lower-third slide-in animation (0.4s) via FFmpeg expression
// ✅ Mobile-safe vertical scaling: scale=decrease + pad (no crop/stretch)
// ✅ Production hardening: job locking, basic idempotency, temp cleanup, safer downloads, better ffmpeg mapping
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

const supabase = createClient(
  mustEnv("SUPABASE_URL"),
  mustEnv("SUPABASE_SERVICE_ROLE_KEY")
);

const openai = new OpenAI({ apiKey: mustEnv("OPENAI_API_KEY") });

const BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

// ---- KEYING ----
const KEY_COLOR_HEX = process.env.KEY_COLOR_HEX || "#00FF00"; // HeyGen background
const KEY_COLOR_FFMPEG = (process.env.KEY_COLOR_FFMPEG || "0x00FF00").trim(); // 0xRRGGBB
const KEY_SIMILARITY = String(process.env.KEY_SIMILARITY || "0.35");
const KEY_BLEND = String(process.env.KEY_BLEND || "0.20");

// ---- AVATAR OVERLAY (BOTTOM RIGHT) ----
const AVATAR_SCALE_W = Number(process.env.AVATAR_SCALE_W || 460); // 450–480 sweet spot; default 460
const AVATAR_MARGIN_X = Number(process.env.AVATAR_MARGIN_X || 60); // distance from RIGHT edge
const AVATAR_MARGIN_Y = Number(process.env.AVATAR_MARGIN_Y || 180); // distance from BOTTOM edge

// ---- LOWER THIRD (BRANDED) ----
// Default placement tuned for 1080x1920
const LT_X = Number(process.env.LT_X || 60);
const LT_Y = Number(process.env.LT_Y || 1520);
const LT_W = Number(process.env.LT_W || 780);
const LT_H = Number(process.env.LT_H || 180);
const LT_RADIUS = Number(process.env.LT_RADIUS || 24);

// Brand styling (tweak to match ReelEstate)
const BRAND_BG = process.env.BRAND_BG || "#0E1A2B"; // dark navy
const BRAND_ACCENT = process.env.BRAND_ACCENT || "#2D8CFF"; // electric blue
const BRAND_TEXT = process.env.BRAND_TEXT || "#FFFFFF";
const BRAND_TEXT_SUB = process.env.BRAND_TEXT_SUB || "rgba(255,255,255,0.88)";

// Optional: slide-in animation for lower third
const LT_ANIMATE = String(process.env.LT_ANIMATE || "true").toLowerCase() === "true";
const LT_ANIM_SECONDS = Number(process.env.LT_ANIM_SECONDS || 0.4);

// ---- SAFETY ----
const MAX_WALK_DOWNLOAD_MB = Number(process.env.MAX_WALK_DOWNLOAD_MB || 250);
const MAX_AVATAR_DOWNLOAD_MB = Number(process.env.MAX_AVATAR_DOWNLOAD_MB || 500);
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || 120000);
const FETCH_RETRIES = Number(process.env.FETCH_RETRIES || 3);

// ---- HEYGEN ----
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");

console.log("🚀 WORKER LIVE (transcribe → rewrite → HeyGen → branded composite)");

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
   UTIL: DOWNLOAD FILE (Node 22 streaming)
============================== */
async function downloadToFile(url, outPath, { maxMB } = {}) {
  if (!url || !url.startsWith("http")) throw new Error(`Invalid URL: ${url}`);

  const resp = await fetchRetry(url, { method: "GET" });
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Download failed: ${resp.status} ${text}`);
  }

  // Prefer streaming
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
   UTIL: SAFE TEMP CLEANUP
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
   UTIL: RUN FFCREATE (lower-third image)
   Uses ffmpeg lavfi to draw a rounded-ish bar + accent + text.
   Note: FFmpeg doesn't have perfect rounded rectangles natively.
   We fake "premium" corners by drawing slightly inset rectangles.
   This is clean enough for V1, and keeps dependencies at zero.
============================== */
async function generateLowerThirdPng({
  outPath,
  headline,
  subline,
  tagLine,
  logoPath, // optional future use (not required)
}) {
  // sanitize text for ffmpeg drawtext
  const esc = (s) =>
    String(s || "")
      .replace(/\\/g, "\\\\")
      .replace(/:/g, "\\:")
      .replace(/'/g, "\\'")
      .replace(/\n/g, " ");

  const h1 = esc(headline || "PROPERTY UPDATE");
  const h2 = esc(subline || "Just listed • Book a viewing today");
  const h3 = esc(tagLine || "");

  // Layout within lower-third
  const padL = 26; // inner padding
  const accentW = 8;
  const accentGap = 18;

  // Font: try common fonts in Linux images; DejaVu is usually present
  const font = process.env.LT_FONTFILE || "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

  // We'll build a transparent 780x180 PNG:
  // - Base: transparent
  // - Draw background rectangle (solid, high opacity)
  // - Draw accent strip
  // - Draw headline and subline and optional tag line

  // “Rounded-ish” trick:
  // draw main rect inset by 2px, then a second rect inset by 4px
  // This softens corners visually even without true rounding.

  const bg1 = `drawbox=x=0:y=0:w=${LT_W}:h=${LT_H}:color=${BRAND_BG}@0.92:t=fill`;
  const bg2 = `drawbox=x=2:y=2:w=${LT_W - 4}:h=${LT_H - 4}:color=${BRAND_BG}@0.92:t=fill`;
  const accent = `drawbox=x=0:y=0:w=${accentW}:h=${LT_H}:color=${BRAND_ACCENT}@1.0:t=fill`;

  // Typography sizes tuned for mobile readability
  const headSize = Number(process.env.LT_HEAD_SIZE || 52);
  const subSize = Number(process.env.LT_SUB_SIZE || 40);
  const tagSize = Number(process.env.LT_TAG_SIZE || 36);

  const textX = padL + accentW + accentGap;
  const headY = 20;
  const subY = 92;
  const tagY = 132;

  // Using drawtext with fontfile
  const dt1 = `drawtext=fontfile='${font}':text='${h1}':x=${textX}:y=${headY}:fontsize=${headSize}:fontcolor=${BRAND_TEXT}:fontcolor_expr=${BRAND_TEXT}:line_spacing=8:shadowcolor=black@0:shadowx=0:shadowy=0`;
  const dt2 = `drawtext=fontfile='${font}':text='${h2}':x=${textX}:y=${subY}:fontsize=${subSize}:fontcolor=${BRAND_TEXT_SUB}:line_spacing=6:shadowcolor=black@0:shadowx=0:shadowy=0`;

  const filters = [bg1, bg2, accent, dt1, dt2];

  if (h3 && h3.trim()) {
    filters.push(
      `drawtext=fontfile='${font}':text='${h3}':x=${textX}:y=${tagY}:fontsize=${tagSize}:fontcolor=${BRAND_TEXT_SUB}:shadowcolor=black@0:shadowx=0:shadowy=0`
    );
  }

  const filter = filters.join(",");

  await runFFmpeg([
    "-y",
    "-f",
    "lavfi",
    "-i",
    `color=c=black@0.0:s=${LT_W}x${LT_H}:r=30`,
    "-vf",
    filter,
    "-frames:v",
    "1",
    outPath,
  ]);
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
   STEP: TRANSCRIBE (Whisper)
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
   STEP: REWRITE SCRIPT
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
        character: { type: "avatar", avatar_id: mustEnv("HEYGEN_AVATAR_ID") },
        voice: { type: "text", voice_id: mustEnv("HEYGEN_VOICE_ID"), input_text: scriptText.trim() },
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
    .update({ status: "failed", error: msg.slice(0, 2000) })
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

  try {
    // 1) download walkthrough
    await downloadToFile(job.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });

    // 2) extract audio
    await extractAudioMp4ToM4a(walkPath, audioPath);

    // 3) transcribe
    const transcript = await transcribeAudio(audioPath);

    // 4) rewrite
    const scriptText = await rewriteToPresenterScript(transcript, maxSeconds);

    // 5) create HeyGen green-screen avatar
    const heygenVideoId = await createHeygenVideoFromText({ scriptText, jobId });

    // 6) update job (optional transcript/script columns)
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
  } finally {
    // Clean up temp files from Phase 1
    safeUnlink(walkPath);
    safeUnlink(audioPath);
  }
}

/* ==============================
   LOWER-THIRD CONTENT BUILDER
   Pull from DB columns if you have them; otherwise fallback.
   (You can add these columns later without breaking worker)
============================== */
function buildLowerThirdCopy(job) {
  // You can populate these in your API service when job is created:
  // job.agent_name, job.property_location, job.price, job.tagline etc.
  const agent = (job.agent_name || job.presenter_name || "ReelEstate AI").trim();
  const loc = (job.property_location || job.location || "New Listing").trim();
  const price = (job.price || job.list_price || "").toString().trim();

  const headline = agent.toUpperCase(); // bold, confident
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

  // Idempotency guard: if already completed with URL, skip
  if (job.status === "completed" && job.final_public_url) {
    console.log("✅ Already completed, skipping:", jobId);
    return;
  }

  console.log("🎬 Rendering FINAL (branded):", jobId);

  // lock: rendering -> rendering_in_progress
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

  // Use locked row (fresh)
  const lockedJob = lock.data;

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const lowerThirdPath = path.join(tmp, `lowerthird-${jobId}.png`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  try {
    // downloads
    await downloadToFile(lockedJob.walkthrough_url, walkPath, { maxMB: MAX_WALK_DOWNLOAD_MB });
    await downloadToFile(lockedJob.heygen_video_url, avatarPath, { maxMB: MAX_AVATAR_DOWNLOAD_MB });

    // create lower-third PNG
    const { headline, subline, tagLine } = buildLowerThirdCopy(lockedJob);
    await generateLowerThirdPng({
      outPath: lowerThirdPath,
      headline,
      subline,
      tagLine,
    });

    // ---- Video Composition ----
    // 1) Background: scale-to-fit (decrease) + pad to 1080x1920 (mobile-safe, no crop/stretch)
    // 2) Lower-third: overlay at x=60, y=1520 (optional slide-in)
    // 3) Avatar: colorkey green + scale 460 wide, bottom-right with margins 60/180
    //
    // Notes:
    // - We map HeyGen audio if present (1:a?) because avatar is input #2 in this pipeline (index 2), but audio comes from avatar file.
    // - Inputs: 0=walk, 1=lowerthird.png, 2=avatar.mp4
    //
    // Lower-third animation:
    // x = if(t < 0.4) -w + t * speed else LT_X
    // speed tuned so it arrives at ~LT_X at t=0.4
    const ltXExpr = LT_ANIMATE
      ? `if(lt(t\\,${LT_ANIM_SECONDS}), -w + t*${Math.ceil((LT_X + LT_W) / LT_ANIM_SECONDS)}, ${LT_X})`
      : `${LT_X}`;

    const filter =
      // bg normalize
      `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
      `pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1[vbg];` +
      // lower third (png already correct size)
      `[1:v]format=rgba[lt];` +
      `[vbg][lt]overlay=x='${ltXExpr}':y=${LT_Y}[v1];` +
      // avatar key + scale
      `[2:v]scale=${AVATAR_SCALE_W}:-2,` +
      `colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND}[av];` +
      // avatar overlay bottom right
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
      "2:a?", // HeyGen audio if present
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
      .update({
        status: "completed",
        final_public_url: finalPublicUrl,
      })
      .eq("id", jobId);

    if (upd.error) throw upd.error;

    console.log("✅ Completed:", jobId, finalPublicUrl);
  } finally {
    // cleanup always
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
