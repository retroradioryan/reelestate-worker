// worker.js (FINAL PRODUCTION — Whisper → Script → HeyGen → Composite + Logo + Lower Third + Email)
// Pipeline:
// queued -> processing -> heygen_requested -> (API webhook sets rendering + heygen_video_url) -> rendering_in_progress -> completed
//
// Requires Render ENV vars:
// SUPABASE_URL
// SUPABASE_SERVICE_ROLE_KEY
// OPENAI_API_KEY
// HEYGEN_API_KEY
// HEYGEN_CALLBACK_BASE_URL
// HEYGEN_WEBHOOK_SECRET
// HEYGEN_AVATAR_ID_MALE
// HEYGEN_AVATAR_ID_FEMALE
// HEYGEN_VOICE_ID_MALE
// HEYGEN_VOICE_ID_FEMALE
// RESEND_API_KEY
// FROM_EMAIL
//
// Optional ENV vars:
// STORAGE_BUCKET=videos
// POLL_MS=5000
// KEY_COLOR_HEX=#00FF00
// KEY_COLOR_FFMPEG=0x00FF00
// KEY_SIMILARITY=0.32
// KEY_BLEND=0.18
// AVATAR_SCALE_W=560
// AVATAR_MARGIN_X=60
// AVATAR_MARGIN_Y=10
// AVATAR_OPACITY=0.92
// EDGE_SOFTEN=1
// LT_TEXT="Brand New Listing"
// LT_BAR_Y=1660
// LT_BAR_H=240
// LT_BAR_ALPHA=0.55
// LOGO_URL=<public png url>
// LOGO_W=210
// LOGO_MARGIN_X=40
// LOGO_MARGIN_Y=40
// FONT_FILE=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf
//
// Notes:
// - Audio: final video uses HeyGen audio (avatar voice), not the walkthrough audio.
// - Walkthrough audio is used ONLY to transcribe + rewrite script.

import { createClient } from "@supabase/supabase-js";
import { spawn } from "child_process";
import OpenAI from "openai";
import fs from "fs";
import path from "path";
import { Resend } from "resend";

/* ==============================
   ENV
============================== */
function mustEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`❌ Missing environment variable: ${name}`);
    process.exit(1);
  }
  return v;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
const OPENAI_API_KEY = mustEnv("OPENAI_API_KEY");

const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");

const HEYGEN_AVATAR_ID_MALE = mustEnv("HEYGEN_AVATAR_ID_MALE");
const HEYGEN_AVATAR_ID_FEMALE = mustEnv("HEYGEN_AVATAR_ID_FEMALE");

const HEYGEN_VOICE_ID_MALE = mustEnv("HEYGEN_VOICE_ID_MALE");
const HEYGEN_VOICE_ID_FEMALE = mustEnv("HEYGEN_VOICE_ID_FEMALE");

const RESEND_API_KEY = mustEnv("RESEND_API_KEY");
const FROM_EMAIL = mustEnv("FROM_EMAIL");

const STORAGE_BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

// Keying
const KEY_COLOR_HEX = process.env.KEY_COLOR_HEX || "#00FF00";
const KEY_COLOR_FFMPEG = (process.env.KEY_COLOR_FFMPEG || "0x00FF00").trim();
const KEY_SIMILARITY = String(process.env.KEY_SIMILARITY || "0.32");
const KEY_BLEND = String(process.env.KEY_BLEND || "0.18");

// Layout tuning
const AVATAR_SCALE_W = Number(process.env.AVATAR_SCALE_W || 560);
const AVATAR_MARGIN_X = Number(process.env.AVATAR_MARGIN_X || 60);
const AVATAR_MARGIN_Y = Number(process.env.AVATAR_MARGIN_Y || 10); // very low, “sits” on ground
const AVATAR_OPACITY = Number(process.env.AVATAR_OPACITY || 0.92); // reduce “opaque cut-out”
const EDGE_SOFTEN = String(process.env.EDGE_SOFTEN || "1"); // boxblur strength (0 disables)

const LT_TEXT = process.env.LT_TEXT || "Brand New Listing";
const LT_BAR_Y = Number(process.env.LT_BAR_Y || 1660);
const LT_BAR_H = Number(process.env.LT_BAR_H || 240);
const LT_BAR_ALPHA = Number(process.env.LT_BAR_ALPHA || 0.55);

// Static logo (top-right)
const LOGO_URL =
  process.env.LOGO_URL ||
  "https://mzoygebnoenwlxbjbrdb.supabase.co/storage/v1/object/public/logos/reelestate-logo.png";
const LOGO_W = Number(process.env.LOGO_W || 210);
const LOGO_MARGIN_X = Number(process.env.LOGO_MARGIN_X || 40);
const LOGO_MARGIN_Y = Number(process.env.LOGO_MARGIN_Y || 40);

// Font
const FONT_FILE =
  process.env.FONT_FILE || "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf";

/* ==============================
   CLIENTS
============================== */
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const resend = new Resend(RESEND_API_KEY);

console.log("🚀 WORKER LIVE");
console.log("Polling every", POLL_MS, "ms");

/* ==============================
   UTILS
============================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function safeUnlink(p) {
  try {
    if (p && fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);
    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("error", reject);
    ff.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`FFmpeg failed (${code})`))));
  });
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = 120000) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
}

async function downloadFile(url, outPath) {
  const resp = await fetchWithTimeout(url, { method: "GET" }, 120000);
  if (!resp.ok) throw new Error(`Download failed: ${resp.status} (${url})`);
  const buffer = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(outPath, buffer);
}

async function uploadToStorage(localPath, storagePath) {
  const buffer = fs.readFileSync(localPath);

  const { error } = await supabase.storage.from(STORAGE_BUCKET).upload(storagePath, buffer, {
    contentType: "video/mp4",
    upsert: true,
  });
  if (error) throw error;

  const { data } = supabase.storage.from(STORAGE_BUCKET).getPublicUrl(storagePath);
  return data.publicUrl;
}

async function sendFinalEmail(to, url, jobId) {
  await resend.emails.send({
    from: FROM_EMAIL,
    to,
    subject: "🎬 Your ReelEstate Video Is Ready",
    html: `
      <div style="font-family: Arial, sans-serif; line-height: 1.4;">
        <h2>Your video is ready ✅</h2>
        <p><strong>Job:</strong> ${jobId}</p>
        <p>Click below to view/download your rendered video:</p>
        <p><a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a></p>
      </div>
    `,
  });
}

/* ==============================
   AUDIO → WHISPER → SCRIPT
============================== */
async function extractAudioToM4a(videoPath, audioOutPath) {
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

function estimateWordTarget(seconds) {
  // ~2.4 words/sec pace
  return Math.max(35, Math.round(seconds * 2.4));
}

async function generateAvatarScriptFromWalkthrough(walkthroughUrl, jobId, maxSeconds = 20) {
  const tmp = "/tmp";
  const videoPath = path.join(tmp, `walk-${jobId}.mp4`);
  const audioPath = path.join(tmp, `audio-${jobId}.m4a`);

  try {
    await downloadFile(walkthroughUrl, videoPath);
    await extractAudioToM4a(videoPath, audioPath);

    console.log("🧠 Transcribing walkthrough audio (Whisper)…");

    const transcriptRes = await openai.audio.transcriptions.create({
      model: "whisper-1",
      file: fs.createReadStream(audioPath),
    });

    const transcript = (transcriptRes?.text || "").trim();
    if (!transcript) throw new Error("Empty transcript from Whisper");

    console.log("✍️ Writing avatar script from transcript…");

    const wordTarget = estimateWordTarget(maxSeconds);

    const scriptRes = await openai.chat.completions.create({
      model: process.env.OPENAI_SCRIPT_MODEL || "gpt-4o-mini",
      temperature: 0.6,
      messages: [
        {
          role: "system",
          content:
            "You are a professional real estate agent speaking on camera. " +
            "Rewrite the transcript into a confident natural spoken script for an avatar narrator. " +
            "No bullet points. No headings. No emojis. No stage directions. " +
            "Do NOT mention 'walkthrough', 'recording', or 'this video'. " +
            "Keep it concise and persuasive. End with a simple call-to-action to book a viewing.",
        },
        {
          role: "user",
          content: `MAX SECONDS: ${maxSeconds}\nWORD TARGET: ~${wordTarget}\n\nTRANSCRIPT:\n${transcript}`,
        },
      ],
    });

    const script = (scriptRes?.choices?.[0]?.message?.content || "").trim();
    if (!script) throw new Error("Empty script from OpenAI");

    return { transcript, script };
  } finally {
    safeUnlink(videoPath);
    safeUnlink(audioPath);
  }
}

/* ==============================
   HEYGEN: CREATE VIDEO
   callback_url MUST be root-level
============================== */
async function createHeygenVideo({ scriptText, jobId, avatarId, voiceId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("🎬 Creating HeyGen video for job:", jobId);
  console.log("Callback URL:", callbackUrl);

  const payload = {
    video_inputs: [
      {
        character: { type: "avatar", avatar_id: avatarId },
        voice: { type: "text", voice_id: voiceId, input_text: scriptText.trim() },
        background: { type: "color", value: KEY_COLOR_HEX },
      },
    ],
    dimension: { width: 1080, height: 1920 },
    callback_url: callbackUrl,
  };

  const resp = await fetchWithTimeout("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": HEYGEN_API_KEY,
    },
    body: JSON.stringify(payload),
  });

  const text = await resp.text().catch(() => "");
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {}

  if (!resp.ok) {
    console.error("HeyGen error:", resp.status, text);
    throw new Error(`HeyGen generate failed (${resp.status})`);
  }

  const videoId = json?.data?.video_id;
  if (!videoId) {
    console.error("HeyGen response:", json);
    throw new Error("HeyGen did not return video_id");
  }

  console.log("✅ HEYGEN VIDEO ID:", videoId);
  return videoId;
}

/* ==============================
   DB HELPERS
============================== */
async function failJob(jobId, err) {
  const msg = String(err?.message || err || "Unknown error");
  console.error("❌ Job failed:", jobId, msg);

  await supabase
    .from("render_jobs")
    .update({ status: "failed", error: msg.slice(0, 2000) })
    .eq("id", jobId);
}

/* ==============================
   PHASE 1 — queued -> heygen_requested
============================== */
async function processQueued(job) {
  const jobId = job.id;

  // Lock job
  const { data: locked, error: lockErr } = await supabase
    .from("render_jobs")
    .update({ status: "processing" })
    .eq("id", jobId)
    .eq("status", "queued")
    .select("*")
    .maybeSingle();

  if (lockErr) throw lockErr;
  if (!locked) return;

  console.log("📦 Processing QUEUED job:", jobId);

  const maxSeconds = Number(locked.max_seconds || 20);
  const avatarType = String(locked.avatar_type || "female").toLowerCase();
  const isMale = avatarType === "male";

  const avatarId = isMale ? HEYGEN_AVATAR_ID_MALE : HEYGEN_AVATAR_ID_FEMALE;
  const voiceId = isMale ? HEYGEN_VOICE_ID_MALE : HEYGEN_VOICE_ID_FEMALE;

  // Transcribe walkthrough -> write script
  const { transcript, script } = await generateAvatarScriptFromWalkthrough(
    locked.walkthrough_url,
    jobId,
    maxSeconds
  );

  // Ask HeyGen to generate avatar video (green-screen)
  const heygenVideoId = await createHeygenVideo({
    scriptText: script,
    jobId,
    avatarId,
    voiceId,
  });

  // Save
  // NOTE: transcript_text / script_text columns must exist, otherwise remove these two fields.
  const { error: updErr } = await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: heygenVideoId,
      transcript_text: transcript,
      script_text: script,
    })
    .eq("id", jobId);

  if (updErr) throw updErr;

  console.log("⏳ Waiting for webhook to set status=rendering...");
}

/* ==============================
   PHASE 2 — rendering -> completed
   Composite:
   - Background: walkthrough (scaled/padded to 1080x1920)
   - Avatar: colorkey + soften + opacity, bottom-right “sitting”
   - Lower third: bar + text
   - Logo: static png, top-right
   - Audio: HeyGen audio (1:a?)
============================== */
function escapeDrawtext(s) {
  return String(s || "")
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'");
}

async function processRendering(job) {
  const jobId = job.id;

  // Lock rendering job
  const { data: locked, error: lockErr } = await supabase
    .from("render_jobs")
    .update({ status: "rendering_in_progress" })
    .eq("id", jobId)
    .eq("status", "rendering")
    .select("*")
    .maybeSingle();

  if (lockErr) throw lockErr;
  if (!locked) return;

  if (!locked.heygen_video_url) {
    console.log("⚠️ rendering job has no heygen_video_url yet:", jobId);
    await supabase.from("render_jobs").update({ status: "rendering" }).eq("id", jobId);
    return;
  }

  console.log("🎬 Compositing FINAL video:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const logoPath = path.join(tmp, `logo-${jobId}.png`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  try {
    await downloadFile(locked.walkthrough_url, walkPath);
    await downloadFile(locked.heygen_video_url, avatarPath);
    await downloadFile(LOGO_URL, logoPath);

    const safeLT = escapeDrawtext(LT_TEXT);

    // Slight edge soften optional
    const soften =
      EDGE_SOFTEN && Number(EDGE_SOFTEN) > 0
        ? `,boxblur=${EDGE_SOFTEN}:${EDGE_SOFTEN}`
        : "";

    // Filter graph
    // Inputs:
    // 0 = walkthrough mp4
    // 1 = avatar mp4 (green screen)
    // 2 = logo png
    const filter =
      // background normalize to 1080x1920
      `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
      `pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1[vbg0];` +

      // lower third bar + text
      `[vbg0]drawbox=x=0:y=${LT_BAR_Y}:w=1080:h=${LT_BAR_H}:color=black@${LT_BAR_ALPHA}:t=fill,` +
      `drawtext=fontfile='${FONT_FILE}':text='${safeLT}':fontcolor=white:fontsize=64:` +
      `x=(w-text_w)/2:y=${LT_BAR_Y + 60}[vbg];` +

      // key avatar + scale + soften + opacity
      `[1:v]scale=${AVATAR_SCALE_W}:-2,format=rgba,` +
      `colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND}${soften},` +
      `colorchannelmixer=aa=${AVATAR_OPACITY}[av];` +

      // overlay avatar bottom-right
      `[vbg][av]overlay=x=W-w-${AVATAR_MARGIN_X}:y=H-h-${AVATAR_MARGIN_Y}[v1];` +

      // logo top-right (keep it crisp)
      `[2:v]scale=${LOGO_W}:-1,format=rgba[lg];` +
      `[v1][lg]overlay=x=W-w-${LOGO_MARGIN_X}:y=${LOGO_MARGIN_Y}[outv]`;

    await runFFmpeg([
      "-y",
      "-i",
      walkPath,
      "-i",
      avatarPath,
      "-i",
      logoPath,
      "-filter_complex",
      filter,
      "-map",
      "[outv]",
      "-map",
      "1:a?", // HeyGen audio (avatar)
      "-shortest",
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

    const storagePath = `renders/final-${jobId}.mp4`;
    const publicUrl = await uploadToStorage(finalPath, storagePath);

    const { error: doneErr } = await supabase
      .from("render_jobs")
      .update({
        status: "completed",
        final_public_url: publicUrl,
      })
      .eq("id", jobId);

    if (doneErr) throw doneErr;

    console.log("✅ Completed:", jobId, publicUrl);

    if (locked.email) {
      await sendFinalEmail(locked.email, publicUrl, jobId);
      console.log("📧 Email sent to:", locked.email);
    }
  } finally {
    safeUnlink(walkPath);
    safeUnlink(avatarPath);
    safeUnlink(logoPath);
    safeUnlink(finalPath);
  }
}

/* ==============================
   MAIN LOOP
============================== */
async function loop() {
  while (true) {
    try {
      // 1) queued
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

      // 2) rendering
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
