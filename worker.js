// worker.js (PRODUCTION — FULL TRANSCRIBE (<=5min) → SYNCED MONTAGE (max_seconds) → HeyGen → Composite + Logo + Lower Third + Email)
//
// ✅ Uses EXISTING DB columns ONLY:
// - transcript_text: full Whisper transcript
// - script_text: JSON string containing montage plan + final narration script
// - max_seconds: desired final length (default 120)
// - logo_url: optional per-job logo (falls back to env LOGO_URL)
// - final_public_url: output link
//
// Montage sync approach:
// 1) Whisper full transcript with timestamps (<=5 mins cap)
// 2) GPT builds montage plan: [{start,end,line}] where total duration ≈ targetSeconds
// 3) GPT also returns combined "script" (lines joined naturally)
// 4) Worker cuts & concatenates those windows into a montage background
// 5) HeyGen narrates the montage script
// 6) Composite avatar over montage with lower third + logo
//
// Pipeline:
// queued -> processing -> heygen_requested -> (webhook sets rendering + heygen_video_url) -> rendering_in_progress -> completed

console.log("HEYGEN_API_KEY exists?", Boolean(process.env.HEYGEN_API_KEY));
console.log("HEYGEN_API_KEY length:", process.env.HEYGEN_API_KEY?.length);

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

// Cost control
const MAX_TRANSCRIBE_SECONDS = Number(process.env.MAX_TRANSCRIBE_SECONDS || 300); // 5 mins max
const MAX_SEGMENTS_TO_SEND = Number(process.env.MAX_SEGMENTS_TO_SEND || 450);

// Output defaults
const DEFAULT_TARGET_SECONDS = Number(process.env.DEFAULT_TARGET_SECONDS || 300); // 5 minutes default
const MAX_TARGET_SECONDS = Number(process.env.MAX_TARGET_SECONDS || 300); // clamp user input (optional safety)

// Keying
const KEY_COLOR_HEX = process.env.KEY_COLOR_HEX || "#00FF00";
const KEY_COLOR_FFMPEG = (process.env.KEY_COLOR_FFMPEG || "0x00FF00").trim();
const KEY_SIMILARITY = String(process.env.KEY_SIMILARITY || "0.32");
const KEY_BLEND = String(process.env.KEY_BLEND || "0.18");

// Layout tuning
const AVATAR_SCALE_W = Number(process.env.AVATAR_SCALE_W || 560);
const AVATAR_MARGIN_X = Number(process.env.AVATAR_MARGIN_X || 60);
const AVATAR_MARGIN_Y = Number(process.env.AVATAR_MARGIN_Y || 10);
const AVATAR_OPACITY = Number(process.env.AVATAR_OPACITY || 0.92);
const EDGE_SOFTEN = String(process.env.EDGE_SOFTEN || "1");

// Lower third
const LT_TEXT = process.env.LT_TEXT || "Brand New Listing";
const LT_BAR_Y = Number(process.env.LT_BAR_Y || 1660);
const LT_BAR_H = Number(process.env.LT_BAR_H || 240);
const LT_BAR_ALPHA = Number(process.env.LT_BAR_ALPHA || 0.55);

// Default logo (can be overridden per job by render_jobs.logo_url)
const DEFAULT_LOGO_URL =
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
console.log("Limits:", {
  MAX_TRANSCRIBE_SECONDS,
  DEFAULT_TARGET_SECONDS,
  MAX_TARGET_SECONDS,
});

/* ==============================
   UTILS
============================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function safeUnlink(p) {
  try {
    if (p && fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);
    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("error", reject);
    ff.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`FFmpeg failed (${code})`))
    );
  });
}

function runFFprobe(args) {
  return new Promise((resolve, reject) => {
    const fp = spawn("ffprobe", args);
    let out = "";
    let err = "";
    fp.stdout.on("data", (d) => (out += d.toString()));
    fp.stderr.on("data", (d) => (err += d.toString()));
    fp.on("error", reject);
    fp.on("close", (code) => {
      if (code === 0) return resolve({ out, err });
      reject(new Error(`ffprobe failed (${code}): ${err || out}`));
    });
  });
}

async function getVideoDurationSeconds(localVideoPath) {
  const { out } = await runFFprobe([
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    localVideoPath,
  ]);
  const dur = Number(String(out || "").trim());
  if (!Number.isFinite(dur) || dur <= 0) return null;
  return dur;
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
   AUDIO → WHISPER → MONTAGE PLAN
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
  // ~2.4 words/sec
  return Math.max(60, Math.round(seconds * 2.4));
}

/**
 * Build montage video: trims each segment and concatenates (video only).
 * segments: [{start:number,end:number}]
 */
async function buildMontageVideo(inWalkPath, outMontagePath, segments, targetSeconds) {
  if (!Array.isArray(segments) || segments.length === 0) {
    // fallback: first targetSeconds
    await runFFmpeg([
      "-y",
      "-i",
      inWalkPath,
      "-t",
      String(targetSeconds),
      "-c:v",
      "libx264",
      "-preset",
      "veryfast",
      "-crf",
      "23",
      "-pix_fmt",
      "yuv420p",
      "-movflags",
      "+faststart",
      outMontagePath,
    ]);
    return;
  }

  const parts = [];
  for (let i = 0; i < segments.length; i++) {
    const s = Math.max(0, Number(segments[i].start || 0));
    const e = Math.max(s, Number(segments[i].end || s));
    parts.push(`[0:v]trim=start=${s}:end=${e},setpts=PTS-STARTPTS[v${i}]`);
  }

  const concatInputs = segments.map((_, i) => `[v${i}]`).join("");
  const filter = `${parts.join(";")};${concatInputs}concat=n=${segments.length}:v=1:a=0[vout]`;

  await runFFmpeg([
    "-y",
    "-i",
    inWalkPath,
    "-filter_complex",
    filter,
    "-map",
    "[vout]",
    "-t",
    String(targetSeconds),
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "23",
    "-pix_fmt",
    "yuv420p",
    "-movflags",
    "+faststart",
    outMontagePath,
  ]);
}

async function generateMontagePlanFromWalkthrough(walkthroughUrl, jobId, targetSeconds) {
  const tmp = "/tmp";
  const videoPath = path.join(tmp, `walk-${jobId}.mp4`);
  const audioPath = path.join(tmp, `audio-${jobId}.m4a`);

  try {
    await downloadFile(walkthroughUrl, videoPath);

    // cost control: reject > 5 min
    const duration = await getVideoDurationSeconds(videoPath);
    if (duration && duration > MAX_TRANSCRIBE_SECONDS) {
      throw new Error(
        `Walkthrough is ${Math.round(duration)}s (~${Math.ceil(duration / 60)}min). ` +
          `Max allowed is ${MAX_TRANSCRIBE_SECONDS}s (${Math.ceil(MAX_TRANSCRIBE_SECONDS / 60)}min).`
      );
    }

    await extractAudioToM4a(videoPath, audioPath);

    console.log("🧠 Whisper transcribing full audio (verbose_json) …");
    const transcriptRes = await openai.audio.transcriptions.create({
      model: "whisper-1",
      file: fs.createReadStream(audioPath),
      response_format: "verbose_json",
    });

    const transcript = String(transcriptRes?.text || "").trim();
    const segs = Array.isArray(transcriptRes?.segments) ? transcriptRes.segments : [];

    if (!transcript) throw new Error("Empty transcript from Whisper");

    const timed = segs.slice(0, MAX_SEGMENTS_TO_SEND).map((s) => ({
      start: Number(s.start || 0),
      end: Number(s.end || 0),
      text: String(s.text || "").trim(),
    }));

    const wordTarget = estimateWordTarget(targetSeconds);

    console.log("✍️ GPT building montage plan …");

    const planRes = await openai.chat.completions.create({
      model: process.env.OPENAI_SCRIPT_MODEL || "gpt-4o-mini",
      temperature: 0.4,
      messages: [
        {
          role: "system",
          content:
            "You are a senior video editor for real-estate walk-throughs. " +
            "Create a montage plan that stays synced with narration.\n\n" +
            "Return ONLY valid JSON:\n" +
            "{\n" +
            '  "segments": [ { "start": number, "end": number, "line": string } ],\n' +
            '  "script": string\n' +
            "}\n\n" +
            `Rules:\n- Total duration (sum of end-start) ≈ ${targetSeconds} seconds (±3s).\n` +
            "- Segments MUST be chronological.\n" +
            "- Each segment 6–25 seconds.\n" +
            "- Each segment 'line' MUST describe what is being shown in that window.\n" +
            "- 'script' should be a natural narration built from the lines.\n" +
            "- Style: confident agent voice, no bullets/headings/emojis/stage directions. " +
            "Do NOT say 'walkthrough', 'recording', or 'this video'. End with a call-to-action.\n" +
            `- Keep the full narration around ${wordTarget} words.\n`,
        },
        {
          role: "user",
          content:
            `TARGET_SECONDS: ${targetSeconds}\n\n` +
            `FULL_TRANSCRIPT:\n${transcript}\n\n` +
            `TIMED_SEGMENTS:\n${JSON.stringify(timed)}\n`,
        },
      ],
    });

    const raw = String(planRes?.choices?.[0]?.message?.content || "").trim();
    const json = safeJsonParse(raw);

    if (!json?.segments?.length) {
      console.log("⚠️ Montage plan invalid JSON; fallback to first segment.");
      return {
        transcript,
        plan: {
          segments: [{ start: 0, end: targetSeconds, line: "" }],
          script: "",
        },
      };
    }

    const maxT =
      duration ||
      (segs.length ? Number(segs[segs.length - 1]?.end || 0) : null) ||
      null;

    let cleaned = [];
    let sum = 0;
    let lastStart = -1;

    for (const seg of json.segments) {
      let s = Number(seg.start || 0);
      let e = Number(seg.end || 0);
      if (!Number.isFinite(s) || !Number.isFinite(e) || e <= s) continue;

      if (maxT && maxT > 0) {
        s = clamp(s, 0, maxT);
        e = clamp(e, 0, maxT);
        if (e <= s) continue;
      } else {
        s = Math.max(0, s);
        e = Math.max(s, e);
      }

      if (s <= lastStart) continue; // enforce chronological
      lastStart = s;

      const d = e - s;
      if (d < 6 || d > 25) continue;

      cleaned.push({
        start: s,
        end: e,
        line: String(seg.line || "").trim(),
      });

      sum += d;
      if (sum >= targetSeconds - 2) break;
    }

    if (!cleaned.length) {
      cleaned = [{ start: 0, end: targetSeconds, line: "" }];
    }

    const script =
      String(json.script || "").trim() ||
      cleaned.map((x) => x.line).filter(Boolean).join(" ").trim();

    if (!script) {
      // last resort: create minimal script
      return { transcript, plan: { segments: cleaned, script: "A quick highlight tour of this property—get in touch to book a viewing." } };
    }

    return { transcript, plan: { segments: cleaned, script } };
  } finally {
    safeUnlink(videoPath);
    safeUnlink(audioPath);
  }
}

/* ==============================
   HEYGEN
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

  await supabase.from("render_jobs").update({ status: "failed", error: msg.slice(0, 2000) }).eq("id", jobId);
}

/* ==============================
   PHASE 1 — queued -> heygen_requested
============================== */
async function processQueued(job) {
  const jobId = job.id;

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

  // Target seconds from job.max_seconds (default 120). Clamp to protect costs/UX.
  let targetSeconds = Number(locked.max_seconds || DEFAULT_TARGET_SECONDS);
  if (!Number.isFinite(targetSeconds) || targetSeconds <= 0) targetSeconds = DEFAULT_TARGET_SECONDS;
  targetSeconds = clamp(targetSeconds, 20, MAX_TARGET_SECONDS);

  let avatarId = null;
  let voiceId = null;

  // Resolve avatar from avatars table if present, else fallback to env
  if (locked.avatar_id) {
    const { data: avatar, error: avatarErr } = await supabase
      .from("avatars")
      .select("provider_avatar_id")
      .eq("id", locked.avatar_id)
      .single();

    if (avatarErr || !avatar) throw new Error("Avatar not found for job " + jobId);
    avatarId = avatar.provider_avatar_id;
  }

  // Resolve voice from voices table if present, else fallback to env
  if (locked.voice_id) {
    const { data: voice, error: voiceErr } = await supabase
      .from("voices")
      .select("provider_voice_id")
      .eq("id", locked.voice_id)
      .single();

    if (voiceErr || !voice) throw new Error("Voice not found for job " + jobId);
    voiceId = voice.provider_voice_id;
  }

  if (!avatarId || !voiceId) {
    console.log("⚠️ Falling back to legacy avatar_type logic");
    const avatarType = String(locked.avatar_type || "female").toLowerCase();
    const isMale = avatarType === "male";
    avatarId = isMale ? HEYGEN_AVATAR_ID_MALE : HEYGEN_AVATAR_ID_FEMALE;
    voiceId = isMale ? HEYGEN_VOICE_ID_MALE : HEYGEN_VOICE_ID_FEMALE;
  }

  // Full transcribe (<=5min) -> montage plan for targetSeconds
  const { transcript, plan } = await generateMontagePlanFromWalkthrough(
    locked.walkthrough_url,
    jobId,
    targetSeconds
  );

  // Pack plan JSON into script_text
  const packed = JSON.stringify({
    targetSeconds,
    script: plan.script,
    segments: plan.segments, // [{start,end,line}]
  });

  // Create HeyGen avatar video from plan.script
  const heygenVideoId = await createHeygenVideo({
    scriptText: plan.script,
    jobId,
    avatarId,
    voiceId,
  });

  const { error: updErr } = await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: heygenVideoId,
      transcript_text: transcript,
      script_text: packed,         // montage plan stored here
      max_seconds: targetSeconds,  // normalize saved value
    })
    .eq("id", jobId);

  if (updErr) throw updErr;

  console.log("⏳ Waiting for webhook to set status=rendering...");
}

/* ==============================
   PHASE 2 — rendering -> completed
============================== */
function escapeDrawtext(s) {
  return String(s || "")
    .replace(/\\/g, "\\\\")
    .replace(/:/g, "\\:")
    .replace(/'/g, "\\'");
}

async function processRendering(job) {
  const jobId = job.id;

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

  console.log("🎬 Compositing FINAL montage video:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const montagePath = path.join(tmp, `montage-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const logoPath = path.join(tmp, `logo-${jobId}.png`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  try {
    await downloadFile(locked.walkthrough_url, walkPath);
    await downloadFile(locked.heygen_video_url, avatarPath);

    const logoUrl = (locked.logo_url || DEFAULT_LOGO_URL).trim();
    await downloadFile(logoUrl, logoPath);

    // Parse montage plan from script_text
    const packed = safeJsonParse(String(locked.script_text || ""));
    const targetSeconds = clamp(
      Number(packed?.targetSeconds || locked.max_seconds || DEFAULT_TARGET_SECONDS),
      20,
      MAX_TARGET_SECONDS
    );

    const segments = Array.isArray(packed?.segments) ? packed.segments : [];

    // Build montage background
    await buildMontageVideo(walkPath, montagePath, segments, targetSeconds);

    const headline = (locked.property_headline || LT_TEXT || "Brand New Listing").trim();
    const safeLT = escapeDrawtext(headline);

    const soften =
      EDGE_SOFTEN && Number(EDGE_SOFTEN) > 0 ? `,boxblur=${EDGE_SOFTEN}:${EDGE_SOFTEN}` : "";

    const filter =
      `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
      `pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1[vbg0];` +
      `[vbg0]drawbox=x=0:y=${LT_BAR_Y}:w=1080:h=${LT_BAR_H}:color=black@${LT_BAR_ALPHA}:t=fill,` +
      `drawtext=fontfile='${FONT_FILE}':text='${safeLT}':fontcolor=white:fontsize=64:` +
      `x=(w-text_w)/2:y=${LT_BAR_Y + 60}[vbg];` +
      `[1:v]scale=${AVATAR_SCALE_W}:-2,format=rgba,` +
      `colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND}${soften},` +
      `colorchannelmixer=aa=${AVATAR_OPACITY}[av];` +
      `[vbg][av]overlay=x=W-w-${AVATAR_MARGIN_X}:y=${LT_BAR_Y}-h-30[v1];` +
      `[2:v]scale=${LOGO_W}:-1,format=rgba[lg];` +
      `[v1][lg]overlay=x=W-w-${LOGO_MARGIN_X}:y=${LOGO_MARGIN_Y}[outv]`;

    await runFFmpeg([
      "-y",
      "-i",
      montagePath,
      "-i",
      avatarPath,
      "-i",
      logoPath,
      "-filter_complex",
      filter,
      "-map",
      "[outv]",
      "-map",
      "1:a?", // HeyGen audio
      "-shortest",
      "-t",
      String(targetSeconds),
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
        final_storage_path: storagePath,
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
    safeUnlink(montagePath);
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
      // queued
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

      // rendering
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
