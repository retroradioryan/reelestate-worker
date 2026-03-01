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
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");
const HEYGEN_AVATAR_ID_FEMALE = mustEnv("HEYGEN_AVATAR_ID_FEMALE");
const HEYGEN_AVATAR_ID_MALE = mustEnv("HEYGEN_AVATAR_ID_MALE");

const RESEND_API_KEY = mustEnv("RESEND_API_KEY");
const FROM_EMAIL = mustEnv("FROM_EMAIL");

const STORAGE_BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

/* === Visual tuning === */
const KEY_COLOR_HEX = "#00FF00";
const KEY_COLOR_FFMPEG = "0x00FF00";
const KEY_SIMILARITY = "0.32";  // slightly tighter
const KEY_BLEND = "0.18";       // softer edge

const AVATAR_SCALE_W = 560;     // slightly larger
const AVATAR_MARGIN_X = 60;     // from right
const AVATAR_MARGIN_Y = 20;     // VERY low so she sits grounded

const LT_TEXT = "Brand New Listing";
const LT_BAR_Y = 1660;
const LT_BAR_H = 240;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const resend = new Resend(RESEND_API_KEY);

console.log("🚀 WORKER LIVE");

/* ==============================
   UTILS
============================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);
    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("close", (code) =>
      code === 0 ? resolve() : reject(new Error(`FFmpeg failed (${code})`))
    );
  });
}

async function downloadFile(url, outPath) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error("Download failed");
  const buffer = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(outPath, buffer);
}

function safeUnlink(p) {
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

async function uploadToStorage(localPath, storagePath) {
  const buffer = fs.readFileSync(localPath);

  await supabase.storage.from(STORAGE_BUCKET).upload(storagePath, buffer, {
    contentType: "video/mp4",
    upsert: true,
  });

  const { data } = supabase.storage
    .from(STORAGE_BUCKET)
    .getPublicUrl(storagePath);

  return data.publicUrl;
}

async function sendFinalEmail(to, url) {
  await resend.emails.send({
    from: FROM_EMAIL,
    to,
    subject: "🎬 Your ReelEstate Video Is Ready",
    html: `
      <div style="font-family: Arial;">
        <h2>Your video is ready ✅</h2>
        <p>Click below to download:</p>
        <p><a href="${url}" target="_blank">${url}</a></p>
      </div>
    `,
  });
}

/* ==============================
   AUDIO → TRANSCRIBE → SCRIPT
============================== */
async function extractAudio(videoPath, audioPath) {
  await runFFmpeg([
    "-y",
    "-i", videoPath,
    "-vn",
    "-ac", "1",
    "-ar", "44100",
    "-c:a", "aac",
    audioPath,
  ]);
}

async function generateScript(walkthroughUrl, jobId, maxSeconds) {
  const tmp = "/tmp";
  const videoPath = `${tmp}/walk-${jobId}.mp4`;
  const audioPath = `${tmp}/audio-${jobId}.m4a`;

  try {
    await downloadFile(walkthroughUrl, videoPath);
    await extractAudio(videoPath, audioPath);

    const transcriptRes = await openai.audio.transcriptions.create({
      file: fs.createReadStream(audioPath),
      model: "whisper-1",
    });

    const transcript = transcriptRes.text;

    const scriptRes = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      temperature: 0.6,
      messages: [
        {
          role: "system",
          content:
            "Rewrite this into a confident 20-second real estate script. No bullet points. No headings. End with call to action."
        },
        {
          role: "user",
          content: transcript,
        },
      ],
    });

    return {
      transcript,
      script: scriptRes.choices[0].message.content,
    };
  } finally {
    safeUnlink(videoPath);
    safeUnlink(audioPath);
  }
}

/* ==============================
   HEYGEN
============================== */
async function createHeygenVideo({ scriptText, jobId, avatarId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}?token=${HEYGEN_WEBHOOK_SECRET}&job_id=${jobId}`;

  const payload = {
    video_inputs: [
      {
        character: { type: "avatar", avatar_id: avatarId },
        voice: { type: "text", voice_id: HEYGEN_VOICE_ID, input_text: scriptText },
        background: { type: "color", value: KEY_COLOR_HEX },
      },
    ],
    dimension: { width: 1080, height: 1920 },
    callback_url: callbackUrl,
  };

  const resp = await fetch("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": HEYGEN_API_KEY,
    },
    body: JSON.stringify(payload),
  });

  const json = await resp.json();
  return json.data.video_id;
}

/* ==============================
   PROCESS QUEUED
============================== */
async function processQueued(job) {
  const jobId = job.id;

  const { data: locked } = await supabase
    .from("render_jobs")
    .update({ status: "processing" })
    .eq("id", jobId)
    .eq("status", "queued")
    .select("*")
    .maybeSingle();

  if (!locked) return;

  const avatarId =
    locked.avatar_type === "male"
      ? HEYGEN_AVATAR_ID_MALE
      : HEYGEN_AVATAR_ID_FEMALE;

  const { transcript, script } = await generateScript(
    locked.walkthrough_url,
    jobId,
    locked.max_seconds || 20
  );

  const videoId = await createHeygenVideo({
    scriptText: script,
    jobId,
    avatarId,
  });

  await supabase.from("render_jobs").update({
    status: "heygen_requested",
    heygen_video_id: videoId,
    transcript_text: transcript,
    script_text: script,
  }).eq("id", jobId);
}

/* ==============================
   PROCESS RENDERING
============================== */
async function processRendering(job) {
  const jobId = job.id;

  const { data: locked } = await supabase
    .from("render_jobs")
    .update({ status: "rendering_in_progress" })
    .eq("id", jobId)
    .eq("status", "rendering")
    .select("*")
    .maybeSingle();

  if (!locked || !locked.heygen_video_url) return;

  const tmp = "/tmp";
  const walkPath = `${tmp}/walk-${jobId}.mp4`;
  const avatarPath = `${tmp}/avatar-${jobId}.mp4`;
  const finalPath = `${tmp}/final-${jobId}.mp4`;

  try {
    await downloadFile(locked.walkthrough_url, walkPath);
    await downloadFile(locked.heygen_video_url, avatarPath);

    const font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf";

    const filter =
      `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
      `pad=1080:1920:(ow-iw)/2:(oh-ih)/2,` +
      `drawbox=x=0:y=${LT_BAR_Y}:w=1080:h=${LT_BAR_H}:color=black@0.6:t=fill,` +
      `drawtext=fontfile=${font}:text='${LT_TEXT}':fontcolor=white:fontsize=64:` +
      `x=(w-text_w)/2:y=${LT_BAR_Y + 60}[vbg];` +
      `[1:v]scale=${AVATAR_SCALE_W}:-2,format=rgba,` +
      `colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND},` +
      `boxblur=1:1[av];` +
      `[vbg][av]overlay=x=W-w-${AVATAR_MARGIN_X}:y=H-h-${AVATAR_MARGIN_Y}[outv]`;

    await runFFmpeg([
      "-y",
      "-i", walkPath,
      "-i", avatarPath,
      "-filter_complex", filter,
      "-map", "[outv]",
      "-map", "1:a?",
      "-shortest",
      "-c:v", "libx264",
      "-crf", "23",
      "-preset", "veryfast",
      "-pix_fmt", "yuv420p",
      "-c:a", "aac",
      "-movflags", "+faststart",
      finalPath,
    ]);

    const storagePath = `renders/final-${jobId}.mp4`;
    const publicUrl = await uploadToStorage(finalPath, storagePath);

    await supabase.from("render_jobs").update({
      status: "completed",
      final_public_url: publicUrl,
    }).eq("id", jobId);

    if (locked.email) {
      await sendFinalEmail(locked.email, publicUrl);
    }

  } finally {
    safeUnlink(walkPath);
    safeUnlink(avatarPath);
    safeUnlink(finalPath);
  }
}

/* ==============================
   LOOP
============================== */
async function loop() {
  while (true) {
    try {
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

      const { data: rendering } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "rendering")
        .limit(1);

      if (rendering?.length) {
        await processRendering(rendering[0]);
      }

    } catch (err) {
      console.error("Worker error:", err);
    }

    await sleep(POLL_MS);
  }
}

loop();
