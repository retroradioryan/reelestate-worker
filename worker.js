import { createClient } from "@supabase/supabase-js";
import { spawn } from "child_process";
import OpenAI from "openai";
import fs from "fs";

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
const OPENAI_API_KEY = mustEnv("OPENAI_API_KEY");

const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_AVATAR_ID = mustEnv("HEYGEN_AVATAR_ID");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

const STORAGE_BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

console.log("🚀 WORKER LIVE");

/* ==============================
   UTILS
============================== */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);
    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error("FFmpeg failed"));
    });
  });
}

async function downloadFile(url, outPath) {
  const resp = await fetch(url);
  const buffer = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(outPath, buffer);
}

async function uploadToStorage(localPath, storagePath) {
  const buffer = fs.readFileSync(localPath);

  await supabase.storage
    .from(STORAGE_BUCKET)
    .upload(storagePath, buffer, {
      contentType: "video/mp4",
      upsert: true,
    });

  const { data } = supabase.storage
    .from(STORAGE_BUCKET)
    .getPublicUrl(storagePath);

  return data.publicUrl;
}

/* ==============================
   AUDIO → TRANSCRIBE → SUMMARY
============================== */

async function extractAudio(videoPath, audioPath) {
  await runFFmpeg([
    "-y",
    "-i", videoPath,
    "-vn",
    "-acodec", "mp3",
    audioPath,
  ]);
}

async function generateAvatarScript(walkthroughUrl, jobId) {
  const videoPath = `/tmp/walk-${jobId}.mp4`;
  const audioPath = `/tmp/audio-${jobId}.mp3`;

  await downloadFile(walkthroughUrl, videoPath);
  await extractAudio(videoPath, audioPath);

  console.log("🧠 Transcribing walkthrough audio...");

  const transcript = await openai.audio.transcriptions.create({
    file: fs.createReadStream(audioPath),
    model: "whisper-1",
  });

  console.log("✍️ Generating agent script...");

  const summary = await openai.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "Summarise this property walkthrough into a confident, polished 20-second real estate script spoken by a professional agent."
      },
      {
        role: "user",
        content: transcript.text,
      },
    ],
  });

  return summary.choices[0].message.content;
}

/* ==============================
   HEYGEN VIDEO CREATE
============================== */

async function createHeygenVideo({ scriptText, jobId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}&job_id=${encodeURIComponent(jobId)}`;

  console.log("🎬 Creating HeyGen video:", jobId);

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
        background: { type: "color", value: "#00FF00" },
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
  if (!json?.data?.video_id) throw new Error("HeyGen video_id missing");

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

  console.log("📦 Processing job:", jobId);

  const script = await generateAvatarScript(
    locked.walkthrough_url,
    jobId
  );

  const videoId = await createHeygenVideo({ scriptText: script, jobId });

  await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: videoId,
    })
    .eq("id", jobId);
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

  console.log("🎬 Compositing final video:", jobId);

  const walkPath = `/tmp/walk-${jobId}.mp4`;
  const avatarPath = `/tmp/avatar-${jobId}.mp4`;
  const finalPath = `/tmp/final-${jobId}.mp4`;

  await downloadFile(locked.walkthrough_url, walkPath);
  await downloadFile(locked.heygen_video_url, avatarPath);

  const filter =
    `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
    `pad=1080:1920:(ow-iw)/2:(oh-ih)/2,` +
    `drawbox=x=0:y=1650:w=1080:h=220:color=black@0.55:t=fill,` +
    `drawtext=fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf:` +
    `text='Brand New Listing':fontcolor=white:fontsize=64:x=(w-text_w)/2:y=1710[vbg];` +
    `[1:v]scale=520:-2,` +
    `colorkey=0x00FF00:0.32:0.15,` +
    `format=rgba,` +
    `colorchannelmixer=aa=0.92[av];` +
    `[vbg][av]overlay=W-w-80:H-h-180[outv]`;

  await runFFmpeg([
    "-y",
    "-i", walkPath,
    "-i", avatarPath,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "1:a?",
    "-shortest",
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-pix_fmt", "yuv420p",
    "-c:a", "aac",
    "-b:a", "160k",
    "-movflags", "+faststart",
    finalPath,
  ]);

  const publicUrl = await uploadToStorage(
    finalPath,
    `renders/final-${jobId}.mp4`
  );

  await supabase
    .from("render_jobs")
    .update({
      status: "completed",
      final_public_url: publicUrl,
    })
    .eq("id", jobId);

  console.log("✅ Completed:", jobId);

  fs.unlinkSync(walkPath);
  fs.unlinkSync(avatarPath);
  fs.unlinkSync(finalPath);
}

/* ==============================
   MAIN LOOP
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
      console.error("❌ Worker error:", err);
    }

    await sleep(POLL_MS);
  }
}

loop();
