import { createClient } from "@supabase/supabase-js";
import { spawn } from "child_process";
import fs from "fs";
import path from "path";

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

const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_AVATAR_ID = mustEnv("HEYGEN_AVATAR_ID");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

const STORAGE_BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("🚀 WORKER LIVE");
console.log("Polling every", POLL_MS, "ms");

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

  const { error } = await supabase.storage
    .from(STORAGE_BUCKET)
    .upload(storagePath, buffer, {
      contentType: "video/mp4",
      upsert: true,
    });

  if (error) throw error;

  const { data } = supabase.storage
    .from(STORAGE_BUCKET)
    .getPublicUrl(storagePath);

  return data.publicUrl;
}

/* ==============================
   HEYGEN: CREATE VIDEO
============================== */

async function createHeygenVideo({ scriptText, jobId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("🎬 Creating HeyGen video for job:", jobId);

  const payload = {
    video_inputs: [
      {
        character: { type: "avatar", avatar_id: HEYGEN_AVATAR_ID },
        voice: { type: "text", voice_id: HEYGEN_VOICE_ID, input_text: scriptText },
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

  const videoId = json?.data?.video_id;
  if (!videoId) throw new Error("HeyGen did not return video_id");

  console.log("✅ HEYGEN VIDEO ID:", videoId);
  return videoId;
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

  console.log("📦 Processing QUEUED job:", jobId);

  const script =
    "Welcome to this beautiful new listing. Contact us today to arrange your private viewing.";

  const videoId = await createHeygenVideo({ scriptText: script, jobId });

  await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: videoId,
    })
    .eq("id", jobId);

  console.log("⏳ Waiting for webhook...");
}

/* ==============================
   PROCESS RENDERING (COMPOSITE)
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

  if (!locked) return;

  if (!locked.heygen_video_url) {
    await supabase.from("render_jobs").update({ status: "rendering" }).eq("id", jobId);
    return;
  }

  console.log("🎬 Compositing final video:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  await downloadFile(locked.walkthrough_url, walkPath);
  await downloadFile(locked.heygen_video_url, avatarPath);

  const filter =
    `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
    `pad=1080:1920:(ow-iw)/2:(oh-ih)/2[vbg];` +
    `[1:v]scale=460:-2,colorkey=0x00FF00:0.35:0.2[av];` +
    `[vbg][av]overlay=W-w-60:H-h-180[outv]`;

  await runFFmpeg([
    "-y",
    "-i", walkPath,
    "-i", avatarPath,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "1:a?",
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-pix_fmt", "yuv420p",
    "-c:a", "aac",
    "-b:a", "160k",
    "-movflags", "+faststart",
    finalPath,
  ]);

  const storagePath = `renders/final-${jobId}.mp4`;
  const publicUrl = await uploadToStorage(finalPath, storagePath);

  await supabase
    .from("render_jobs")
    .update({
      status: "completed",
      final_public_url: publicUrl,
    })
    .eq("id", jobId);

  console.log("✅ Branded video complete:", jobId);

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
