import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

/* ==============================
   ENV
============================== */

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function getSupabase() {
  return createClient(
    mustEnv("SUPABASE_URL"),
    mustEnv("SUPABASE_SERVICE_ROLE_KEY")
  );
}

const BUCKET = process.env.STORAGE_BUCKET || "videos";

/* ==============================
   HELPERS
============================== */

async function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function downloadToFile(url, outPath) {
  if (!url || !url.startsWith("http")) {
    throw new Error(`Invalid download URL: ${url}`);
  }

  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Download failed: ${resp.status}`);

  await new Promise((resolve, reject) => {
    const stream = fs.createWriteStream(outPath);
    resp.body.pipe(stream);
    resp.body.on("error", reject);
    stream.on("finish", resolve);
  });
}

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);
    ff.stderr.on("data", d => console.log(d.toString()));
    ff.on("error", reject);
    ff.on("close", code => {
      if (code === 0) resolve();
      else reject(new Error(`FFmpeg exited ${code}`));
    });
  });
}

/* ==============================
   HEYGEN
============================== */

async function heygenCreateVideo(audioUrl, jobId) {
  const avatarId = mustEnv("HEYGEN_AVATAR_ID");
  const baseUrl = mustEnv("PUBLIC_BASE_URL");
  const secret = mustEnv("HEYGEN_WEBHOOK_SECRET");

  const callbackUrl =
    `${baseUrl}/heygen-callback?token=${secret}&job_id=${jobId}`;

  const body = {
    video_inputs: [{
      character: {
        type: "avatar",
        avatar_id: avatarId
      },
      voice: {
        type: "audio",
        audio_url: audioUrl
      },
      background: {
        type: "color",
        value: "#00FF00"
      }
    }],
    dimension: { width: 1080, height: 1920 },
    callback_url: callbackUrl
  };

  const resp = await fetch(
    "https://api.heygen.com/v2/video/generate",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key": mustEnv("HEYGEN_API_KEY")
      },
      body: JSON.stringify(body)
    }
  );

  const json = await resp.json();
  if (!resp.ok) throw new Error(JSON.stringify(json));

  return json?.data?.video_id;
}

/* ==============================
   PHASE 1 — QUEUED
============================== */

async function processQueued(job) {
  const supabase = getSupabase();
  const jobId = job.id;

  console.log("Processing QUEUED job:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const audioPath = path.join(tmp, `audio-${jobId}.m4a`);

  await downloadToFile(job.walkthrough_url, walkPath);

  await runFFmpeg([
    "-y",
    "-i", walkPath,
    "-vn",
    "-c:a", "aac",
    "-b:a", "128k",
    audioPath
  ]);

  const audioBuf = fs.readFileSync(audioPath);
  const audioStoragePath = `renders/audio-${jobId}.m4a`;

  const up = await supabase.storage
    .from(BUCKET)
    .upload(audioStoragePath, audioBuf, {
      contentType: "audio/mp4",
      upsert: true
    });

  if (up.error) throw up.error;

  const { data: pub } =
    supabase.storage.from(BUCKET).getPublicUrl(up.data.path);

  const heygenVideoId =
    await heygenCreateVideo(pub.publicUrl, jobId);

  await supabase.from("render_jobs").update({
    status: "heygen_requested",
    heygen_video_id: heygenVideoId
  }).eq("id", jobId);

  console.log("HeyGen requested:", jobId);
}

/* ==============================
   PHASE 2 — RENDERING
============================== */

async function processRendering(job) {
  const supabase = getSupabase();
  const jobId = job.id;

  console.log("Processing RENDERING job:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  await downloadToFile(job.walkthrough_url, walkPath);
  await downloadToFile(job.heygen_video_url, avatarPath);

  await runFFmpeg([
    "-y",
    "-i", walkPath,
    "-i", avatarPath,
    "-filter_complex",
    "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[vbg];" +
    "[1:v]scale=iw*0.5:-2,chromakey=0x00FF00:0.18:0.08[fg];" +
    "[vbg][fg]overlay=W-w-60:H-h-100[outv]",
    "-map", "[outv]",
    "-map", "1:a?",
    "-c:v", "libx264",
    "-preset", "ultrafast",
    "-crf", "28",
    "-pix_fmt", "yuv420p",
    "-c:a", "aac",
    finalPath
  ]);

  const buffer = fs.readFileSync(finalPath);
  const storagePath = `renders/final-${jobId}.mp4`;

  const up = await supabase.storage
    .from(BUCKET)
    .upload(storagePath, buffer, {
      contentType: "video/mp4",
      upsert: true
    });

  if (up.error) throw up.error;

  const { data: pub } =
    supabase.storage.from(BUCKET).getPublicUrl(up.data.path);

  await supabase.from("render_jobs").update({
    status: "completed",
    final_public_url: pub.publicUrl
  }).eq("id", jobId);

  console.log("Completed job:", jobId);
}

/* ==============================
   MAIN LOOP
============================== */

async function loop() {
  const supabase = getSupabase();

  while (true) {
    try {

      const { data: queued } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "queued")
        .limit(1);

      if (queued?.length) {
        await processQueued(queued[0]);
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

    await sleep(4000);
  }
}

console.log("Worker started...");
loop();
