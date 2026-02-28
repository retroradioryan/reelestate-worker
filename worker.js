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

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function downloadToFile(url, outPath) {
  if (!url || !url.startsWith("http")) {
    throw new Error(`Invalid download URL: ${url}`);
  }

  const resp = await fetch(url);

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Download failed: ${resp.status} ${text}`);
  }

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
   HEYGEN CREATE
============================== */

async function heygenCreateVideo(audioUrl) {
  const avatarId = mustEnv("HEYGEN_AVATAR_ID");

  const resp = await fetch(
    "https://api.heygen.com/v2/video/generate",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key": mustEnv("HEYGEN_API_KEY")
      },
      body: JSON.stringify({
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
        dimension: { width: 1080, height: 1920 }
      })
    }
  );

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HeyGen create error: ${text}`);
  }

  const json = await resp.json();
  return json?.data?.video_id;
}

/* ==============================
   HEYGEN STATUS (FIXED)
============================== */

async function checkHeygenStatus(videoId) {
  const resp = await fetch(
    `https://api.heygen.com/v2/video/${videoId}`,
    {
      headers: {
        "X-Api-Key": mustEnv("HEYGEN_API_KEY")
      }
    }
  );

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HeyGen status error: ${text}`);
  }

  const json = await resp.json();
  return json?.data;
}

/* ==============================
   PHASE 1 — QUEUED
============================== */

async function processQueued(job) {
  const supabase = getSupabase();
  const jobId = job.id;

  console.log("Processing QUEUED:", jobId);

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

  const audioBuffer = fs.readFileSync(audioPath);
  const storagePath = `renders/audio-${jobId}.m4a`;

  await supabase.storage
    .from(BUCKET)
    .upload(storagePath, audioBuffer, {
      contentType: "audio/mp4",
      upsert: true
    });

  const { data: pub } =
    supabase.storage.from(BUCKET).getPublicUrl(storagePath);

  const videoId = await heygenCreateVideo(pub.publicUrl);

  await supabase.from("render_jobs").update({
    status: "heygen_requested",
    heygen_video_id: videoId
  }).eq("id", jobId);
}

/* ==============================
   PHASE 2 — POLL HEYGEN
============================== */

async function processHeygen(job) {
  const supabase = getSupabase();

  console.log("Checking HeyGen:", job.id);

  const status = await checkHeygenStatus(job.heygen_video_id);

  if (status?.status === "completed") {
    await supabase.from("render_jobs").update({
      status: "rendering",
      heygen_video_url: status.video_url
    }).eq("id", job.id);
  }
}

/* ==============================
   PHASE 3 — FINAL RENDER
============================== */

async function processRendering(job) {
  const supabase = getSupabase();
  const jobId = job.id;

  console.log("Rendering:", jobId);

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

  await supabase.storage
    .from(BUCKET)
    .upload(storagePath, buffer, {
      contentType: "video/mp4",
      upsert: true
    });

  const { data: pub } =
    supabase.storage.from(BUCKET).getPublicUrl(storagePath);

  await supabase.from("render_jobs").update({
    status: "completed",
    final_public_url: pub.publicUrl
  }).eq("id", jobId);

  console.log("Completed:", jobId);
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

      const { data: heygen } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "heygen_requested")
        .limit(1);

      if (heygen?.length) {
        await processHeygen(heygen[0]);
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

    await sleep(5000);
  }
}

console.log("Worker started...");
loop();
