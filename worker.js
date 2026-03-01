import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

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

const BUCKET = process.env.STORAGE_BUCKET || "videos";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

console.log("🔥 NEW WORKER VERSION LOADED 🔥");

/* ==============================
   DOWNLOAD FILE
============================== */

async function downloadToFile(url, outPath) {
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

/* ==============================
   RUN FFMPEG
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
   CREATE HEYGEN VIDEO (TEXT MODE)
============================== */

async function createHeygenVideo(scriptText) {
  console.log("Sending script to HeyGen:");
  console.log(scriptText);

  const resp = await fetch(
    "https://api.heygen.com/v2/video/generate",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key": mustEnv("HEYGEN_API_KEY"),
      },
      body: JSON.stringify({
        video_inputs: [
          {
            character: {
              type: "avatar",
              avatar_id: mustEnv("HEYGEN_AVATAR_ID"),
            },
            input_text: scriptText.trim(),
            voice_id: mustEnv("HEYGEN_VOICE_ID"),
            background: {
              type: "color",
              value: "#00FF00",
            },
          },
        ],
        dimension: {
          width: 1080,
          height: 1920,
        },
      }),
    }
  );

  const body = await resp.text();

  if (!resp.ok) {
    throw new Error(`HeyGen error: ${body}`);
  }

  const json = JSON.parse(body);
  const videoId = json?.data?.video_id;

  if (!videoId) throw new Error("No video_id returned");

  console.log("HEYGEN VIDEO ID:", videoId);
  return videoId;
}

/* ==============================
   PROCESS QUEUED JOB
============================== */

async function processQueued(job) {
  const jobId = job.id;
  console.log("Processing QUEUED:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);

  await downloadToFile(job.walkthrough_url, walkPath);

  // TEMP SCRIPT (Replace later with GPT summary)
  const scriptText = `
Welcome to this stunning property.
This beautifully presented home offers bright living spaces,
modern finishes, and an exceptional location.
Book your private viewing today.
`;

  const videoId = await createHeygenVideo(scriptText);

  await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: videoId,
    })
    .eq("id", jobId);
}

/* ==============================
   PROCESS RENDERING JOB
============================== */

async function processRendering(job) {
  const jobId = job.id;
  console.log("Rendering FINAL:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  await downloadToFile(job.walkthrough_url, walkPath);
  await downloadToFile(job.heygen_video_url, avatarPath);

  await runFFmpeg([
    "-y",
    "-i",
    walkPath,
    "-i",
    avatarPath,
    "-filter_complex",
    "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[vbg];" +
      "[1:v]scale=iw*0.5:-2,chromakey=0x00FF00:0.18:0.08[fg];" +
      "[vbg][fg]overlay=W-w-60:H-h-100[outv]",
    "-map",
    "[outv]",
    "-map",
    "1:a",
    "-c:v",
    "libx264",
    "-preset",
    "ultrafast",
    "-crf",
    "28",
    "-pix_fmt",
    "yuv420p",
    "-c:a",
    "aac",
    finalPath,
  ]);

  const buffer = fs.readFileSync(finalPath);
  const storagePath = `renders/final-${jobId}.mp4`;

  const { error } = await supabase.storage
    .from(BUCKET)
    .upload(storagePath, buffer, {
      contentType: "video/mp4",
      upsert: true,
    });

  if (error) throw error;

  const { data: pub } =
    supabase.storage.from(BUCKET).getPublicUrl(storagePath);

  await supabase
    .from("render_jobs")
    .update({
      status: "completed",
      final_public_url: pub.publicUrl,
    })
    .eq("id", jobId);

  console.log("Completed:", jobId);
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

loop();
