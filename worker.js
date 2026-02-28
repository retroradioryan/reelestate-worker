import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

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

async function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function downloadToFile(url, outPath) {
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

async function processJob(job) {
  const supabase = getSupabase();
  const jobId = job.id;

  try {
    console.log("Processing job:", jobId);

    const tmp = "/tmp";
    const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
    const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
    const logoPath = path.join(tmp, `logo-${jobId}.png`);
    const finalPath = path.join(tmp, `final-${jobId}.mp4`);

    await downloadToFile(job.walkthrough_url, walkPath);
    await downloadToFile(job.heygen_video_url, avatarPath);
    await downloadToFile(job.logo_url, logoPath);

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

  } catch (err) {
    console.error("Worker error:", err);
    await supabase.from("render_jobs").update({
      status: "failed",
      error: String(err)
    }).eq("id", jobId);
  }
}

async function loop() {
  const supabase = getSupabase();

  while (true) {
    try {
      const { data: jobs } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "rendering")
        .limit(1);

      if (jobs && jobs.length > 0) {
        await processJob(jobs[0]);
      }

    } catch (err) {
      console.error("Loop error:", err);
    }

    await sleep(4000);
  }
}

console.log("Worker started...");
loop();
