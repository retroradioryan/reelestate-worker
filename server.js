import express from "express";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();

/* ----------------------------------
   BODY LIMITS (important for signed URLs)
---------------------------------- */
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

/* ----------------------------------
   HELPERS
---------------------------------- */

function getSupabase() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url || !key) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  return createClient(url, key);
}

async function downloadToFile(fileUrl, outPath) {
  console.log("Downloading:", fileUrl);

  const response = await fetch(fileUrl);

  if (!response.ok) {
    throw new Error(`Download failed: ${response.status}`);
  }

  await new Promise((resolve, reject) => {
    const fileStream = fs.createWriteStream(outPath);
    response.body.pipe(fileStream);
    response.body.on("error", reject);
    fileStream.on("finish", resolve);
  });

  console.log("Downloaded to:", outPath);
}

/* ----------------------------------
   SAFE FFMPEG RUNNER (async)
---------------------------------- */

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    console.log("Running FFmpeg...");

    const ff = spawn("ffmpeg", args);

    ff.stderr.on("data", (data) => {
      console.log(data.toString());
    });

    ff.on("close", (code) => {
      if (code === 0) {
        console.log("FFmpeg complete");
        resolve();
      } else {
        reject(new Error(`FFmpeg exited with code ${code}`));
      }
    });
  });
}

/* ----------------------------------
   HEALTH CHECK
---------------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

/* ----------------------------------
   COMPOSE WALKTHROUGH
---------------------------------- */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    console.log("Compose request received");

    const { walkthroughUrl, avatarUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !avatarUrl) {
      return res.status(400).json({
        ok: false,
        error: "Missing walkthroughUrl or avatarUrl"
      });
    }

    const supabase = getSupabase();
    const bucket = process.env.STORAGE_BUCKET || "videos";

    const tmpDir = "/tmp";
    const id = Date.now();

    const walkthroughPath = path.join(tmpDir, `walk-${id}.mp4`);
    const avatarPath = path.join(tmpDir, `avatar-${id}.mp4`);
    const outputPath = path.join(tmpDir, `final-${id}.mp4`);

    /* ----------------------------------
       1️⃣ DOWNLOAD INPUTS
    ---------------------------------- */

    await downloadToFile(walkthroughUrl, walkthroughPath);
    await downloadToFile(avatarUrl, avatarPath);

    /* ----------------------------------
       2️⃣ COMPOSE VIDEO
       - Vertical 1080x1920
       - Avatar PiP bottom-right
       - REMOVE walkthrough audio
       - USE avatar audio
    ---------------------------------- */

    const ffmpegArgs = [
      "-y",
      "-t", String(maxSeconds),

      // Inputs
      "-i", walkthroughPath,
      "-i", avatarPath,

      // Filters
      "-filter_complex",
      "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[bg];" +
      "[1:v]scale=iw*0.30:-2,chromakey=0x00FF00:0.18:0.10,format=rgba[fg];" +
      "[bg][fg]overlay=W-w-40:H-h-60",

      // 🔥 THIS IS THE IMPORTANT PART
      // Use avatar audio (input 1)
      "-map", "1:a?",

      // Video encoding
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      "-pix_fmt", "yuv420p",

      outputPath
    ];

    await runFFmpeg(ffmpegArgs);

    /* ----------------------------------
       3️⃣ UPLOAD FINAL VIDEO
    ---------------------------------- */

    const fileBuffer = fs.readFileSync(outputPath);
    const storagePath = `renders/walkthrough-${id}.mp4`;

    const { data, error } = await supabase.storage
      .from(bucket)
      .upload(storagePath, fileBuffer, {
        contentType: "video/mp4",
        upsert: true
      });

    if (error) throw error;

    const { data: publicUrl } =
      supabase.storage.from(bucket).getPublicUrl(data.path);

    console.log("Upload complete:", publicUrl.publicUrl);

    res.json({
      ok: true,
      url: publicUrl.publicUrl
    });

  } catch (error) {
    console.error("COMPOSE ERROR:", error);

    res.status(500).json({
      ok: false,
      error: String(error)
    });
  }
});

/* ----------------------------------
   START SERVER
---------------------------------- */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`Worker running on port ${PORT}`);
});
