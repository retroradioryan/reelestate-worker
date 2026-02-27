import express from "express";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();

/* ----------------------------------
   IMPORTANT: Increase body limit
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

  const r = await fetch(fileUrl);
  if (!r.ok) throw new Error(`Download failed ${r.status}`);

  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(outPath);
    r.body.pipe(ws);
    r.body.on("error", reject);
    ws.on("finish", resolve);
  });
}

/* ----------------------------------
   HEALTH
---------------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

/* ----------------------------------
   SAFE FFMPEG RUNNER
---------------------------------- */

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);

    ff.stderr.on("data", (data) => {
      console.log(data.toString());
    });

    ff.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`FFmpeg exited with code ${code}`));
    });
  });
}

/* ----------------------------------
   COMPOSE WALKTHROUGH (SAFE)
---------------------------------- */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    console.log("Compose request received");

    const { walkthroughUrl, avatarUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !avatarUrl) {
      return res.status(400).json({
        ok: false,
        error: "Missing walkthroughUrl or avatarUrl",
      });
    }

    const bucket = process.env.STORAGE_BUCKET || "videos";
    const supabase = getSupabase();

    const tmpDir = "/tmp";
    const id = Date.now();

    const walkthroughPath = path.join(tmpDir, `walk-${id}.mp4`);
    const avatarPath = path.join(tmpDir, `avatar-${id}.mp4`);
    const outputPath = path.join(tmpDir, `final-${id}.mp4`);

    // 1️⃣ Download files
    await downloadToFile(walkthroughUrl, walkthroughPath);
    await downloadToFile(avatarUrl, avatarPath);

    console.log("Downloads complete");

    // 2️⃣ Run FFmpeg (lighter settings for Render stability)
    const args = [
      "-y",
      "-t", String(maxSeconds),
      "-i", walkthroughPath,
      "-i", avatarPath,
      "-filter_complex",
      "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[bg];" +
      "[1:v]scale=iw*0.30:-2,chromakey=0x00FF00:0.18:0.10,format=rgba[fg];" +
      "[bg][fg]overlay=W-w-40:H-h-60",
      "-map", "0:a?",
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      "-pix_fmt", "yuv420p",
      outputPath
    ];

    await runFFmpeg(args);

    console.log("FFmpeg complete");

    // 3️⃣ Upload to Supabase
    const fileBuffer = fs.readFileSync(outputPath);
    const storagePath = `renders/walkthrough-${id}.mp4`;

    const { data, error } = await supabase.storage
      .from(bucket)
      .upload(storagePath, fileBuffer, {
        contentType: "video/mp4",
        upsert: true,
      });

    if (error) throw error;

    const { data: publicUrl } =
      supabase.storage.from(bucket).getPublicUrl(data.path);

    console.log("Upload complete");

    res.json({
      ok: true,
      url: publicUrl.publicUrl,
    });

  } catch (e) {
    console.error("COMPOSE ERROR:", e);
    res.status(500).json({
      ok: false,
      error: String(e),
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
