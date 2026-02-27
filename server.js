import express from "express";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();

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
  const response = await fetch(fileUrl);
  if (!response.ok) throw new Error(`Download failed: ${response.status}`);

  await new Promise((resolve, reject) => {
    const stream = fs.createWriteStream(outPath);
    response.body.pipe(stream);
    response.body.on("error", reject);
    stream.on("finish", resolve);
  });
}

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);

    ff.stderr.on("data", (d) => console.log(d.toString()));

    ff.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`FFmpeg exited with code ${code}`));
    });
  });
}

/* ----------------------------------
   HEALTH
---------------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

/* ----------------------------------
   FAST COMPOSE WALKTHROUGH
---------------------------------- */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const { walkthroughUrl, avatarUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !avatarUrl) {
      return res.status(400).json({
        ok: false,
        error: "walkthroughUrl and avatarUrl required"
      });
    }

    const supabase = getSupabase();
    const bucket = process.env.STORAGE_BUCKET || "videos";

    const tmp = "/tmp";
    const id = Date.now();

    const walkPath = path.join(tmp, `walk-${id}.mp4`);
    const avatarPath = path.join(tmp, `avatar-${id}.mp4`);
    const outputPath = path.join(tmp, `final-${id}.mp4`);

    /* ----------------------------------
       DOWNLOAD
    ---------------------------------- */

    await downloadToFile(walkthroughUrl, walkPath);
    await downloadToFile(avatarUrl, avatarPath);

    /* ----------------------------------
       FAST FILTER STACK
       - Vertical
       - Larger avatar
       - Clean chroma
       - Avatar audio only
    ---------------------------------- */

    const filter =
      "[0:v]transpose=1,scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[bg];" +
      "[1:v]scale=iw*0.45:-2,chromakey=0x00FF00:0.20:0.08[fg];" +
      "[bg][fg]overlay=W-w-60:H-h-120[outv]";

    await runFFmpeg([
      "-y",
      "-t", String(maxSeconds),
      "-i", walkPath,
      "-i", avatarPath,
      "-filter_complex", filter,
      "-map", "[outv]",
      "-map", "1:a?",
      "-c:v", "libx264",
      "-preset", "ultrafast",   // 🔥 fastest possible
      "-crf", "28",             // slightly lower quality for speed
      "-pix_fmt", "yuv420p",
      "-c:a", "aac",
      "-b:a", "128k",
      outputPath
    ]);

    /* ----------------------------------
       UPLOAD
    ---------------------------------- */

    const buffer = fs.readFileSync(outputPath);
    const storagePath = `renders/walkthrough-${id}.mp4`;

    const { data, error } = await supabase.storage
      .from(bucket)
      .upload(storagePath, buffer, {
        contentType: "video/mp4",
        upsert: true
      });

    if (error) throw error;

    const { data: publicUrl } =
      supabase.storage.from(bucket).getPublicUrl(data.path);

    res.json({
      ok: true,
      url: publicUrl.publicUrl
    });

  } catch (err) {
    console.error("ERROR:", err);
    res.status(500).json({ ok: false, error: String(err) });
  }
});

/* ----------------------------------
   START SERVER
---------------------------------- */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`Worker running on port ${PORT}`);
});
