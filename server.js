import express from "express";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();

/* ----------------------------------
   BODY LIMITS
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
  if (!response.ok) throw new Error(`Download failed: ${response.status}`);

  await new Promise((resolve, reject) => {
    const fileStream = fs.createWriteStream(outPath);
    response.body.pipe(fileStream);
    response.body.on("error", reject);
    fileStream.on("finish", resolve);
  });

  console.log("Downloaded:", outPath);
}

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    console.log("Running FFmpeg...");

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
   HEALTH
---------------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

/* ----------------------------------
   COMPOSE WALKTHROUGH (CORPORATE PRO)
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
       2️⃣ CORPORATE PREMIUM FILTER STACK
    ---------------------------------- */

    const filterComplex =
      "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1,format=yuv420p[vbg];" +

      // Subtle vignette
      "[vbg]vignette=PI/5[vbg2];" +

      // Avatar scaling + clean chroma
      "[1:v]scale=iw*0.38:-2,chromakey=0x00FF00:0.16:0.08,format=rgba[fg];" +

      // Drop shadow layer
      "[fg]split[fg1][fg2];" +
      "[fg1]colorchannelmixer=aa=0.35,boxblur=12:4[shadow];" +

      // Place shadow
      "[vbg2][shadow]overlay=W-w-88:H-h-148[bgshadow];" +

      // Place avatar
      "[bgshadow][fg2]overlay=W-w-80:H-h-140[withavatar];" +

      // Subtle frame border
      "[withavatar]drawbox=x=0:y=0:w=iw:h=ih:color=white@0.05:t=20[framed];" +

      // Clean lower third panel
      "[framed]drawbox=x=0:y=ih-260:w=iw:h=200:color=black@0.45:t=fill[outv]";

    const ffmpegArgs = [
      "-y",
      "-t", String(maxSeconds),
      "-i", walkthroughPath,
      "-i", avatarPath,
      "-filter_complex", filterComplex,
      "-map", "[outv]",
      "-map", "1:a?", // Use avatar audio only
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      "-pix_fmt", "yuv420p",
      "-c:a", "aac",
      "-b:a", "128k",
      outputPath
    ];

    await runFFmpeg(ffmpegArgs);

    /* ----------------------------------
       3️⃣ UPLOAD FINAL
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
