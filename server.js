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
   COMPOSE WALKTHROUGH (PRODUCTION)
---------------------------------- */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const { walkthroughUrl, avatarUrl, logoUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !avatarUrl || !logoUrl) {
      return res.status(400).json({
        ok: false,
        error: "walkthroughUrl, avatarUrl and logoUrl required"
      });
    }

    const supabase = getSupabase();
    const bucket = process.env.STORAGE_BUCKET || "videos";

    const tmp = "/tmp";
    const id = Date.now();

    const walkPath = path.join(tmp, `walk-${id}.mp4`);
    const avatarPath = path.join(tmp, `avatar-${id}.mp4`);
    const logoPath = path.join(tmp, `logo-${id}.png`);

    const mainPath = path.join(tmp, `main-${id}.mp4`);
    const introPath = path.join(tmp, `intro-${id}.mp4`);
    const outroPath = path.join(tmp, `outro-${id}.mp4`);
    const finalPath = path.join(tmp, `final-${id}.mp4`);

    /* ----------------------------------
       DOWNLOAD ASSETS
    ---------------------------------- */

    await downloadToFile(walkthroughUrl, walkPath);
    await downloadToFile(avatarUrl, avatarPath);
    await downloadToFile(logoUrl, logoPath);

    /* ----------------------------------
       CREATE MAIN VIDEO
    ---------------------------------- */

    const filter =
      "[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1[vbg];" +
      "[1:v]scale=iw*0.42:-2,chromakey=0x00FF00:0.22:0.06,format=rgba[fg];" +
      "[fg]split[fg1][fg2];" +
      "[fg1]colorchannelmixer=aa=0.5,boxblur=8:4[shadow];" +
      "[vbg][shadow]overlay=W-w-86:H-h-150[bgshadow];" +
      "[bgshadow][fg2]overlay=W-w-80:H-h-140[outv]";

    await runFFmpeg([
      "-y",
      "-t", String(maxSeconds),
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
      "-b:a", "128k",
      mainPath
    ]);

    /* ----------------------------------
       CREATE INTRO (1.5 sec fade)
    ---------------------------------- */

    await runFFmpeg([
      "-y",
      "-loop", "1",
      "-i", logoPath,
      "-t", "1.5",
      "-vf",
      "scale=800:-1,format=rgba,fade=t=in:st=0:d=0.4,fade=t=out:st=1.1:d=0.4," +
      "pad=1080:1920:(1080-iw)/2:(1920-ih)/2:color=black",
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      introPath
    ]);

    /* ----------------------------------
       CREATE OUTRO
    ---------------------------------- */

    await runFFmpeg([
      "-y",
      "-loop", "1",
      "-i", logoPath,
      "-t", "1.5",
      "-vf",
      "scale=800:-1,format=rgba,fade=t=in:st=0:d=0.4,fade=t=out:st=1.1:d=0.4," +
      "pad=1080:1920:(1080-iw)/2:(1920-ih)/2:color=black",
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      outroPath
    ]);

    /* ----------------------------------
       CONCAT ALL
    ---------------------------------- */

    const listPath = path.join(tmp, `list-${id}.txt`);
    fs.writeFileSync(listPath,
      `file '${introPath}'\nfile '${mainPath}'\nfile '${outroPath}'`
    );

    await runFFmpeg([
      "-y",
      "-f", "concat",
      "-safe", "0",
      "-i", listPath,
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      "-c:a", "aac",
      "-b:a", "128k",
      finalPath
    ]);

    /* ----------------------------------
       UPLOAD
    ---------------------------------- */

    const buffer = fs.readFileSync(finalPath);
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

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Worker running on port ${PORT}`);
});
