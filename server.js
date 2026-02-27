import express from "express";
import { execSync } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

/* ----------------------------------
   HELPERS
---------------------------------- */

function getSupabase() {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) {
    throw new Error("Missing env vars: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
  return createClient(url, key);
}

// Stream download to disk (memory safe)
async function downloadToFile(fileUrl, outPath) {
  const r = await fetch(fileUrl);
  if (!r.ok) throw new Error(`Failed download (${r.status}): ${fileUrl}`);

  await new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(outPath);
    r.body.pipe(ws);
    r.body.on("error", reject);
    ws.on("finish", resolve);
  });
}

/* ----------------------------------
   BASIC HEALTH ROUTES
---------------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

/* ----------------------------------
   CHECK FFMPEG
---------------------------------- */

app.get("/ffmpeg", (req, res) => {
  try {
    const out = execSync("ffmpeg -version").toString();
    res.type("text").send(out);
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: "ffmpeg not found",
      details: String(e),
    });
  }
});

/* ----------------------------------
   CHECK SUPABASE CONNECTION
---------------------------------- */

app.get("/supabase", async (req, res) => {
  try {
    const supabase = getSupabase();
    const { data, error } = await supabase.storage.listBuckets();
    if (error) throw error;

    res.json({ ok: true, buckets: data?.map((b) => b.name) || [] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/* ----------------------------------
   RENDER TEST (MEMORY SAFE STREAMING)
   - downloads small sample mp4
   - trims 3 seconds (ultrafast)
   - uploads to Supabase bucket
---------------------------------- */

app.post("/render-test", async (req, res) => {
  try {
    const bucket = process.env.STORAGE_BUCKET || "videos";
    const supabase = getSupabase();

    const sampleUrl =
      req.body?.sampleUrl ||
      "https://storage.googleapis.com/gtv-videos-bucket/sample/ForBiggerEscapes.mp4";

    const tmpDir = "/tmp";
    const id = Date.now();
    const inputPath = path.join(tmpDir, `in-${id}.mp4`);
    const outputPath = path.join(tmpDir, `out-${id}.mp4`);

    // 1) Stream download to disk (no memory spike)
    await downloadToFile(sampleUrl, inputPath);

    // 2) FFmpeg (lightweight encode)
    execSync(
      `ffmpeg -y -i "${inputPath}" -t 3 -c:v libx264 -preset ultrafast -crf 32 -an "${outputPath}"`,
      { stdio: "ignore" }
    );

    // 3) Upload
    const file = fs.readFileSync(outputPath);
    const filePath = `tests/test-${id}.mp4`;

    const { data, error } = await supabase.storage.from(bucket).upload(filePath, file, {
      contentType: "video/mp4",
      upsert: true,
    });
    if (error) throw error;

    const { data: publicUrl } = supabase.storage.from(bucket).getPublicUrl(data.path);

    res.json({
      ok: true,
      bucket,
      path: data.path,
      url: publicUrl.publicUrl,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/* ----------------------------------
   COMPOSE WALKTHROUGH (PRO MODE)
   - walkthrough MP4 + avatar green screen MP4
   - outputs vertical 1080x1920
   - avatar PiP bottom-right
   - optional logo intro/outro as images
---------------------------------- */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const {
      walkthroughUrl,
      avatarUrl,          // green screen MP4
      logoIntroUrl,       // optional PNG/JPG
      logoOutroUrl,       // optional PNG/JPG
      titleText,          // optional (top-left)
      maxSeconds          // optional override
    } = req.body || {};

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
    const composedPath = path.join(tmpDir, `composed-${id}.mp4`);
    const finalPath = path.join(tmpDir, `final-${id}.mp4`);

    const introImgPath = logoIntroUrl ? path.join(tmpDir, `intro-${id}.img`) : null;
    const outroImgPath = logoOutroUrl ? path.join(tmpDir, `outro-${id}.img`) : null;

    // 1) Download assets (streaming)
    await downloadToFile(walkthroughUrl, walkthroughPath);
    await downloadToFile(avatarUrl, avatarPath);
    if (logoIntroUrl) await downloadToFile(logoIntroUrl, introImgPath);
    if (logoOutroUrl) await downloadToFile(logoOutroUrl, outroImgPath);

    // 2) Compose avatar PiP bottom-right
    const limit = Number(maxSeconds || process.env.MAX_WALKTHROUGH_SECONDS || 60);

    // Avatar width fraction (0.30 = 30% of the canvas width)
    const avatarWidthFrac = Number(process.env.DEFAULT_AVATAR_SCALE || 0.30);

    // Bottom-right with padding
    const xExpr = process.env.DEFAULT_AVATAR_X || "W-w-40";
    const yExpr = process.env.DEFAULT_AVATAR_Y || "H-h-60";

    // Optional title text (top-left). Escape for ffmpeg drawtext.
    const safeTitle = (titleText || "")
      .replace(/\\/g, "\\\\")
      .replace(/:/g, "\\:")
      .replace(/'/g, "\\'");

    const drawText = titleText
      ? `,drawtext=text='${safeTitle}':x=40:y=80:fontsize=52:fontcolor=white:box=1:boxcolor=black@0.35:boxborderw=18`
      : "";

    execSync(
      `ffmpeg -y -t ${limit} -i "${walkthroughPath}" -i "${avatarPath}" ` +
        `-filter_complex ` +
        `"` +
        // Base: convert walkthrough to vertical 1080x1920
        `[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1${drawText}[base];` +
        // Avatar: scale + chromakey out green
        `[1:v]scale=iw*${avatarWidthFrac}:-2,chromakey=0x00FF00:0.18:0.10,format=rgba[av];` +
        // Overlay: bottom-right
        `[base][av]overlay=${xExpr}:${yExpr}:format=auto[outv]` +
        `"` +
        ` -map "[outv]" -map 0:a? ` +
        `-c:v libx264 -preset veryfast -crf 23 -c:a aac -b:a 128k "${composedPath}"`,
      { stdio: "ignore" }
    );

    // 3) Optional intro/outro slates (images → 1.5s MP4)
    const parts = [];

    if (introImgPath) {
      const introMp4 = path.join(tmpDir, `intro-${id}.mp4`);
      execSync(
        `ffmpeg -y -loop 1 -i "${introImgPath}" -t 1.5 ` +
          `-vf "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,format=yuv420p" ` +
          `-c:v libx264 -preset veryfast -crf 24 "${introMp4}"`,
        { stdio: "ignore" }
      );
      parts.push(introMp4);
    }

    parts.push(composedPath);

    if (outroImgPath) {
      const outroMp4 = path.join(tmpDir, `outro-${id}.mp4`);
      execSync(
        `ffmpeg -y -loop 1 -i "${outroImgPath}" -t 1.5 ` +
          `-vf "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,format=yuv420p" ` +
          `-c:v libx264 -preset veryfast -crf 24 "${outroMp4}"`,
        { stdio: "ignore" }
      );
      parts.push(outroMp4);
    }

    // Concatenate (re-encode for compatibility)
    if (parts.length === 1) {
      fs.copyFileSync(parts[0], finalPath);
    } else {
      const listPath = path.join(tmpDir, `list-${id}.txt`);
      fs.writeFileSync(listPath, parts.map((p) => `file '${p}'`).join("\n"));

      execSync(
        `ffmpeg -y -f concat -safe 0 -i "${listPath}" ` +
          `-c:v libx264 -preset veryfast -crf 23 -c:a aac -b:a 128k "${finalPath}"`,
        { stdio: "ignore" }
      );
    }

    // 4) Upload final
    const outBuf = fs.readFileSync(finalPath);
    const outStoragePath = `renders/walkthrough-${id}.mp4`;

    const { data, error } = await supabase.storage.from(bucket).upload(outStoragePath, outBuf, {
      contentType: "video/mp4",
      upsert: true,
    });
    if (error) throw error;

    const { data: publicUrl } = supabase.storage.from(bucket).getPublicUrl(data.path);

    res.json({
      ok: true,
      path: data.path,
      url: publicUrl.publicUrl,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/* ----------------------------------
   START SERVER
---------------------------------- */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`Worker running on port ${PORT}`);
});
