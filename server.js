
import express from "express";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

/* -----------------------------
  HELPERS
------------------------------*/

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function getSupabase() {
  const url = mustEnv("SUPABASE_URL");
  const key = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
  return createClient(url, key);
}

async function downloadToFile(fileUrl, outPath) {
  const resp = await fetch(fileUrl);
  if (!resp.ok) throw new Error(`Download failed: ${resp.status} (${fileUrl})`);

  await new Promise((resolve, reject) => {
    const stream = fs.createWriteStream(outPath);
    resp.body.pipe(stream);
    resp.body.on("error", reject);
    stream.on("finish", resolve);
  });
}

function runCmd(bin, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(bin, args);
    let stdout = "";
    p.stdout.on("data", (d) => (stdout += d.toString()));
    p.stderr.on("data", (d) => console.log(d.toString()));
    p.on("close", (code) => {
      if (code === 0) resolve(stdout);
      else reject(new Error(`${bin} exited with code ${code}`));
    });
  });
}

function runFFmpeg(args) {
  return runCmd("ffmpeg", args);
}

async function getRotationDegrees(videoPath) {
  // Reads rotation metadata if present (e.g. WhatsApp videos)
  try {
    const out = await runCmd("ffprobe", [
      "-v",
      "error",
      "-select_streams",
      "v:0",
      "-show_entries",
      "stream_tags=rotate",
      "-of",
      "default=nw=1:nk=1",
      videoPath,
    ]);
    const s = (out || "").trim();
    const deg = parseInt(s, 10);
    if (Number.isFinite(deg)) return deg;
    return 0;
  } catch {
    return 0;
  }
}

function heygenHeaders() {
  // Docs: API uses X-API-KEY header. :contentReference[oaicite:0]{index=0}
  const apiKey = mustEnv("HEYGEN_API_KEY");
  return { "Content-Type": "application/json", "X-API-KEY": apiKey };
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/* -----------------------------
  HEYGEN: CREATE + POLL
------------------------------*/

async function heygenCreateVideoFromAudio({ avatarId, audioUrl, maxSeconds }) {
  // Create a Video endpoint exists in HeyGen API Reference (Video Generation). :contentReference[oaicite:1]{index=1}
  // This payload format matches HeyGen “video_inputs” structure used in v2 generation flows.
  const body = {
    video_inputs: [
      {
        character: {
          type: "avatar",
          avatar_id: avatarId,
        },
        voice: {
          type: "audio",
          audio_url: audioUrl,
        },
        background: {
          type: "color",
          value: "#00FF00",
        },
      },
    ],
    dimension: { width: 1080, height: 1920 },
    duration: Number(maxSeconds),
  };

  const resp = await fetch("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: heygenHeaders(),
    body: JSON.stringify(body),
  });

  const json = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(`HeyGen create failed: ${resp.status} ${JSON.stringify(json)}`);

  const videoId = json?.data?.video_id || json?.data?.id || json?.video_id || json?.id;
  if (!videoId) throw new Error(`HeyGen returned no video_id: ${JSON.stringify(json)}`);
  return videoId;
}

async function heygenPollVideoUrl(videoId) {
  // Get Video Status/Details endpoint listed in docs. :contentReference[oaicite:2]{index=2}
  // Many 404s happen when hitting the wrong path — this is the documented one.
  const statusUrl = `https://api.heygen.com/v1/video_status.get?video_id=${encodeURIComponent(videoId)}`;

  for (let i = 0; i < 120; i++) {
    const resp = await fetch(statusUrl, { headers: heygenHeaders() });
    const json = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(`HeyGen status failed: ${resp.status} ${JSON.stringify(json)}`);

    const status = json?.data?.status || json?.status;
    const videoUrl = json?.data?.video_url || json?.data?.url || json?.video_url || json?.url;

    if (status === "completed" && videoUrl) return videoUrl;
    if (status === "failed") throw new Error(`HeyGen failed: ${JSON.stringify(json)}`);

    await sleep(2000);
  }

  throw new Error("HeyGen timed out waiting for completion");
}

/* -----------------------------
  HEALTH
------------------------------*/

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

/* -----------------------------
  COMPOSE WALKTHROUGH (FAST)
  - walkthroughUrl (video)
  - logoUrl (png)
  - maxSeconds (default 30)
  Uses env:
   - HEYGEN_API_KEY
   - HEYGEN_AVATAR_ID  (IMPORTANT)
   - SUPABASE_URL
   - SUPABASE_SERVICE_ROLE_KEY
   - STORAGE_BUCKET (optional, default "videos")
------------------------------*/

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const { walkthroughUrl, logoUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !logoUrl) {
      return res.status(400).json({
        ok: false,
        error: "walkthroughUrl and logoUrl required",
      });
    }

    const supabase = getSupabase();
    const bucket = process.env.STORAGE_BUCKET || "videos";
    const avatarId = mustEnv("HEYGEN_AVATAR_ID");

    const tmp = "/tmp";
    const id = Date.now();

    const walkPath = path.join(tmp, `walk-${id}.mp4`);
    const logoPath = path.join(tmp, `logo-${id}.png`);

    const audioPath = path.join(tmp, `audio-${id}.m4a`);
    const heygenPath = path.join(tmp, `heygen-${id}.mp4`);

    const mainPath = path.join(tmp, `main-${id}.mp4`);
    const introPath = path.join(tmp, `intro-${id}.mp4`);
    const outroPath = path.join(tmp, `outro-${id}.mp4`);
    const finalPath = path.join(tmp, `final-${id}.mp4`);
    const listPath = path.join(tmp, `list-${id}.txt`);

    // 1) Download assets
    await downloadToFile(walkthroughUrl, walkPath);
    await downloadToFile(logoUrl, logoPath);

    // 2) Extract audio (re-encode to AAC for max compatibility with HeyGen)
    await runFFmpeg([
      "-y",
      "-i",
      walkPath,
      "-t",
      String(maxSeconds),
      "-vn",
      "-ac",
      "2",
      "-ar",
      "48000",
      "-c:a",
      "aac",
      "-b:a",
      "128k",
      audioPath,
    ]);

    // 3) Upload audio to Supabase → public URL for HeyGen
    const audioBuf = fs.readFileSync(audioPath);
    const audioStoragePath = `renders/audio-${id}.m4a`;

    const upAudio = await supabase.storage.from(bucket).upload(audioStoragePath, audioBuf, {
      contentType: "audio/mp4",
      upsert: true,
    });
    if (upAudio.error) throw upAudio.error;

    const { data: audioPublic } = supabase.storage.from(bucket).getPublicUrl(upAudio.data.path);
    const audioUrl = audioPublic.publicUrl;

    // 4) HeyGen: create avatar video using that audio (green bg)
    const videoId = await heygenCreateVideoFromAudio({ avatarId, audioUrl, maxSeconds });
    const heygenVideoUrl = await heygenPollVideoUrl(videoId);

    // 5) Download HeyGen result mp4
    await downloadToFile(heygenVideoUrl, heygenPath);

    // 6) Fix orientation based on rotation metadata
    const rot = await getRotationDegrees(walkPath);
    // If WhatsApp says -90, that’s typically 270; we need a transpose.
    // We only apply when needed (prevents over-rotating “normal” videos).
    let rotFilter = "";
    if (rot === 90) rotFilter = "transpose=1,";
    else if (rot === -90 || rot === 270) rotFilter = "transpose=2,";
    else if (rot === 180 || rot === -180) rotFilter = "hflip,vflip,";

    // 7) Compose main (fast settings)
    // - walkthrough becomes full 1080x1920
    // - avatar key is stronger and fg alpha forced to 1 (less transparent)
    // - shadow for grounding (less “floating”)
    const filter =
      `[0:v]${rotFilter}scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1[vbg];` +
      `[1:v]scale=iw*0.52:-2,chromakey=0x00FF00:0.12:0.05,format=rgba,colorchannelmixer=aa=1.0[fg];` +
      `[fg]split[fgA][fgB];` +
      `[fgA]colorchannelmixer=aa=0.55,boxblur=14:6[shadow];` +
      `[vbg][shadow]overlay=W-w-78:H-h-118[bgshadow];` +
      `[bgshadow][fgB]overlay=W-w-70:H-h-110[outv]`;

    await runFFmpeg([
      "-y",
      "-t",
      String(maxSeconds),
      "-i",
      walkPath,
      "-i",
      heygenPath,
      "-filter_complex",
      filter,
      "-map",
      "[outv]",
      "-map",
      "1:a?",
      "-c:v",
      "libx264",
      "-preset",
      "ultrafast",
      "-crf",
      "30",
      "-pix_fmt",
      "yuv420p",
      "-c:a",
      "aac",
      "-b:a",
      "128k",
      mainPath,
    ]);

    // 8) Intro/outro full screen logo (1.5s)
    // Make logo BIG and centered; black background; quick fade
    const introOutroVF =
      "scale=1080:-1:force_original_aspect_ratio=decrease," +
      "pad=1080:1920:(1080-iw)/2:(1920-ih)/2:color=black," +
      "fade=t=in:st=0:d=0.25,fade=t=out:st=1.25:d=0.25";

    await runFFmpeg([
      "-y",
      "-loop",
      "1",
      "-i",
      logoPath,
      "-t",
      "1.5",
      "-vf",
      introOutroVF,
      "-c:v",
      "libx264",
      "-preset",
      "ultrafast",
      "-crf",
      "30",
      "-pix_fmt",
      "yuv420p",
      introPath,
    ]);

    await runFFmpeg([
      "-y",
      "-loop",
      "1",
      "-i",
      logoPath,
      "-t",
      "1.5",
      "-vf",
      introOutroVF,
      "-c:v",
      "libx264",
      "-preset",
      "ultrafast",
      "-crf",
      "30",
      "-pix_fmt",
      "yuv420p",
      outroPath,
    ]);

    // 9) Concat intro + main + outro (re-encode once, fastest)
    fs.writeFileSync(listPath, `file '${introPath}'\nfile '${mainPath}'\nfile '${outroPath}'\n`);

    await runFFmpeg([
      "-y",
      "-f",
      "concat",
      "-safe",
      "0",
      "-i",
      listPath,
      "-c:v",
      "libx264",
      "-preset",
      "ultrafast",
      "-crf",
      "30",
      "-pix_fmt",
      "yuv420p",
      "-c:a",
      "aac",
      "-b:a",
      "128k",
      finalPath,
    ]);

    // 10) Upload final mp4
    const buffer = fs.readFileSync(finalPath);
    const storagePath = `renders/walkthrough-${id}.mp4`;

    const up = await supabase.storage.from(bucket).upload(storagePath, buffer, {
      contentType: "video/mp4",
      upsert: true,
    });
    if (up.error) throw up.error;

    const { data: publicUrl } = supabase.storage.from(bucket).getPublicUrl(up.data.path);

    res.json({ ok: true, url: publicUrl.publicUrl });
  } catch (err) {
    console.error("ERROR:", err);
    res.status(500).json({ ok: false, error: String(err) });
  }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`Worker running on port ${PORT}`));
