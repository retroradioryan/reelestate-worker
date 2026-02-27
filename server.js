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
  if (!url || !key) throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  return createClient(url, key);
}

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

async function downloadToFile(fileUrl, outPath) {
  const response = await fetch(fileUrl);
  if (!response.ok) throw new Error(`Download failed: ${response.status} (${fileUrl})`);

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

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/* ----------------------------------
   HEYGEN (Generate + Poll)
---------------------------------- */

/**
 * HeyGen Create a Video:
 * https://api.heygen.com/v2/video/generate :contentReference[oaicite:3]{index=3}
 *
 * NOTE: Header name in their docs is shown under “Credentials” on that page.
 * If yours differs, adjust HEYGEN_HEADERS below accordingly.
 */
function heygenHeaders() {
  const apiKey = mustEnv("HEYGEN_API_KEY");
  // Common pattern is X-Api-Key; adjust if your HeyGen doc shows different.
  return {
    "Content-Type": "application/json",
    "X-Api-Key": apiKey,
  };
}

async function heygenCreateAvatarVideo({ audioUrl, maxSeconds }) {
  const avatarId = mustEnv("HEYGEN_AVATAR_ID");

  // Fast + simple: green background, no overlays, no branding in the composition.
  // Using audio_url as the voice source makes the avatar “narrate” the walkthrough audio. :contentReference[oaicite:4]{index=4}
  const body = {
    video_inputs: [
      {
        character: {
          type: "avatar",
          avatar_id: avatarId,
          avatar_style: "normal",
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
    // Keep it short to your render window (fastest path)
    dimension: { width: 1080, height: 1920 },
    // Some accounts support trimming; if not supported, HeyGen will ignore.
    // You can still trim later in FFmpeg.
    duration: maxSeconds,
  };

  const resp = await fetch("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: heygenHeaders(),
    body: JSON.stringify(body),
  });

  const json = await resp.json();
  if (!resp.ok) throw new Error(`HeyGen create failed: ${resp.status} ${JSON.stringify(json)}`);

  // Typically returns something like { data: { video_id } }
  const videoId =
    json?.data?.video_id || json?.video_id || json?.data?.id || json?.id;

  if (!videoId) throw new Error(`HeyGen create returned no video_id: ${JSON.stringify(json)}`);
  return videoId;
}

async function heygenPollVideoUrl(videoId) {
  // HeyGen “Get Video Status/Details” exists in the Video Generation section. :contentReference[oaicite:5]{index=5}
  // Many accounts use: https://api.heygen.com/v1/video_status.get?video_id=XXXX
  // If your docs show a different status endpoint, swap it here.

  const url = `https://api.heygen.com/v1/video_status.get?video_id=${encodeURIComponent(videoId)}`;

  for (let i = 0; i < 90; i++) { // ~3 mins worst-case at 2s intervals
    const resp = await fetch(url, { headers: heygenHeaders() });
    const json = await resp.json();
    if (!resp.ok) throw new Error(`HeyGen status failed: ${resp.status} ${JSON.stringify(json)}`);

    const status = json?.data?.status || json?.status;
    const videoUrl = json?.data?.video_url || json?.data?.url || json?.video_url || json?.url;

    if (status === "completed" && videoUrl) return videoUrl;
    if (status === "failed") throw new Error(`HeyGen video failed: ${JSON.stringify(json)}`);

    await sleep(2000);
  }

  throw new Error("HeyGen timed out waiting for video to complete");
}

/* ----------------------------------
   HEALTH
---------------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

/* ----------------------------------
   COMPOSE WALKTHROUGH (FAST + CORRECT)
---------------------------------- */

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

    const tmp = "/tmp";
    const id = Date.now();

    const walkPath = path.join(tmp, `walk-${id}.mp4`);
    const audioPath = path.join(tmp, `walk-${id}.m4a`);
    const logoPath = path.join(tmp, `logo-${id}.png`);

    const heygenPath = path.join(tmp, `heygen-${id}.mp4`);
    const mainPath = path.join(tmp, `main-${id}.mp4`);
    const introPath = path.join(tmp, `intro-${id}.mp4`);
    const outroPath = path.join(tmp, `outro-${id}.mp4`);
    const finalPath = path.join(tmp, `final-${id}.mp4`);

    // 1) Download walkthrough + logo
    await downloadToFile(walkthroughUrl, walkPath);
    await downloadToFile(logoUrl, logoPath);

    // 2) Extract walkthrough audio (fast, no re-encode if possible)
    await runFFmpeg([
      "-y",
      "-i", walkPath,
      "-t", String(maxSeconds),
      "-vn",
      "-acodec", "copy",
      audioPath,
    ]);

    // 3) Upload audio to Supabase to get a public URL (HeyGen needs a reachable URL)
    const audioBuf = fs.readFileSync(audioPath);
    const audioStoragePath = `renders/audio-${id}.m4a`;

    const upAudio = await supabase.storage
      .from(bucket)
      .upload(audioStoragePath, audioBuf, {
        contentType: "audio/mp4",
        upsert: true,
      });

    if (upAudio.error) throw upAudio.error;

    const { data: audioPublic } = supabase.storage.from(bucket).getPublicUrl(upAudio.data.path);
    const audioUrl = audioPublic.publicUrl;

    // 4) HeyGen: create avatar video using the walkthrough audio as the voice track
    const heygenVideoId = await heygenCreateAvatarVideo({ audioUrl, maxSeconds });
    const heygenVideoUrl = await heygenPollVideoUrl(heygenVideoId);

    // 5) Download the HeyGen result mp4
    await downloadToFile(heygenVideoUrl, heygenPath);

    // 6) Compose main video:
    //    - Fix orientation FIRST (WhatsApp often has rotate metadata; transpose=1 makes it upright)
    //    - Scale/crop to 1080x1920
    //    - Chroma key HeyGen green
    //    - Use HeyGen audio (it’s the walkthrough audio, but timed to avatar)
    const filter =
      // Walkthrough: rotate -> fill 1080x1920
      "[0:v]transpose=1,scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1[vbg];" +
      // HeyGen: key green + scale avatar up a bit + add light shadow so it’s not “floating”
      "[1:v]scale=iw*0.48:-2,chromakey=0x00FF00:0.18:0.08,format=rgba[fg];" +
      "[fg]split[fg1][fg2];" +
      "[fg1]colorchannelmixer=aa=0.55,boxblur=10:4[shadow];" +
      "[vbg][shadow]overlay=W-w-72:H-h-110[bgshadow];" +
      "[bgshadow][fg2]overlay=W-w-64:H-h-102[outv]";

    await runFFmpeg([
      "-y",
      "-t", String(maxSeconds),
      "-i", walkPath,
      "-i", heygenPath,
      "-filter_complex", filter,
      "-map", "[outv]",
      "-map", "1:a?",
      "-c:v", "libx264",
      "-preset", "ultrafast",
      "-crf", "28",
      "-pix_fmt", "yuv420p",
      "-c:a", "aac",
      "-b:a", "128k",
      mainPath,
    ]);

    // 7) Intro/outro (FULL SCREEN logo, 1.5s fade)
    const introOutroVF =
      "scale=900:-1,format=rgba," +
      "fade=t=in:st=0:d=0.25,fade=t=out:st=1.25:d=0.25," +
      "pad=1080:1920:(1080-iw)/2:(1920-ih)/2:color=black";

    await runFFmpeg([
      "-y",
      "-loop", "1",
      "-i", logoPath,
      "-t", "1.5",
      "-vf", introOutroVF,
      "-c:v", "libx264",
      "-preset", "ultrafast",
      "-crf", "28",
      "-pix_fmt", "yuv420p",
      introPath,
    ]);

    await runFFmpeg([
      "-y",
      "-loop", "1",
      "-i", logoPath,
      "-t", "1.5",
      "-vf", introOutroVF,
      "-c:v", "libx264",
      "-preset", "ultrafast",
      "-crf", "28",
      "-pix_fmt", "yuv420p",
      outroPath,
    ]);

    // 8) Concat intro + main + outro
    const listPath = path.join(tmp, `list-${id}.txt`);
    fs.writeFileSync(listPath, `file '${introPath}'\nfile '${mainPath}'\nfile '${outroPath}'\n`);

    await runFFmpeg([
      "-y",
      "-f", "concat",
      "-safe", "0",
      "-i", listPath,
      "-c:v", "libx264",
      "-preset", "ultrafast",
      "-crf", "28",
      "-pix_fmt", "yuv420p",
      "-c:a", "aac",
      "-b:a", "128k",
      finalPath,
    ]);

    // 9) Upload final mp4
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
