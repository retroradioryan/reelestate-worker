import express from "express";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));

/* -----------------------------
   ENV + SUPABASE
----------------------------- */

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

function heygenHeaders() {
  return {
    "Content-Type": "application/json",
    "X-Api-Key": mustEnv("HEYGEN_API_KEY"),
  };
}

const BUCKET = process.env.STORAGE_BUCKET || "videos";

/* -----------------------------
   HELPERS
----------------------------- */

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

function runFFprobeJSON(args) {
  return new Promise((resolve, reject) => {
    const fp = spawn("ffprobe", args);
    let out = "";
    let err = "";
    fp.stdout.on("data", (d) => (out += d.toString()));
    fp.stderr.on("data", (d) => (err += d.toString()));
    fp.on("close", (code) => {
      if (code !== 0) return reject(new Error(`ffprobe failed: ${err}`));
      try {
        resolve(JSON.parse(out));
      } catch (e) {
        reject(e);
      }
    });
  });
}

async function detectRotationDegrees(videoPath) {
  // Reads rotation metadata if present (WhatsApp often stores -90)
  try {
    const json = await runFFprobeJSON([
      "-v",
      "error",
      "-print_format",
      "json",
      "-show_streams",
      videoPath,
    ]);

    const vStream = (json.streams || []).find((s) => s.codec_type === "video");
    const tags = vStream?.tags || {};
    const rotateTag = tags.rotate;

    if (rotateTag !== undefined) {
      const n = parseInt(rotateTag, 10);
      if (!Number.isNaN(n)) return n;
    }

    // Sometimes stored in side_data_list
    const side = vStream?.side_data_list || [];
    for (const sd of side) {
      if (sd.rotation !== undefined) {
        const n = parseInt(sd.rotation, 10);
        if (!Number.isNaN(n)) return n;
      }
    }

    return 0;
  } catch {
    return 0;
  }
}

function rotationFilter(deg) {
  // Convert common rotate metadata into actual pixels rotation
  // We zero metadata later by re-encode anyway.
  if (deg === 90) return "transpose=1"; // clockwise
  if (deg === -90 || deg === 270) return "transpose=2"; // counterclockwise
  if (deg === 180 || deg === -180) return "hflip,vflip";
  return ""; // no rotation needed
}

/* -----------------------------
   HEYGEN: CREATE VIDEO (async)
----------------------------- */

async function heygenCreateVideo({ audioUrl, maxSeconds, callbackUrl }) {
  const avatarId = mustEnv("HEYGEN_AVATAR_ID");

  // NOTE: HeyGen supports callback/webhook on many plans.
  // If your account uses a different field name, this is the ONLY line to adjust.
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
          audio_url: audioUrl, // avatar will speak this audio
        },
        background: { type: "color", value: "#00FF00" },
      },
    ],
    dimension: { width: 1080, height: 1920 },
    duration: maxSeconds,
    callback_url: callbackUrl,
  };

  const resp = await fetch("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: heygenHeaders(),
    body: JSON.stringify(body),
  });

  const json = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(`HeyGen create failed: ${resp.status} ${JSON.stringify(json)}`);
  }

  const videoId = json?.data?.video_id || json?.video_id || json?.data?.id || json?.id;
  if (!videoId) throw new Error(`HeyGen create returned no video_id: ${JSON.stringify(json)}`);
  return videoId;
}

/* -----------------------------
   HEALTH
----------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

/* -----------------------------
   START JOB (returns immediately)
----------------------------- */

app.post("/compose-walkthrough", async (req, res) => {
  const supabase = getSupabase();

  try {
    const { walkthroughUrl, logoUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !logoUrl) {
      return res.status(400).json({
        ok: false,
        error: "walkthroughUrl and logoUrl required",
      });
    }

    const baseUrl = mustEnv("PUBLIC_BASE_URL");
    const secret = mustEnv("HEYGEN_WEBHOOK_SECRET");

    // 1) Create job row
    const { data: job, error: jobErr } = await supabase
      .from("render_jobs")
      .insert({
        status: "queued",
        walkthrough_url: walkthroughUrl,
        logo_url: logoUrl,
        max_seconds: maxSeconds,
      })
      .select("*")
      .single();

    if (jobErr) throw jobErr;

    const jobId = job.id;

    // 2) Download walkthrough to extract audio
    const tmp = "/tmp";
    const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
    const audioPath = path.join(tmp, `audio-${jobId}.m4a`);

    await downloadToFile(walkthroughUrl, walkPath);

    // Extract audio (fast)
    await runFFmpeg([
      "-y",
      "-i",
      walkPath,
      "-t",
      String(maxSeconds),
      "-vn",
      "-c:a",
      "aac",
      "-b:a",
      "128k",
      audioPath,
    ]);

    // 3) Upload audio so HeyGen can fetch it
    const audioBuf = fs.readFileSync(audioPath);
    const audioStoragePath = `renders/audio-${jobId}.m4a`;

    const upAudio = await supabase.storage.from(BUCKET).upload(audioStoragePath, audioBuf, {
      contentType: "audio/mp4",
      upsert: true,
    });
    if (upAudio.error) throw upAudio.error;

    const { data: audioPublic } = supabase.storage.from(BUCKET).getPublicUrl(upAudio.data.path);

    // 4) Tell HeyGen to generate + callback to us
    const callbackUrl = `${baseUrl}/heygen-callback?token=${encodeURIComponent(secret)}&job_id=${jobId}`;

    const heygenVideoId = await heygenCreateVideo({
      audioUrl: audioPublic.publicUrl,
      maxSeconds,
      callbackUrl,
    });

    await supabase.from("render_jobs").update({
      status: "heygen_requested",
      audio_storage_path: audioStoragePath,
      audio_public_url: audioPublic.publicUrl,
      heygen_video_id: heygenVideoId,
    }).eq("id", jobId);

    // Return immediately (NO TIMEOUT)
    return res.json({
      ok: true,
      job_id: jobId,
      heygen_video_id: heygenVideoId,
      status_url: `${baseUrl}/job/${jobId}`,
    });
  } catch (err) {
    console.error("START JOB ERROR:", err);
    return res.status(500).json({ ok: false, error: String(err) });
  }
});

/* -----------------------------
   CHECK JOB STATUS
----------------------------- */

app.get("/job/:id", async (req, res) => {
  const supabase = getSupabase();
  const id = Number(req.params.id);

  const { data, error } = await supabase.from("render_jobs").select("*").eq("id", id).single();
  if (error) return res.status(404).json({ ok: false, error: String(error) });

  res.json({ ok: true, job: data });
});

/* -----------------------------
   HEYGEN CALLBACK (does the heavy work)
   This is where we compose + upload final.
----------------------------- */

app.post("/heygen-callback", async (req, res) => {
  const supabase = getSupabase();

  try {
    // Validate token
    const token = req.query.token;
    const jobId = Number(req.query.job_id);

    if (!token || token !== mustEnv("HEYGEN_WEBHOOK_SECRET")) {
      return res.status(401).json({ ok: false, error: "Invalid token" });
    }
    if (!jobId) {
      return res.status(400).json({ ok: false, error: "Missing job_id" });
    }

    // Parse HeyGen payload variations
    const payload = req.body || {};
    const status = payload?.data?.status || payload?.status;
    const videoId = payload?.data?.video_id || payload?.video_id;
    const videoUrl =
      payload?.data?.video_url ||
      payload?.data?.url ||
      payload?.video_url ||
      payload?.url;

    console.log("HEYGEN CALLBACK:", { jobId, status, videoId, hasUrl: !!videoUrl });

    // Update job row with what we got
    await supabase.from("render_jobs").update({
      status: status === "completed" ? "heygen_completed" : "heygen_requested",
      heygen_video_url: videoUrl || null,
    }).eq("id", jobId);

    // If not completed yet, accept callback (some systems send multiple)
    if (status && status !== "completed") {
      return res.json({ ok: true, received: true, status });
    }

    if (!videoUrl) {
      // Some accounts send callback without URL. In that case you must poll HeyGen status.
      // But you asked for callback architecture — so we fail loudly here.
      await supabase.from("render_jobs").update({
        status: "failed",
        error: "HeyGen callback missing video_url. Your account may not include video_url in callback payload.",
      }).eq("id", jobId);

      return res.status(500).json({ ok: false, error: "Callback missing video_url" });
    }

    // Load job
    const { data: job, error: jobErr } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", jobId)
      .single();

    if (jobErr) throw jobErr;

    await supabase.from("render_jobs").update({ status: "rendering" }).eq("id", jobId);

    const tmp = "/tmp";
    const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
    const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
    const logoPath = path.join(tmp, `logo-${jobId}.png`);

    const mainPath = path.join(tmp, `main-${jobId}.mp4`);
    const introPath = path.join(tmp, `intro-${jobId}.mp4`);
    const outroPath = path.join(tmp, `outro-${jobId}.mp4`);
    const finalPath = path.join(tmp, `final-${jobId}.mp4`);

    // Download assets
    await downloadToFile(job.walkthrough_url, walkPath);
    await downloadToFile(videoUrl, avatarPath);
    await downloadToFile(job.logo_url, logoPath);

    // Fix orientation based on rotation metadata
    const rot = await detectRotationDegrees(walkPath);
    const rotVF = rotationFilter(rot);
    const rotPrefix = rotVF ? `${rotVF},` : "";

    // MAIN COMPOSE (FAST)
    // - walkthrough becomes vertical 1080x1920
    // - avatar greenscreen keyed
    // - avatar larger + less “transparent”
    const filter =
      `[0:v]${rotPrefix}scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920,setsar=1[vbg];` +
      `[1:v]scale=iw*0.55:-2,chromakey=0x00FF00:0.16:0.06,format=rgba[fg];` +
      `[fg]split[fg1][fg2];` +
      `[fg1]colorchannelmixer=aa=0.70,boxblur=8:4[shadow];` +
      `[vbg][shadow]overlay=W-w-70:H-h-110[bgshadow];` +
      `[bgshadow][fg2]overlay=W-w-62:H-h-102[outv]`;

    await runFFmpeg([
      "-y",
      "-t", String(job.max_seconds || 30),
      "-i", walkPath,
      "-i", avatarPath,
      "-filter_complex", filter,
      "-map", "[outv]",
      "-map", "1:a?",              // avatar audio (which is your walkthrough audio)
      "-c:v", "libx264",
      "-preset", "ultrafast",      // FASTEST
      "-crf", "28",
      "-pix_fmt", "yuv420p",
      "-c:a", "aac",
      "-b:a", "128k",
      mainPath,
    ]);

    // INTRO/OUTRO full screen logo (1.5s)
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

    // CONCAT (intro + main + outro)
    const listPath = path.join(tmp, `list-${jobId}.txt`);
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

    // Upload final
    const buffer = fs.readFileSync(finalPath);
    const storagePath = `renders/walkthrough-${jobId}.mp4`;

    const up = await supabase.storage.from(BUCKET).upload(storagePath, buffer, {
      contentType: "video/mp4",
      upsert: true,
    });
    if (up.error) throw up.error;

    const { data: pub } = supabase.storage.from(BUCKET).getPublicUrl(up.data.path);

    await supabase.from("render_jobs").update({
      status: "completed",
      final_storage_path: storagePath,
      final_public_url: pub.publicUrl,
    }).eq("id", jobId);

    return res.json({ ok: true, job_id: jobId, final_url: pub.publicUrl });
  } catch (err) {
    console.error("CALLBACK ERROR:", err);

    // Best effort: mark job failed if we can
    try {
      const supabase = getSupabase();
      const jobId = Number(req.query.job_id);
      if (jobId) {
        await supabase.from("render_jobs").update({
          status: "failed",
          error: String(err),
        }).eq("id", jobId);
      }
    } catch {}

    return res.status(500).json({ ok: false, error: String(err) });
  }
});

/* -----------------------------
   START SERVER
----------------------------- */

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`Worker running on port ${PORT}`));
