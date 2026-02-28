import express from "express";
import { spawn } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "20mb" }));

/* ==============================
   ENV + SUPABASE
============================== */

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

/* ==============================
   HELPERS
============================== */

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
      else reject(new Error(`FFmpeg exited with code ${code}`));
    });
  });
}

/* ==============================
   HEYGEN CREATE
============================== */

async function heygenCreateVideo(audioUrl, jobId) {
  const avatarId = mustEnv("HEYGEN_AVATAR_ID");
  const baseUrl = mustEnv("PUBLIC_BASE_URL");
  const secret = mustEnv("HEYGEN_WEBHOOK_SECRET");

  const webhookUrl =
    `${baseUrl}/heygen-callback?token=${secret}&job_id=${jobId}`;

  const body = {
    video_inputs: [{
      character: {
        type: "avatar",
        avatar_id: avatarId
      },
      voice: {
        type: "audio",
        audio_url: audioUrl
      },
      background: {
        type: "color",
        value: "#00FF00"
      }
    }],
    dimension: { width: 1080, height: 1920 },
    webhook_url: webhookUrl
  };

  const resp = await fetch(
    "https://api.heygen.com/v2/video/generate",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Api-Key": mustEnv("HEYGEN_API_KEY")
      },
      body: JSON.stringify(body)
    }
  );

  const json = await resp.json();

  if (!resp.ok) {
    throw new Error(`HeyGen error: ${JSON.stringify(json)}`);
  }

  return json?.data?.video_id;
}

/* ==============================
   BACKGROUND RENDER
============================== */

async function processRender(jobId, videoUrl) {
  const supabase = getSupabase();

  try {
    console.log("Render started:", jobId);

    const { data: job, error } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", jobId)
      .single();

    if (error) throw error;
    if (!job) throw new Error("Job not found");

    const tmp = "/tmp";
    const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
    const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
    const finalPath = path.join(tmp, `final-${jobId}.mp4`);

    await downloadToFile(job.walkthrough_url, walkPath);
    await downloadToFile(videoUrl, avatarPath);

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

    console.log("Render finished:", jobId);

  } catch (err) {
    console.error("PROCESS RENDER ERROR:", err);

    await supabase.from("render_jobs").update({
      status: "failed",
      error: String(err)
    }).eq("id", jobId);
  }
}

/* ==============================
   ROUTES
============================== */

app.get("/", (req, res) => {
  res.json({ ok: true });
});

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const supabase = getSupabase();
    const { walkthroughUrl, logoUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !logoUrl) {
      return res.status(400).json({ ok: false });
    }

    const { data: job, error } = await supabase
      .from("render_jobs")
      .insert({
        status: "queued",
        walkthrough_url: walkthroughUrl,
        logo_url: logoUrl,
        max_seconds: maxSeconds
      })
      .select("*")
      .single();

    if (error) throw error;

    const jobId = job.id;

    const tmp = "/tmp";
    const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
    const audioPath = path.join(tmp, `audio-${jobId}.m4a`);

    await downloadToFile(walkthroughUrl, walkPath);

    await runFFmpeg([
      "-y",
      "-i", walkPath,
      "-vn",
      "-c:a", "aac",
      "-b:a", "128k",
      audioPath
    ]);

    const audioBuf = fs.readFileSync(audioPath);
    const audioStoragePath = `renders/audio-${jobId}.m4a`;

    const up = await supabase.storage
      .from(BUCKET)
      .upload(audioStoragePath, audioBuf, {
        contentType: "audio/mp4",
        upsert: true
      });

    if (up.error) throw up.error;

    const { data: pub } =
      supabase.storage.from(BUCKET).getPublicUrl(up.data.path);

    const heygenVideoId =
      await heygenCreateVideo(pub.publicUrl, jobId);

    await supabase.from("render_jobs").update({
      status: "heygen_requested",
      heygen_video_id: heygenVideoId
    }).eq("id", jobId);

    res.json({ ok: true, job_id: jobId });

  } catch (err) {
    console.error("START ERROR:", err);
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.get("/job/:id", async (req, res) => {
  try {
    const supabase = getSupabase();
    const { data, error } =
      await supabase.from("render_jobs")
        .select("*")
        .eq("id", req.params.id)
        .single();

    if (error) throw error;

    res.json({ ok: true, job: data });
  } catch {
    res.status(404).json({ ok: false });
  }
});

/* ==============================
   WEBHOOK
============================== */

app.post("/heygen-callback", async (req, res) => {
  try {
    const token = req.query.token;
    const jobId = Number(req.query.job_id);

    if (token !== process.env.HEYGEN_WEBHOOK_SECRET) {
      return res.status(401).json({ ok: false });
    }

    const status = req.body?.data?.status;
    const videoUrl = req.body?.data?.video_url;

    if (status !== "completed" || !videoUrl) {
      return res.json({ ok: true });
    }

    const supabase = getSupabase();

    await supabase.from("render_jobs").update({
      status: "rendering",
      heygen_video_url: videoUrl
    }).eq("id", jobId);

    setImmediate(() => {
      processRender(jobId, videoUrl);
    });

    res.json({ ok: true });

  } catch (err) {
    console.error("CALLBACK ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   GLOBAL SAFETY
============================== */

process.on("unhandledRejection", err => {
  console.error("UNHANDLED REJECTION:", err);
});

process.on("uncaughtException", err => {
  console.error("UNCAUGHT EXCEPTION:", err);
});

/* ==============================
   START SERVER
============================== */

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Worker running on port ${PORT}`);
});
