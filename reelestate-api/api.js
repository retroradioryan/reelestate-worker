import express from "express";
import { createClient } from "@supabase/supabase-js";

/* ==============================
   ENV VALIDATION
============================== */

function mustEnv(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`❌ Missing environment variable: ${name}`);
    process.exit(1);
  }
  return value;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_WEBHOOK_SECRET = process.env.HEYGEN_WEBHOOK_SECRET || null;

/* ==============================
   INIT
============================== */

const app = express();
app.use(express.json({ limit: "20mb" }));

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

console.log("🔥 REELESTATE API (RPC STATUS VERSION) LOADED");

/* ==============================
   HEALTH
============================== */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-api" });
});

/* ==============================
   CREATE JOB
============================== */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const { walkthroughUrl, logoUrl = null, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl) {
      return res.status(400).json({
        ok: false,
        error: "walkthroughUrl required",
      });
    }

    const { data: job, error } = await supabase
      .from("render_jobs")
      .insert({
        status: "queued",
        walkthrough_url: walkthroughUrl,
        logo_url: logoUrl,
        max_seconds: maxSeconds,
      })
      .select("*")
      .single();

    if (error) throw error;

    console.log("✅ Job created:", job.id);

    res.json({ ok: true, job_id: job.id });

  } catch (err) {
    console.error("❌ START JOB ERROR:", err);
    res.status(500).json({
      ok: false,
      error: String(err?.message || err),
    });
  }
});

/* ==============================
   JOB STATUS
============================== */

app.get("/job/:id", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", req.params.id)
      .single();

    if (error || !data) {
      return res.status(404).json({
        ok: false,
        error: "Job not found",
      });
    }

    res.json({ ok: true, job: data });

  } catch (err) {
    console.error("❌ JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   FETCH HEYGEN VIDEO (RPC STYLE)
============================== */

async function fetchHeyGenVideoUrl(videoId) {
  const maxAttempts = 6;
  const delayMs = 2500;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {

    console.log(`🎬 Checking HeyGen video ${videoId} (attempt ${attempt})`);

    try {
      const resp = await fetch(
        "https://api.heygen.com/v1/video.status",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Api-Key": HEYGEN_API_KEY,
          },
          body: JSON.stringify({ video_id: videoId }),
        }
      );

      console.log("HeyGen HTTP status:", resp.status);

      const json = await resp.json().catch(() => null);
      console.log("HeyGen response:", JSON.stringify(json, null, 2));

      if (!resp.ok) {
        await new Promise(r => setTimeout(r, delayMs));
        continue;
      }

      const status = json?.data?.status;
      const videoUrl = json?.data?.video_url;

      if (status === "completed" && videoUrl) {
        return videoUrl;
      }

    } catch (err) {
      console.error("HeyGen fetch error:", err.message);
    }

    await new Promise(r => setTimeout(r, delayMs));
  }

  return null;
}

/* ==============================
   HEYGEN WEBHOOK
============================== */

app.post("/heygen-callback", async (req, res) => {

  // Respond immediately so HeyGen stops retrying
  res.json({ ok: true });

  try {
    const token = req.query?.token;
    const jobId = req.query?.job_id;

    console.log("📩 Webhook received for job:", jobId);

    if (HEYGEN_WEBHOOK_SECRET) {
      if (!token || token !== HEYGEN_WEBHOOK_SECRET) {
        console.log("❌ Invalid webhook token");
        return;
      }
    }

    if (!jobId) {
      console.log("❌ Missing job_id");
      return;
    }

    const { data: job, error } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", jobId)
      .single();

    if (error || !job) {
      console.log("❌ Job not found:", jobId);
      return;
    }

    if (!job.heygen_video_id) {
      console.log("❌ heygen_video_id not stored yet");
      return;
    }

    const videoUrl = await fetchHeyGenVideoUrl(job.heygen_video_id);

    if (!videoUrl) {
      console.log("⏳ Video not ready yet");
      return;
    }

    await supabase
      .from("render_jobs")
      .update({
        status: "rendering",
        heygen_video_url: videoUrl,
      })
      .eq("id", jobId);

    console.log("✅ Job moved to rendering:", jobId);

  } catch (err) {
    console.error("❌ Webhook processing error:", err);
  }
});

/* ==============================
   START SERVER
============================== */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
