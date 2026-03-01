import express from "express";
import { createClient } from "@supabase/supabase-js";

/* ==============================
   ENV VALIDATION
============================== */

function mustEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`❌ Missing environment variable: ${name}`);
    process.exit(1);
  }
  return v;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");

// Optional but recommended
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

console.log("🚀 ReelEstate API starting...");

/* ==============================
   HEALTH
============================== */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-api" });
});

/* ==============================
   START JOB
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

    res.json({
      ok: true,
      job_id: job.id,
    });

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

    res.json({
      ok: true,
      job: data,
    });

  } catch (err) {
    console.error("❌ JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   HELPER: FETCH MP4 FROM HEYGEN
============================== */

async function fetchHeyGenVideoUrl(videoId) {
  const maxAttempts = 6;
  const delayMs = 2500;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {

    console.log(`Fetching HeyGen video ${videoId} (attempt ${attempt})`);

    const resp = await fetch(
      `https://api.heygen.com/v2/video/${videoId}`,
      {
        headers: {
          "X-Api-Key": HEYGEN_API_KEY,
        },
      }
    );

    const json = await resp.json().catch(() => null);

    if (!resp.ok) {
      console.log("HeyGen GET error:", json);
      return null;
    }

    const url =
      json?.data?.video_url ||
      json?.data?.url ||
      json?.data?.videos?.[0]?.video_url ||
      json?.data?.videos?.[0]?.url ||
      null;

    if (url && url.startsWith("http")) {
      return url;
    }

    await new Promise((r) => setTimeout(r, delayMs));
  }

  return null;
}

/* ==============================
   HEYGEN WEBHOOK (FINAL FIX)
   - Does NOT rely on payload shape
   - Uses job_id from query
   - Fetches video via video_id stored in DB
============================== */

app.post("/heygen-callback", async (req, res) => {
  try {
    // Respond immediately so HeyGen stops retrying
    res.json({ ok: true });

    const token = req.query?.token;
    const jobId = req.query?.job_id;

    console.log("Webhook received for job:", jobId);

    if (HEYGEN_WEBHOOK_SECRET) {
      if (!token || token !== HEYGEN_WEBHOOK_SECRET) {
        console.log("❌ Invalid webhook token");
        return;
      }
    }

    if (!jobId) {
      console.log("❌ No job_id in callback");
      return;
    }

    // Get job
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
      console.log("❌ No heygen_video_id stored yet");
      return;
    }

    // Fetch MP4 from HeyGen directly
    const videoUrl = await fetchHeyGenVideoUrl(job.heygen_video_id);

    if (!videoUrl) {
      console.log("⚠️ Video URL not ready yet");
      return;
    }

    // Update job
    const { error: updateError } = await supabase
      .from("render_jobs")
      .update({
        status: "rendering",
        heygen_video_url: videoUrl,
      })
      .eq("id", jobId);

    if (updateError) {
      console.log("❌ Failed updating job:", updateError);
      return;
    }

    console.log("✅ Job moved to rendering:", jobId);

  } catch (err) {
    console.error("❌ CALLBACK ERROR:", err);
  }
});

/* ==============================
   START SERVER
============================== */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
