import express from "express";
import { createClient } from "@supabase/supabase-js";

/* ==============================
   ENV
============================== */

function mustEnv(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`❌ Missing env var: ${name}`);
    process.exit(1);
  }
  return v;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
const HEYGEN_WEBHOOK_SECRET = process.env.HEYGEN_WEBHOOK_SECRET || null;

const app = express();
app.use(express.json({ limit: "20mb" }));

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

console.log("🔥 REELESTATE API (WEBHOOK-ONLY MODE) LOADED");

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
    res.status(500).json({ ok: false });
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
      return res.status(404).json({ ok: false });
    }

    res.json({ ok: true, job: data });

  } catch {
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   HEYGEN WEBHOOK (FINAL FIX)
============================== */

app.post("/heygen-callback", async (req, res) => {

  // Always respond immediately
  res.json({ ok: true });

  try {
    const token = req.query?.token;
    const jobId = req.query?.job_id;

    if (HEYGEN_WEBHOOK_SECRET && token !== HEYGEN_WEBHOOK_SECRET) {
      console.log("❌ Invalid webhook token");
      return;
    }

    if (!jobId) {
      console.log("❌ Missing job_id");
      return;
    }

    console.log("📩 Webhook received for job:", jobId);
    console.log("Full payload:", JSON.stringify(req.body, null, 2));

    // Extract video URL from ANY possible field
    const videoUrl =
      req.body?.data?.video_url ||
      req.body?.data?.url ||
      req.body?.event_data?.video_url ||
      req.body?.event_data?.url ||
      req.body?.video_url ||
      null;

    if (!videoUrl) {
      console.log("⏳ No video_url found in webhook yet");
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
    console.error("❌ Webhook error:", err);
  }
});

/* ==============================
   START SERVER
============================== */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
