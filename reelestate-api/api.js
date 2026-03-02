import express from "express";
import { createClient } from "@supabase/supabase-js";

/* ==============================
   ENV
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
const HEYGEN_WEBHOOK_SECRET = process.env.HEYGEN_WEBHOOK_SECRET || null;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const app = express();
app.use(express.json({ limit: "25mb" }));

console.log("🔥 REELESTATE API LOADED");

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
    const { walkthroughUrl, maxSeconds = 20, avatarType, email } = req.body;

    if (!walkthroughUrl)
      return res.status(400).json({ ok: false, error: "walkthroughUrl required" });

    if (!email)
      return res.status(400).json({ ok: false, error: "email required" });

    if (!avatarType || !["male", "female"].includes(String(avatarType).toLowerCase()))
      return res.status(400).json({ ok: false, error: "avatarType must be 'male' or 'female'" });

    const { data: job, error } = await supabase
      .from("render_jobs")
      .insert({
        status: "queued",
        walkthrough_url: walkthroughUrl,
        max_seconds: maxSeconds,
        avatar_type: String(avatarType).toLowerCase(),
        email,
      })
      .select("*")
      .single();

    if (error) throw error;

    console.log("✅ Job created:", job.id);

    res.json({ ok: true, job_id: job.id });

  } catch (err) {
    console.error("❌ CREATE JOB ERROR:", err);
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

/* ==============================
   JOB STATUS (Optional)
============================== */
app.get("/job/:id", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", req.params.id)
      .single();

    if (error || !data)
      return res.status(404).json({ ok: false, error: "Job not found" });

    res.json({ ok: true, job: data });

  } catch (err) {
    console.error("❌ JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   HEYGEN WEBHOOK (PROPER v2 HANDLING)
============================== */
app.post("/heygen-callback", async (req, res) => {
  // Always respond immediately (prevents retries)
  res.json({ ok: true });

  try {
    const token = req.query?.token ? String(req.query.token) : null;
    const jobId = req.query?.job_id ? String(req.query.job_id) : null;

    if (HEYGEN_WEBHOOK_SECRET && token !== HEYGEN_WEBHOOK_SECRET) {
      console.log("❌ Invalid webhook token");
      return;
    }

    if (!jobId) {
      console.log("❌ Missing job_id");
      return;
    }

    console.log("📩 Webhook body:", JSON.stringify(req.body));

    const eventType = req.body?.event_type || req.body?.type || null;

    // Ignore anything except completed
    if (eventType !== "video.completed") {
      console.log("ℹ️ Ignoring event:", eventType);
      return;
    }

    const videoUrl =
      req.body?.data?.video_url ||
      req.body?.data?.url ||
      null;

    if (!videoUrl) {
      console.log("❌ Completed event but no video_url found");
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
   START
============================== */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
