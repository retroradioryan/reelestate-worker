import express from "express";
import { createClient } from "@supabase/supabase-js";
console.log("🔥 NEW API VERSION LOADED");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

/*
=====================================
  REQUIRED ENV CHECK
=====================================
*/
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error("❌ Missing Supabase environment variables.");
  process.exit(1);
}

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

/*
=====================================
  HEALTH CHECK
=====================================
*/
app.get("/", (req, res) => {
  res.json({
    status: "API running",
    timestamp: new Date().toISOString(),
  });
});

/*
=====================================
  HEYGEN WEBHOOK
=====================================
  Worker sends:
  ?token=SECRET&job_id=123

  HeyGen sends:
  {
    "event_type": "video.completed",
    "data": {
      "video_id": "...",
      "video_url": "..."
    }
  }
=====================================
*/
app.post("/heygen-callback", async (req, res) => {
  try {
    const token = req.query.token;
    const job_id = req.query.job_id;

    // 🔎 DEBUG LOGS FOR TOKEN ISSUE
    console.log("---- WEBHOOK TOKEN DEBUG ----");
    console.log("Incoming token:", token);
    console.log("Expected token:", process.env.HEYGEN_WEBHOOK_SECRET);
    console.log("Tokens match?:", token === process.env.HEYGEN_WEBHOOK_SECRET);
    console.log("--------------------------------");

    // Optional: validate secret if set
    if (process.env.HEYGEN_WEBHOOK_SECRET) {
      if (!token || token !== process.env.HEYGEN_WEBHOOK_SECRET) {
        console.warn("❌ Invalid webhook token.");
        return res.status(401).json({ error: "Invalid token" });
      }
    }

    const eventType = req.body?.event_type;
    const videoUrl = req.body?.data?.video_url;

    console.log("Webhook received:", {
      job_id,
      eventType,
      hasVideoUrl: !!videoUrl,
    });

    if (!job_id) {
      console.warn("❌ Missing job_id in query.");
      return res.status(400).json({ error: "Missing job_id in query" });
    }

    if (!videoUrl) {
      console.warn("❌ Missing video_url in payload.");
      return res.status(400).json({ error: "Missing video_url in payload" });
    }

    const { error } = await supabase
      .from("render_jobs")
      .update({
        status: "rendering",
        heygen_video_url: videoUrl,
      })
      .eq("id", job_id);

    if (error) {
      console.error("❌ Supabase update error:", error);
      return res.status(500).json({ error: "Database update failed" });
    }

    console.log("✅ Job moved to rendering:", job_id);

    return res.json({ success: true });
  } catch (err) {
    console.error("❌ Webhook error:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/*
=====================================
  START SERVER
=====================================
*/
app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
