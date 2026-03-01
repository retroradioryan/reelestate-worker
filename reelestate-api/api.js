import express from "express";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

/*
  HEALTH CHECK
*/
app.get("/", (req, res) => {
  res.json({ status: "API running" });
});

/*
  HEYGEN WEBHOOK
*/
app.post("/heygen-callback", async (req, res) => {
  try {
    const { job_id, video_url } = req.body;

    if (!job_id || !video_url) {
      return res.status(400).json({ error: "Missing job_id or video_url" });
    }

    const { error } = await supabase
      .from("render_jobs")
      .update({
        status: "rendering",
        heygen_video_url: video_url,
      })
      .eq("id", job_id);

    if (error) {
      console.error("Supabase update error:", error);
      return res.status(500).json({ error: "Database update failed" });
    }

    console.log("Webhook updated job:", job_id);
    res.json({ success: true });
  } catch (err) {
    console.error("Webhook error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

/*
  START SERVER
*/
app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
});
