import express from "express";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-api" });
});

app.post("/heygen-callback", async (req, res) => {
  const token = req.query.token;
  const jobId = req.query.job_id;

  if (token !== process.env.HEYGEN_WEBHOOK_SECRET) {
    return res.status(403).json({ error: "Invalid webhook token" });
  }

  const videoUrl = req.body?.data?.video_url;

  if (!videoUrl) {
    return res.status(400).json({ error: "Missing video_url" });
  }

  await supabase
    .from("render_jobs")
    .update({
      status: "rendering",
      heygen_video_url: videoUrl
    })
    .eq("id", jobId);

  res.json({ ok: true });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
