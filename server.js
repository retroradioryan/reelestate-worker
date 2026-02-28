import express from "express";
import { createClient } from "@supabase/supabase-js";

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

const supabase = createClient(
  mustEnv("SUPABASE_URL"),
  mustEnv("SUPABASE_SERVICE_ROLE_KEY")
);

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
    const { walkthroughUrl, logoUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !logoUrl) {
      return res.status(400).json({
        ok: false,
        error: "walkthroughUrl and logoUrl required",
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

    res.json({
      ok: true,
      job_id: job.id,
    });

  } catch (err) {
    console.error("START ERROR:", err);
    res.status(500).json({
      ok: false,
      error: String(err),
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
    console.error("JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   HEYGEN WEBHOOK (FINAL CORRECT VERSION)
============================== */

app.post("/heygen-callback", async (req, res) => {
  try {
    console.log("WEBHOOK RECEIVED:", req.body);

    const videoId = req.body?.data?.video_id;
    const status = req.body?.data?.status;
    const videoUrl = req.body?.data?.video_url;

    if (!videoId) {
      return res.json({ ok: true }); // Ignore malformed events
    }

    if (status === "completed" && videoUrl) {

      await supabase
        .from("render_jobs")
        .update({
          status: "rendering",
          heygen_video_url: videoUrl,
        })
        .eq("heygen_video_id", videoId);

      console.log("Updated job to rendering:", videoId);
    }

    res.json({ ok: true });

  } catch (err) {
    console.error("CALLBACK ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   START SERVER
============================== */

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`API running on port ${PORT}`);
});
