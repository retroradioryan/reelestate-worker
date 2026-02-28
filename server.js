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

function getSupabase() {
  return createClient(
    mustEnv("SUPABASE_URL"),
    mustEnv("SUPABASE_SERVICE_ROLE_KEY")
  );
}

/* ==============================
   ROUTES
============================== */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-api" });
});

/* START JOB (LIGHTWEIGHT ONLY) */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const supabase = getSupabase();
    const { walkthroughUrl, logoUrl, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl || !logoUrl) {
      return res.status(400).json({
        ok: false,
        error: "walkthroughUrl and logoUrl required"
      });
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

    res.json({
      ok: true,
      job_id: job.id
    });

  } catch (err) {
    console.error("START ERROR:", err);
    res.status(500).json({
      ok: false,
      error: String(err)
    });
  }
});

/* JOB STATUS */

app.get("/job/:id", async (req, res) => {
  try {
    const supabase = getSupabase();

    const { data, error } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", req.params.id)
      .single();

    if (error || !data) {
      return res.status(404).json({
        ok: false,
        error: "Job not found"
      });
    }

    res.json({
      ok: true,
      job: data
    });

  } catch (err) {
    console.error("JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* HEYGEN CALLBACK (NO FFMPEG HERE) */

app.post("/heygen-callback", async (req, res) => {
  try {
    const token = req.query.token;
    const jobId = Number(req.query.job_id);

    if (token !== process.env.HEYGEN_WEBHOOK_SECRET) {
      return res.status(401).json({ ok: false });
    }

    const status = req.body?.data?.status;
    const videoUrl = req.body?.data?.video_url;

    const supabase = getSupabase();

    if (status === "completed" && videoUrl) {
      await supabase.from("render_jobs").update({
        status: "rendering",
        heygen_video_url: videoUrl
      }).eq("id", jobId);
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
