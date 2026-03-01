import express from "express";
import { createClient } from "@supabase/supabase-js";

/* ==============================
   ENV VALIDATION
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
   HEALTH CHECK
============================== */

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "reelestate-api",
  });
});

/* ==============================
   START JOB
============================== */

app.post("/compose-walkthrough", async (req, res) => {
  try {
    const { walkthroughUrl, logoUrl, maxSeconds = 30 } = req.body;

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
        logo_url: logoUrl || null,
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
    console.error("❌ JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   HEYGEN WEBHOOK (FULLY ROBUST)
============================== */

app.post("/heygen-callback", async (req, res) => {
  try {
    console.log("===== HEYGEN WEBHOOK RECEIVED =====");
    console.log(JSON.stringify(req.body, null, 2));
    console.log("====================================");

    const body = req.body;

    // Support multiple possible payload structures
    const eventType =
      body?.event_type ||
      body?.eventType ||
      body?.type ||
      null;

    const videoId =
      body?.data?.video_id ||
      body?.event_data?.video_id ||
      body?.video_id ||
      null;

    const videoUrl =
      body?.data?.video_url ||
      body?.data?.url ||
      body?.event_data?.video_url ||
      body?.event_data?.url ||
      body?.video_url ||
      null;

    console.log("Parsed eventType:", eventType);
    console.log("Parsed videoId:", videoId);
    console.log("Parsed videoUrl:", videoUrl);

    if (!videoId) {
      console.log("⚠️ No videoId found. Ignoring.");
      return res.json({ ok: true });
    }

    // Ignore GIF preview events
    if (eventType === "avatar_video_gif.success") {
      console.log("Ignoring GIF event.");
      return res.json({ ok: true });
    }

    // Only proceed when MP4 URL exists
    if (eventType === "avatar_video.success" && videoUrl) {

      const { error } = await supabase
        .from("render_jobs")
        .update({
          status: "rendering",
          heygen_video_url: videoUrl,
        })
        .eq("heygen_video_id", videoId);

      if (error) {
        console.error("❌ Supabase update error:", error);
      } else {
        console.log("✅ Job moved to rendering:", videoId);
      }

    } else {
      console.log("Event received but no video URL yet.");
    }

    // Always return success so HeyGen stops retrying
    res.json({ ok: true });

  } catch (err) {
    console.error("❌ CALLBACK ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   START SERVER
============================== */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
