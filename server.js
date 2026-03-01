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
const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");

// Optional: if you want token protection on webhook URL
const HEYGEN_WEBHOOK_SECRET = process.env.HEYGEN_WEBHOOK_SECRET || null;

/* ==============================
   INIT
============================== */
const app = express();
app.use(express.json({ limit: "20mb" }));

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("🚀 ReelEstate API starting...");

/* ==============================
   HELPERS
============================== */
async function fetchHeyGenVideo(videoId) {
  // HeyGen sometimes needs a moment after success event before URL is attached
  // so we allow a few retries.
  const maxAttempts = 6;
  const delayMs = 2500;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const resp = await fetch(`https://api.heygen.com/v2/video/${videoId}`, {
      headers: { "X-Api-Key": HEYGEN_API_KEY },
    });

    const json = await resp.json().catch(() => null);

    console.log(`HeyGen GET video attempt ${attempt}/${maxAttempts} status=${resp.status}`);
    if (json) console.log(JSON.stringify(json, null, 2));

    // Try common locations for the mp4 URL
    const url =
      json?.data?.video_url ||
      json?.data?.url ||
      json?.data?.videos?.[0]?.video_url ||
      json?.data?.videos?.[0]?.url ||
      json?.data?.result?.video_url ||
      json?.data?.result?.url ||
      null;

    if (url && typeof url === "string" && url.startsWith("http")) {
      return url;
    }

    // wait and retry
    await new Promise((r) => setTimeout(r, delayMs));
  }

  return null;
}

/* ==============================
   HEALTH CHECK
============================== */
app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-api" });
});

/* ==============================
   START JOB (OPTIONAL — you can still use curl)
============================== */
app.post("/compose-walkthrough", async (req, res) => {
  try {
    const { walkthroughUrl, logoUrl = null, maxSeconds = 30 } = req.body;

    if (!walkthroughUrl) {
      return res.status(400).json({ ok: false, error: "walkthroughUrl required" });
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
    res.status(500).json({ ok: false, error: String(err?.message || err) });
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
      return res.status(404).json({ ok: false, error: "Job not found" });
    }

    res.json({ ok: true, job: data });
  } catch (err) {
    console.error("❌ JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false, error: "Internal error" });
  }
});

/* ==============================
   HEYGEN WEBHOOK (FIXED)
   - Accepts: /heygen-callback?token=12345&job_id=41 (optional token)
   - Reads video_id from payload (various shapes)
   - Fetches MP4 URL from HeyGen API using video_id
   - Updates render_jobs by heygen_video_id (preferred)
     and falls back to query job_id if present
============================== */
app.post("/heygen-callback", async (req, res) => {
  try {
    // Always respond OK quickly; do work safely but fast
    res.json({ ok: true });

    // ---- Token check (optional) ----
    if (HEYGEN_WEBHOOK_SECRET) {
      const incomingToken = req.query?.token;
      if (!incomingToken || incomingToken !== HEYGEN_WEBHOOK_SECRET) {
        console.warn("❌ Invalid webhook token.", { incomingToken });
        return;
      }
    }

    console.log("===== HEYGEN WEBHOOK RECEIVED =====");
    console.log("Query:", req.query);
    console.log(JSON.stringify(req.body, null, 2));
    console.log("====================================");

    const body = req.body;

    const eventType =
      body?.event_type ||
      body?.eventType ||
      body?.type ||
      null;

    // ignore gif preview events
    if (eventType === "avatar_video_gif.success") {
      console.log("Ignoring GIF event.");
      return;
    }

    const videoId =
      body?.data?.video_id ||
      body?.event_data?.video_id ||
      body?.video_id ||
      body?.data?.id ||
      body?.event_data?.id ||
      null;

    if (!videoId) {
      console.log("⚠️ No videoId found. Ignoring.");
      return;
    }

    // Only handle "success" event
    if (eventType !== "avatar_video.success") {
      console.log("⚠️ Not avatar_video.success. Ignoring.", { eventType });
      return;
    }

    console.log("✅ Success event received for videoId:", videoId);
    console.log("Fetching MP4 URL from HeyGen...");

    const mp4Url = await fetchHeyGenVideo(videoId);

    if (!mp4Url) {
      console.log("⚠️ MP4 URL not available yet after retries. Will rely on HeyGen retrying webhook.");
      return;
    }

    console.log("✅ MP4 URL resolved:", mp4Url);

    // Prefer updating by heygen_video_id (most reliable)
    let updated = false;

    const updByVideoId = await supabase
      .from("render_jobs")
      .update({
        status: "rendering",
        heygen_video_url: mp4Url,
      })
      .eq("heygen_video_id", videoId);

    if (!updByVideoId.error) {
      updated = true;
      console.log("✅ Updated job by heygen_video_id:", videoId);
    } else {
      console.error("❌ Update by heygen_video_id failed:", updByVideoId.error);
    }

    // Fallback: if job_id is provided in query
    if (!updated && req.query?.job_id) {
      const jobId = String(req.query.job_id);
      const updByJobId = await supabase
        .from("render_jobs")
        .update({
          status: "rendering",
          heygen_video_url: mp4Url,
        })
        .eq("id", jobId);

      if (updByJobId.error) {
        console.error("❌ Update by job_id failed:", updByJobId.error);
      } else {
        console.log("✅ Updated job by job_id:", jobId);
      }
    }
  } catch (err) {
    console.error("❌ CALLBACK ERROR:", err);
    // note: we already returned ok: true
  }
});

/* ==============================
   START SERVER
============================== */
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`🚀 API running on port ${PORT}`);
});
