import express from "express";
import { createClient } from "@supabase/supabase-js";
import { Resend } from "resend";

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
const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const RESEND_API_KEY = mustEnv("RESEND_API_KEY");
const FROM_EMAIL = mustEnv("FROM_EMAIL");

const HEYGEN_WEBHOOK_SECRET = process.env.HEYGEN_WEBHOOK_SECRET || null;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const resend = new Resend(RESEND_API_KEY);

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
   CREATE JOB (called by website)
============================== */
app.post("/compose-walkthrough", async (req, res) => {
  try {
    const { walkthroughUrl, maxSeconds = 20, avatarType, email } = req.body;

    if (!walkthroughUrl) {
      return res.status(400).json({ ok: false, error: "walkthroughUrl required" });
    }
    if (!email) {
      return res.status(400).json({ ok: false, error: "email required" });
    }
    if (!avatarType || !["male", "female"].includes(String(avatarType).toLowerCase())) {
      return res.status(400).json({ ok: false, error: "avatarType must be 'male' or 'female'" });
    }

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
   JOB STATUS
============================== */
app.get("/job/:id", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", req.params.id)
      .single();

    if (error || !data) return res.status(404).json({ ok: false, error: "Job not found" });

    res.json({ ok: true, job: data });
  } catch (err) {
    console.error("❌ JOB STATUS ERROR:", err);
    res.status(500).json({ ok: false });
  }
});

/* ==============================
   HEYGEN STATUS FETCH (v1)
============================== */
async function fetchHeyGenVideoUrl(videoId) {
  const maxAttempts = 12;         // ~30s total
  const delayMs = 2500;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`🎬 Checking HeyGen status for ${videoId} (attempt ${attempt})`);

    try {
      const resp = await fetch("https://api.heygen.com/v1/video.status", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Api-Key": HEYGEN_API_KEY,
        },
        body: JSON.stringify({ video_id: videoId }),
      });

      const json = await resp.json().catch(() => null);

      console.log("HeyGen HTTP status:", resp.status);
      if (json) console.log("HeyGen response:", JSON.stringify(json, null, 2));

      if (!resp.ok) {
        await new Promise((r) => setTimeout(r, delayMs));
        continue;
      }

      const status = json?.data?.status;
      const url =
        json?.data?.video_url ||
        json?.data?.url ||
        json?.data?.videos?.[0]?.video_url ||
        json?.data?.videos?.[0]?.url ||
        null;

      if (status === "completed" && url && String(url).startsWith("http")) {
        return url;
      }
    } catch (err) {
      console.error("HeyGen fetch error:", err?.message || err);
    }

    await new Promise((r) => setTimeout(r, delayMs));
  }

  return null;
}

/* ==============================
   EMAIL SEND (final link)
============================== */
async function sendFinalEmail(to, finalUrl) {
  await resend.emails.send({
    from: FROM_EMAIL,
    to,
    subject: "🎬 Your ReelEstate Video Is Ready",
    html: `
      <div style="font-family: Arial, sans-serif;">
        <h2>Your video is ready ✅</h2>
        <p>Click below to view/download your rendered video:</p>
        <p><a href="${finalUrl}" target="_blank" rel="noopener noreferrer">${finalUrl}</a></p>
      </div>
    `,
  });
}

/* ==============================
   HEYGEN WEBHOOK
   - Respond immediately
   - Validate token (optional)
   - Use webhook as a TRIGGER
   - Fetch MP4 URL via HeyGen status
   - Update render_jobs => rendering + heygen_video_url
============================== */
app.post("/heygen-callback", async (req, res) => {
  res.json({ ok: true }); // immediate response to stop retries

  try {
    const token = req.query?.token ? String(req.query.token) : null;
    const jobId = req.query?.job_id ? String(req.query.job_id) : null;

    console.log("📩 Webhook hit for job:", jobId);

    if (HEYGEN_WEBHOOK_SECRET) {
      if (!token || token !== HEYGEN_WEBHOOK_SECRET) {
        console.log("❌ Invalid webhook token");
        return;
      }
    }

    if (!jobId) {
      console.log("❌ Missing job_id in query");
      return;
    }

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
      console.log("❌ No heygen_video_id on job yet:", jobId);
      return;
    }

    // If already have URL, don't thrash
    if (job.heygen_video_url && String(job.heygen_video_url).startsWith("http")) {
      console.log("✅ Job already has heygen_video_url:", jobId);
      return;
    }

    const videoUrl = await fetchHeyGenVideoUrl(job.heygen_video_id);

    if (!videoUrl) {
      console.log("⏳ Video URL not ready yet");
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

    // OPTIONAL: if you want, you can email here when HeyGen is ready,
    // but you asked for email after FINAL composite, so keep it in worker.
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
