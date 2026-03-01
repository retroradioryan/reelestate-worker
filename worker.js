import { createClient } from "@supabase/supabase-js";

/* ==============================
   ENV VALIDATION
============================== */
function mustEnv(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`❌ Missing environment variable: ${name}`);
    process.exit(1);
  }
  return value;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");

const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_AVATAR_ID = mustEnv("HEYGEN_AVATAR_ID");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

const POLL_MS = Number(process.env.POLL_MS || 5000);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log("🚀 WORKER LIVE");
console.log("Polling every", POLL_MS, "ms");
console.log("HEYGEN_CALLBACK_BASE_URL:", HEYGEN_CALLBACK_BASE_URL);

/* ==============================
   UTILS
============================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text().catch(() => "");

  if (!response.ok) {
    console.error("❌ HTTP ERROR:", response.status, text);
    throw new Error(text || "Request failed");
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error("Invalid JSON response");
  }
}

/* ==============================
   HEYGEN: CREATE VIDEO
   IMPORTANT: callback_url MUST be ROOT level
============================== */
async function createHeygenVideo({ scriptText, jobId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("🎬 Creating HeyGen video for job:", jobId);
  console.log("Callback URL:", callbackUrl);

  const payload = {
    video_inputs: [
      {
        character: { type: "avatar", avatar_id: HEYGEN_AVATAR_ID },
        voice: { type: "text", voice_id: HEYGEN_VOICE_ID, input_text: scriptText },
        background: { type: "color", value: "#00FF00" },
      },
    ],
    dimension: { width: 1080, height: 1920 },
    callback_url: callbackUrl, // ✅ ROOT-LEVEL
  };

  const json = await fetchJSON("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": HEYGEN_API_KEY,
    },
    body: JSON.stringify(payload),
  });

  const videoId = json?.data?.video_id;
  if (!videoId) {
    console.error("HeyGen response:", json);
    throw new Error("HeyGen did not return video_id");
  }

  console.log("✅ HEYGEN VIDEO ID:", videoId);
  return videoId;
}

/* ==============================
   PROCESS QUEUED (LOCKED)
============================== */
async function processQueued(job) {
  const jobId = job.id;

  // Lock job to prevent duplicates
  const { data: locked, error: lockErr } = await supabase
    .from("render_jobs")
    .update({ status: "processing" })
    .eq("id", jobId)
    .eq("status", "queued")
    .select("*")
    .maybeSingle();

  if (lockErr) throw lockErr;
  if (!locked) {
    console.log("⏭️ Job already taken:", jobId);
    return;
  }

  console.log("📦 Processing QUEUED job:", jobId);

  // Keep script simple & stable (you can swap back to OpenAI later)
  const script =
    "Welcome to this beautiful new listing. Contact us today to arrange your private viewing.";

  const videoId = await createHeygenVideo({ scriptText: script, jobId });

  const { error: updErr } = await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: videoId,
    })
    .eq("id", jobId);

  if (updErr) throw updErr;

  console.log("⏳ Waiting for webhook to set status=rendering...");
}

/* ==============================
   PROCESS RENDERING (LOCKED)
   For now we mark completed using HeyGen video URL.
   (Swap in your FFmpeg branding phase afterwards.)
============================== */
async function processRendering(job) {
  const jobId = job.id;

  const { data: locked, error: lockErr } = await supabase
    .from("render_jobs")
    .update({ status: "rendering_in_progress" })
    .eq("id", jobId)
    .eq("status", "rendering")
    .select("*")
    .maybeSingle();

  if (lockErr) throw lockErr;
  if (!locked) {
    console.log("⏭️ Rendering already in progress:", jobId);
    return;
  }

  if (!locked.heygen_video_url) {
    console.log("⚠️ Rendering job has no heygen_video_url yet:", jobId);
    // Put it back to rendering so it can be retried
    await supabase.from("render_jobs").update({ status: "rendering" }).eq("id", jobId);
    return;
  }

  console.log("🎞 Completing job using HeyGen URL:", jobId);

  const { error: doneErr } = await supabase
    .from("render_jobs")
    .update({
      status: "completed",
      final_public_url: locked.heygen_video_url,
    })
    .eq("id", jobId);

  if (doneErr) throw doneErr;

  console.log("✅ Job completed:", jobId);
}

/* ==============================
   MAIN LOOP
============================== */
async function loop() {
  while (true) {
    try {
      // 1) Check queued
      const { data: queued, error: qErr } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "queued")
        .order("created_at", { ascending: true })
        .limit(1);

      if (qErr) throw qErr;

      if (queued?.length) {
        await processQueued(queued[0]);
        await sleep(POLL_MS);
        continue;
      }

      // 2) Check rendering
      const { data: rendering, error: rErr } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "rendering")
        .order("created_at", { ascending: true })
        .limit(1);

      if (rErr) throw rErr;

      if (rendering?.length) {
        await processRendering(rendering[0]);
      }
    } catch (err) {
      console.error("❌ Worker error:", err?.message || err);
    }

    await sleep(POLL_MS);
  }
}

loop();
