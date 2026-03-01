import { createClient } from "@supabase/supabase-js";
import { spawn } from "child_process";
import OpenAI from "openai";
import fs from "fs";
import path from "path";
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

const OPENAI_API_KEY = mustEnv("OPENAI_API_KEY");

const HEYGEN_API_KEY = mustEnv("HEYGEN_API_KEY");
const HEYGEN_CALLBACK_BASE_URL = mustEnv("HEYGEN_CALLBACK_BASE_URL");
const HEYGEN_WEBHOOK_SECRET = mustEnv("HEYGEN_WEBHOOK_SECRET");
const HEYGEN_VOICE_ID = mustEnv("HEYGEN_VOICE_ID");

const HEYGEN_AVATAR_ID_FEMALE = mustEnv("HEYGEN_AVATAR_ID_FEMALE");
const HEYGEN_AVATAR_ID_MALE = mustEnv("HEYGEN_AVATAR_ID_MALE");

const RESEND_API_KEY = mustEnv("RESEND_API_KEY");
const FROM_EMAIL = mustEnv("FROM_EMAIL");

const STORAGE_BUCKET = process.env.STORAGE_BUCKET || "videos";
const POLL_MS = Number(process.env.POLL_MS || 5000);

// keying + layout tuning
const KEY_COLOR_HEX = process.env.KEY_COLOR_HEX || "#00FF00";
const KEY_COLOR_FFMPEG = (process.env.KEY_COLOR_FFMPEG || "0x00FF00").trim();
const KEY_SIMILARITY = String(process.env.KEY_SIMILARITY || "0.35");
const KEY_BLEND = String(process.env.KEY_BLEND || "0.20");

// avatar placement (make her sit lower, less floating)
const AVATAR_SCALE_W = Number(process.env.AVATAR_SCALE_W || 520);
const AVATAR_MARGIN_X = Number(process.env.AVATAR_MARGIN_X || 70);  // from right
const AVATAR_MARGIN_Y = Number(process.env.AVATAR_MARGIN_Y || 80);  // from bottom (LOWER = closer to bottom)

// lower third
const LT_TEXT = process.env.LT_TEXT || "Brand New Listing";
const LT_BAR_Y = Number(process.env.LT_BAR_Y || 1650);
const LT_BAR_H = Number(process.env.LT_BAR_H || 220);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const resend = new Resend(RESEND_API_KEY);

console.log("🚀 WORKER LIVE");
console.log("Polling every", POLL_MS, "ms");

/* ==============================
   UTILS
============================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function runFFmpeg(args) {
  return new Promise((resolve, reject) => {
    const ff = spawn("ffmpeg", args);
    ff.stderr.on("data", (d) => console.log(d.toString()));
    ff.on("error", reject);
    ff.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`FFmpeg failed (${code})`))));
  });
}

async function downloadFile(url, outPath) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Download failed: ${resp.status}`);
  const buffer = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(outPath, buffer);
}

function safeUnlink(p) {
  try {
    if (p && fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

async function uploadToStorage(localPath, storagePath) {
  const buffer = fs.readFileSync(localPath);

  const { error } = await supabase.storage.from(STORAGE_BUCKET).upload(storagePath, buffer, {
    contentType: "video/mp4",
    upsert: true,
  });
  if (error) throw error;

  const { data } = supabase.storage.from(STORAGE_BUCKET).getPublicUrl(storagePath);
  return data.publicUrl;
}

async function sendFinalEmail(to, url) {
  await resend.emails.send({
    from: FROM_EMAIL,
    to,
    subject: "🎬 Your ReelEstate Video Is Ready",
    html: `
      <div style="font-family: Arial, sans-serif;">
        <h2>Your video is ready ✅</h2>
        <p>Click below to view/download your rendered video:</p>
        <p><a href="${url}" target="_blank" rel="noopener noreferrer">${url}</a></p>
      </div>
    `,
  });
}

/* ==============================
   AUDIO → WHISPER → SCRIPT
============================== */
async function extractAudio(videoPath, audioPath) {
  // Use m4a/aac (more reliable than mp3 in some ffmpeg builds)
  await runFFmpeg([
    "-y",
    "-i", videoPath,
    "-vn",
    "-ac", "1",
    "-ar", "44100",
    "-c:a", "aac",
    "-b:a", "128k",
    audioPath,
  ]);
}

function estimateWordTarget(seconds) {
  // ~2.4 words/sec is a decent spoken pace
  return Math.max(35, Math.round(seconds * 2.4));
}

async function generateAvatarScriptFromWalkthrough(walkthroughUrl, jobId, maxSeconds = 20) {
  const tmp = "/tmp";
  const videoPath = path.join(tmp, `walk-${jobId}.mp4`);
  const audioPath = path.join(tmp, `audio-${jobId}.m4a`);

  try {
    await downloadFile(walkthroughUrl, videoPath);
    await extractAudio(videoPath, audioPath);

    console.log("🧠 Transcribing walkthrough audio (Whisper)…");

    const transcriptRes = await openai.audio.transcriptions.create({
      file: fs.createReadStream(audioPath),
      model: "whisper-1",
    });

    const transcript = (transcriptRes?.text || "").trim();
    if (!transcript) throw new Error("Empty transcript from Whisper");

    console.log("✍️ Writing ~" + maxSeconds + "s avatar script…");

    const wordTarget = estimateWordTarget(maxSeconds);

    const scriptRes = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      temperature: 0.6,
      messages: [
        {
          role: "system",
          content:
            "You are a professional real estate agent speaking on camera. " +
            "Rewrite the transcript into a confident natural script for a voiceover avatar. " +
            "No headings. No bullet points. No emojis. No stage directions. " +
            "Do not mention 'walkthrough', 'recording', or 'this video'. " +
            "Keep it tight, persuasive, and end with a simple call-to-action to book a viewing.",
        },
        {
          role: "user",
          content: `MAX SECONDS: ${maxSeconds}\nWORD TARGET: ~${wordTarget}\n\nTRANSCRIPT:\n${transcript}`,
        },
      ],
    });

    const script = (scriptRes?.choices?.[0]?.message?.content || "").trim();
    if (!script) throw new Error("Empty script from OpenAI");

    return { transcript, script };
  } finally {
    safeUnlink(videoPath);
    safeUnlink(audioPath);
  }
}

/* ==============================
   HEYGEN: CREATE VIDEO
   callback_url MUST be root-level
============================== */
async function createHeygenVideo({ scriptText, jobId, avatarId }) {
  const callbackUrl =
    `${HEYGEN_CALLBACK_BASE_URL}` +
    `?token=${encodeURIComponent(HEYGEN_WEBHOOK_SECRET)}` +
    `&job_id=${encodeURIComponent(jobId)}`;

  console.log("🎬 Creating HeyGen video for job:", jobId);
  console.log("Callback URL:", callbackUrl);

  const payload = {
    video_inputs: [
      {
        character: { type: "avatar", avatar_id: avatarId },
        voice: { type: "text", voice_id: HEYGEN_VOICE_ID, input_text: scriptText.trim() },
        background: { type: "color", value: KEY_COLOR_HEX },
      },
    ],
    dimension: { width: 1080, height: 1920 },
    callback_url: callbackUrl,
  };

  const resp = await fetch("https://api.heygen.com/v2/video/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Api-Key": HEYGEN_API_KEY,
    },
    body: JSON.stringify(payload),
  });

  const json = await resp.json().catch(() => null);

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

  const { data: locked, error: lockErr } = await supabase
    .from("render_jobs")
    .update({ status: "processing" })
    .eq("id", jobId)
    .eq("status", "queued")
    .select("*")
    .maybeSingle();

  if (lockErr) throw lockErr;
  if (!locked) return;

  console.log("📦 Processing QUEUED job:", jobId);

  const maxSeconds = Number(locked.max_seconds || 20);
  const avatarType = String(locked.avatar_type || "female").toLowerCase();
  const avatarId = avatarType === "male" ? HEYGEN_AVATAR_ID_MALE : HEYGEN_AVATAR_ID_FEMALE;

  const { transcript, script } = await generateAvatarScriptFromWalkthrough(
    locked.walkthrough_url,
    jobId,
    maxSeconds
  );

  const videoId = await createHeygenVideo({
    scriptText: script,
    jobId,
    avatarId,
  });

  await supabase
    .from("render_jobs")
    .update({
      status: "heygen_requested",
      heygen_video_id: videoId,
      transcript_text: transcript,
      script_text: script,
    })
    .eq("id", jobId);

  console.log("⏳ Waiting for webhook to set status=rendering...");
}

/* ==============================
   PROCESS RENDERING (COMPOSITE)
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
  if (!locked) return;

  if (!locked.heygen_video_url) {
    console.log("⚠️ rendering job has no heygen_video_url yet:", jobId);
    await supabase.from("render_jobs").update({ status: "rendering" }).eq("id", jobId);
    return;
  }

  console.log("🎬 Compositing final video:", jobId);

  const tmp = "/tmp";
  const walkPath = path.join(tmp, `walk-${jobId}.mp4`);
  const avatarPath = path.join(tmp, `avatar-${jobId}.mp4`);
  const finalPath = path.join(tmp, `final-${jobId}.mp4`);

  try {
    await downloadFile(locked.walkthrough_url, walkPath);
    await downloadFile(locked.heygen_video_url, avatarPath);

    // Lower third + better “sit” placement
    // - background normalized to 1080x1920
    // - lower third bar
    // - text: Brand New Listing
    // - avatar: scale + colorkey + slight edge soften (boxblur) to reduce harshness
    // - overlay bottom right, close to ground

    const fontFile =
      process.env.FONT_FILE ||
      "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf";

    const safeText = LT_TEXT.replace(/:/g, "\\:").replace(/'/g, "\\'");

    const filter =
      `[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,` +
      `pad=1080:1920:(ow-iw)/2:(oh-ih)/2,setsar=1,` +
      `drawbox=x=0:y=${LT_BAR_Y}:w=1080:h=${LT_BAR_H}:color=black@0.55:t=fill,` +
      `drawtext=fontfile=${fontFile}:text='${safeText}':fontcolor=white:fontsize=64:` +
      `x=(w-text_w)/2:y=${LT_BAR_Y + 55}[vbg];` +
      `[1:v]scale=${AVATAR_SCALE_W}:-2,format=rgba,` +
      `colorkey=${KEY_COLOR_FFMPEG}:${KEY_SIMILARITY}:${KEY_BLEND},` +
      `boxblur=1:1[av];` + // softens edges a touch (feels less “cut out”)
      `[vbg][av]overlay=x=W-w-${AVATAR_MARGIN_X}:y=H-h-${AVATAR_MARGIN_Y}[outv]`;

    await runFFmpeg([
      "-y",
      "-i", walkPath,
      "-i", avatarPath,
      "-filter_complex", filter,
      "-map", "[outv]",
      "-map", "1:a?",          // HeyGen audio
      "-shortest",
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      "-pix_fmt", "yuv420p",
      "-c:a", "aac",
      "-b:a", "160k",
      "-movflags", "+faststart",
      finalPath,
    ]);

    const storagePath = `renders/final-${jobId}.mp4`;
    const publicUrl = await uploadToStorage(finalPath, storagePath);

    await supabase
      .from("render_jobs")
      .update({
        status: "completed",
        final_public_url: publicUrl,
      })
      .eq("id", jobId);

    console.log("✅ Completed:", jobId, publicUrl);

    if (locked.email) {
      await sendFinalEmail(locked.email, publicUrl);
      console.log("📧 Email sent to:", locked.email);
    }
  } finally {
    safeUnlink(walkPath);
    safeUnlink(avatarPath);
    safeUnlink(finalPath);
  }
}

/* ==============================
   FAIL JOB
============================== */
async function failJob(jobId, err) {
  const msg = String(err?.message || err || "Unknown error");
  console.error("❌ Job failed:", jobId, msg);

  await supabase
    .from("render_jobs")
    .update({ status: "failed", error: msg.slice(0, 2000) })
    .eq("id", jobId);
}

/* ==============================
   MAIN LOOP
============================== */
async function loop() {
  while (true) {
    try {
      const { data: queued, error: qErr } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "queued")
        .order("created_at", { ascending: true })
        .limit(1);

      if (qErr) throw qErr;

      if (queued?.length) {
        const job = queued[0];
        try {
          await processQueued(job);
        } catch (err) {
          await failJob(job.id, err);
        }
        await sleep(POLL_MS);
        continue;
      }

      const { data: rendering, error: rErr } = await supabase
        .from("render_jobs")
        .select("*")
        .eq("status", "rendering")
        .order("created_at", { ascending: true })
        .limit(1);

      if (rErr) throw rErr;

      if (rendering?.length) {
        const job = rendering[0];
        try {
          await processRendering(job);
        } catch (err) {
          await failJob(job.id, err);
        }
      }
    } catch (err) {
      console.error("❌ Worker loop error:", err?.message || err);
    }

    await sleep(POLL_MS);
  }
}

loop();
