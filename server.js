import express from "express";
import { execSync } from "child_process";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

/* ----------------------------------
   BASIC HEALTH ROUTES
---------------------------------- */

app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

/* ----------------------------------
   CHECK FFMPEG
---------------------------------- */

app.get("/ffmpeg", (req, res) => {
  try {
    const out = execSync("ffmpeg -version").toString();
    res.type("text").send(out);
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: "ffmpeg not found",
      details: String(e),
    });
  }
});

/* ----------------------------------
   CHECK SUPABASE CONNECTION
---------------------------------- */

app.get("/supabase", async (req, res) => {
  try {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !key) {
      return res.status(400).json({ ok: false, error: "Missing env vars" });
    }

    const supabase = createClient(url, key);
    const { data, error } = await supabase.storage.listBuckets();
    if (error) throw error;

    res.json({ ok: true, buckets: data?.map((b) => b.name) || [] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/* ----------------------------------
   RENDER TEST (LIGHTWEIGHT VERSION)
---------------------------------- */

app.post("/render-test", async (req, res) => {
  try {
    const bucket = process.env.STORAGE_BUCKET || "videos";
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !key) {
      return res.status(400).json({ ok: false, error: "Missing env vars" });
    }

    const supabase = createClient(url, key);

    // Smaller sample video (more stable)
    const sampleUrl =
      req.body?.sampleUrl ||
      "https://storage.googleapis.com/gtv-videos-bucket/sample/ForBiggerEscapes.mp4";

    const tmpDir = "/tmp";
    const inputPath = path.join(tmpDir, `in-${Date.now()}.mp4`);
    const outputPath = path.join(tmpDir, `out-${Date.now()}.mp4`);

    /* -------- DOWNLOAD -------- */

    const response = await fetch(sampleUrl);
    if (!response.ok) throw new Error("Failed to download sample video");

    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(inputPath, buffer);

    /* -------- FFMPEG (LIGHTWEIGHT ENCODE) -------- */

    execSync(
      `ffmpeg -y -i "${inputPath}" -t 3 -c:v libx264 -preset ultrafast -crf 32 -an "${outputPath}"`,
      { stdio: "ignore" }
    );

    /* -------- UPLOAD TO SUPABASE -------- */

    const file = fs.readFileSync(outputPath);
    const filePath = `tests/test-${Date.now()}.mp4`;

    const { data, error } = await supabase.storage
      .from(bucket)
      .upload(filePath, file, {
        contentType: "video/mp4",
        upsert: true,
      });

    if (error) throw error;

    const { data: publicUrl } = supabase.storage
      .from(bucket)
      .getPublicUrl(data.path);

    res.json({
      ok: true,
      bucket,
      path: data.path,
      url: publicUrl.publicUrl,
    });

  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

/* ----------------------------------
   START SERVER
---------------------------------- */

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`Worker running on port ${PORT}`);
});
