import express from "express";
import { execSync } from "child_process";
import { createClient } from "@supabase/supabase-js";

const app = express();
app.use(express.json());

// health routes
app.get("/", (req, res) => {
  res.json({ ok: true, service: "reelestate-worker" });
});

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

// verify ffmpeg is installed
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

// optional: quick supabase sanity check (doesn't upload anything)
app.get("/supabase", async (req, res) => {
  try {
    const url = process.env.SUPABASE_URL;
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!url || !key) {
      return res.status(400).json({ ok: false, error: "Missing env vars" });
    }

    const supabase = createClient(url, key);

    // list buckets (requires service role)
    const { data, error } = await supabase.storage.listBuckets();
    if (error) throw error;

    res.json({ ok: true, buckets: data?.map((b) => b.name) || [] });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

const PORT = process.env.PORT || 10000;

app.listen(PORT, () => {
  console.log(`Worker running on port ${PORT}`);
});
