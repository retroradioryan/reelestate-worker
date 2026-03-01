app.post("/heygen-callback", async (req, res) => {
  try {
    // respond immediately
    res.json({ ok: true });

    const token = req.query?.token;
    const jobId = req.query?.job_id;

    console.log("Webhook hit for job:", jobId);

    if (process.env.HEYGEN_WEBHOOK_SECRET) {
      if (!token || token !== process.env.HEYGEN_WEBHOOK_SECRET) {
        console.log("Invalid webhook token");
        return;
      }
    }

    if (!jobId) {
      console.log("No job_id provided");
      return;
    }

    // Get job from DB
    const { data: job, error } = await supabase
      .from("render_jobs")
      .select("*")
      .eq("id", jobId)
      .single();

    if (error || !job) {
      console.log("Job not found:", jobId);
      return;
    }

    if (!job.heygen_video_id) {
      console.log("No heygen_video_id stored yet");
      return;
    }

    console.log("Fetching HeyGen video:", job.heygen_video_id);

    const resp = await fetch(
      `https://api.heygen.com/v2/video/${job.heygen_video_id}`,
      {
        headers: {
          "X-Api-Key": process.env.HEYGEN_API_KEY,
        },
      }
    );

    const json = await resp.json().catch(() => null);
    console.log("HeyGen GET response:", JSON.stringify(json, null, 2));

    const videoUrl =
      json?.data?.video_url ||
      json?.data?.url ||
      json?.data?.videos?.[0]?.video_url ||
      json?.data?.videos?.[0]?.url ||
      null;

    if (!videoUrl) {
      console.log("Video URL not ready yet");
      return;
    }

    await supabase
      .from("render_jobs")
      .update({
        status: "rendering",
        heygen_video_url: videoUrl,
      })
      .eq("id", jobId);

    console.log("Job moved to rendering:", jobId);

  } catch (err) {
    console.error("Webhook error:", err);
  }
});
