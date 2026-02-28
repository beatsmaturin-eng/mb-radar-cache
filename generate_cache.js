// generate_cache.js
// Node 20+
// Genera cache.json consumiendo las URLs listadas en sources.json

import fs from "fs";
import path from "path";

const SOURCES_PATH = path.join(process.cwd(), "sources.json");
const OUT_PATH = path.join(process.cwd(), "cache.json");

function nowISO() {
  return new Date().toISOString();
}

function normalizeSpaces(s) {
  return String(s || "")
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanTrackText(s) {
  s = normalizeSpaces(s);
  s = s.replace(/^(#?\s*\d+\s*[.)-]\s*)/u, "");
  return s.trim();
}

function splitArtistTitle(track) {
  track = cleanTrackText(track);
  const seps = [" - ", " — ", " – ", " : "];
  for (const sep of seps) {
    if (track.includes(sep)) {
      const [a, t] = track.split(sep, 2).map(x => x.trim());
      if (a && t) return [a, t];
    }
  }
  const m = track.match(/^(.+?)\s*[-–—:]\s*(.+)$/u);
  if (m) return [m[1].trim(), m[2].trim()];
  return ["", track];
}

function normKey(artist, title) {
  const s = (artist + " - " + title).toLowerCase()
    .replace(/[^a-z0-9\u00c0-\u017f\s\-]/gu, "")
    .replace(/\s+/g, " ")
    .trim();
  return s;
}

function detectSourceType(url) {
  const u = url.toLowerCase();
  if (u.includes("kworb.net/youtube/insights/")) return "youtube_insights";
  if (u.includes("kworb.net/spotify/country/")) return "spotify_country";
  if (u.includes("kworb.net/charts/deezer/")) return "deezer_chart";
  if (u.includes("kworb.net/charts/itunes/")) return "itunes_chart";
  return "unknown";
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9,es;q=0.8,pt;q=0.7",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.text();
}

function parseKworbTracks(html, sourceType, sourceName, sourceUrl, region, bucket, max = 200) {
  const items = [];
  if (!html || html.length < 800) return items;

  const rowMatches = [...html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)];
  let pos = 0;

  for (const rm of rowMatches) {
    if (items.length >= max) break;
    const row = rm[1];

    // pos
    const mNum = row.match(/class="num"[^>]*>\s*([0-9]{1,3})\s*</i);
    if (mNum) pos = parseInt(mNum[1], 10);
    else {
      const mAny = row.match(/>\s*([0-9]{1,3})\s*</);
      pos = mAny ? parseInt(mAny[1], 10) : (pos + 1);
    }

    // track text: link-first
    let track = "";
    const aMatches = [...row.matchAll(/<a[^>]*>([\s\S]*?)<\/a>/gi)];
    for (const am of aMatches) {
      const t = cleanTrackText(am[1]);
      if (!t) continue;
      if (/[-–—:]/u.test(t) && t.length >= 6) { track = t; break; }
    }

    // fallback: td más largo
    if (!track) {
      const tdMatches = [...row.matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)];
      let best = "";
      for (const tm of tdMatches) {
        const t = cleanTrackText(tm[1]);
        if (!t) continue;
        if (t.length > best.length && /[A-Za-z\u00c0-\u017f]/u.test(t)) best = t;
      }
      track = best;
    }

    if (!track) continue;

    const [artist, title] = splitArtistTitle(track);

    items.push({
      source_type: sourceType,
      source_name: sourceName,
      source_url: sourceUrl,
      region,
      bucket,
      pos,
      track_raw: track,
      artist,
      title,
      streams: null,
      delta: null,
      itunes_genre: "",
      genre_label: "Pop Latino",
      release_date: "",
      release_year: null,
      age_days: null,
      freshness_code: "unknown",
      freshness_label: "Sin fecha",
      youtube_video_id: "",
      youtube_url: "",
      cover_url: "",
      published: nowISO(),
    });
  }

  return items;
}

function aggregateAndDedup(items) {
  const map = new Map();

  for (const it of items) {
    const artist = it.artist || "";
    const title = it.title || it.track_raw || "";
    const k = normKey(artist, title);
    if (!k || k === "-") continue;

    if (!map.has(k)) {
      map.set(k, {
        ...it,
        sources_positions: [],
        best_pos: null,
        avg_pos: null,
      });
    }

    const entry = map.get(k);
    entry.sources_positions.push({
      source_name: it.source_name || "",
      bucket: it.bucket || "",
      region: it.region || "",
      pos: it.pos ?? null,
    });

    if (it.pos) {
      if (entry.best_pos === null || it.pos < entry.best_pos) entry.best_pos = it.pos;
    }
  }

  const out = [...map.values()];
  for (const it of out) {
    const ps = it.sources_positions.map(x => x.pos).filter(Boolean);
    if (ps.length) it.avg_pos = Math.round(ps.reduce((a, b) => a + b, 0) / ps.length);
    const bp = it.best_pos ?? 999;
    it.score = Math.max(1, 200 - bp);
  }

  out.sort((a, b) => (b.score ?? 0) - (a.score ?? 0));
  return out;
}

async function main() {
  const sources = JSON.parse(fs.readFileSync(SOURCES_PATH, "utf8"));
  const all = [];
  const errors = [];

  for (const src of sources) {
    const url = src.url;
    const name = src.name || url;
    const region = src.region || "";
    const bucket = src.bucket || "";
    const st = detectSourceType(url);

    try {
      const html = await fetchText(url);
      const items = parseKworbTracks(html, st, name, url, region, bucket, 200);
      if (!items.length) {
        errors.push({ source: name, error: "parse_empty" });
      } else {
        all.push(...items);
      }
    } catch (e) {
      errors.push({ source: name, error: "fetch_failed", detail: String(e.message || e) });
    }
  }

  const unique = aggregateAndDedup(all);

  const payload = {
    ok: true,
    generated_at: nowISO(),
    raw_count: all.length,
    count: unique.length,
    sources_count: sources.length,
    itunes_used: 0,
    yt_used: 0,
    errors,
    items: unique,
  };

  fs.writeFileSync(OUT_PATH, JSON.stringify(payload, null, 2), "utf8");
  console.log("Saved cache.json", { raw: payload.raw_count, count: payload.count, errors: payload.errors.length });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
