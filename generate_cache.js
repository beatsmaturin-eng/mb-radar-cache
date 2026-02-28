// generate_cache.js
// Node 20+
// Genera cache.json consumiendo las URLs listadas en sources.json

import fs from "fs";
import path from "path";

const SOURCES_PATH = path.join(process.cwd(), "sources.json");
// --- iTunes enrich (no auth) ---
// Persists small cache in repo to avoid re-querying.
const ITUNES_CACHE_PATH = path.join(__dirname, "itunes_cache.json");
function loadItunesCache(){
  try { return JSON.parse(fs.readFileSync(ITUNES_CACHE_PATH,"utf8")); } catch { return {}; }
}
function saveItunesCache(cache){
  fs.writeFileSync(ITUNES_CACHE_PATH, JSON.stringify(cache, null, 2), "utf8");
}
function normKey(s){ return String(s||"").toLowerCase().replace(/\s+/g," ").trim(); }
function trackKey(artist, title){ return `${normKey(artist)} — ${normKey(title)}`; }

async function itunesLookup(artist, title){
  const term = encodeURIComponent(`${artist} ${title}`.trim());
  const url = `https://itunes.apple.com/search?term=${term}&entity=song&limit=1`;
  const res = await fetch(url, { headers: { "User-Agent": "MB-RadarCache/1.0" }});
  if(!res.ok) throw new Error(`itunes_http_${res.status}`);
  const json = await res.json();
  const item = (json.results && json.results[0]) ? json.results[0] : null;
  if(!item) return null;
  return {
    itunes_genre: item.primaryGenreName || "",
    release_date: item.releaseDate ? String(item.releaseDate).slice(0,10) : "",
    track_view_url: item.trackViewUrl || "",
    artwork: (item.artworkUrl100 || item.artworkUrl60 || item.artworkUrl30 || "").replace("100x100", "300x300")
  };
}

function calcAgeDays(releaseDateISO){
  if(!releaseDateISO) return null;
  const d = new Date(releaseDateISO);
  if(isNaN(d.getTime())) return null;
  const now = new Date();
  const diff = now.getTime() - d.getTime();
  return Math.max(0, Math.floor(diff / (1000*60*60*24)));
}

async function enrichWithItunes(items, errors){
  const cache = loadItunesCache();
  let used = 0;

  // Enrich unique tracks only once (items are already de-duped).
  // Concurrency limiter:
  const CONCURRENCY = 6;
  let i = 0;

  async function worker(){
    while(i < items.length){
      const idx = i++;
      const it = items[idx];
      const key = trackKey(it.artist, it.title);
      if(cache[key]){
        const c = cache[key];
        it.itunes_genre = it.itunes_genre || c.itunes_genre || "";
        it.release_date = it.release_date || c.release_date || "";
        it.itunes_artwork = it.itunes_artwork || c.artwork || "";
        it.release_year = it.release_year || (it.release_date ? Number(String(it.release_date).slice(0,4)) : null);
        it.age_days = it.age_days ?? calcAgeDays(it.release_date);
        continue;
      }
      try{
        const found = await itunesLookup(it.artist, it.title);
        if(found){
          cache[key] = found;
          used++;
          it.itunes_genre = it.itunes_genre || found.itunes_genre || "";
          it.release_date = it.release_date || found.release_date || "";
          it.itunes_artwork = it.itunes_artwork || found.artwork || "";
          it.release_year = it.release_year || (it.release_date ? Number(String(it.release_date).slice(0,4)) : null);
          it.age_days = it.age_days ?? calcAgeDays(it.release_date);
        } else {
          cache[key] = { itunes_genre:"", release_date:"", track_view_url:"", artwork:"" };
        }
      }catch(e){
        errors.push({ source: "iTunes", error: "itunes_lookup_failed", detail: String(e.message||e), track: `${it.artist} - ${it.title}` });
      }
    }
  }

  const workers = Array.from({length: CONCURRENCY}, ()=>worker());
  await Promise.all(workers);
  saveItunesCache(cache);
  return used;
}


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
      genre_label: "",
      release_date: "",
      release_year: null,
      age_days: null,
      freshness_code: "unknown",
      freshness_label: "",
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

  const itunes_used = await enrichWithItunes(unique, errors);

  const payload = {
    ok: true,
    generated_at: nowISO(),
    raw_count: all.length,
    count: unique.length,
    sources_count: sources.length,
    itunes_used: itunes_used,
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
