const BASE = "https://ws.audioscrobbler.com/2.0/";

interface LfmArtist {
  name: string;
}
interface LfmResponse {
  artists?: { artist?: LfmArtist[] };
}

async function fetchPage(apiKey: string, page: number): Promise<LfmArtist[]> {
  const url =
    `${BASE}?method=chart.getTopArtists` +
    `&api_key=${encodeURIComponent(apiKey)}` +
    `&format=json&limit=500&page=${page}`;

  // next.revalidate uses Next.js's persistent fetch data cache (survives HMR).
  const res = await fetch(url, { next: { revalidate: 86400 } });
  if (!res.ok) return [];
  const d = (await res.json()) as LfmResponse;
  return d.artists?.artist ?? [];
}

export async function fetchTopArtists(): Promise<string[]> {
  const key = process.env.LASTFM_API_KEY;
  if (!key) return [];

  const [p1, p2] = await Promise.all([
    fetchPage(key, 1).catch(() => [] as LfmArtist[]),
    fetchPage(key, 2).catch(() => [] as LfmArtist[]),
  ]);

  return [...new Set([...p1, ...p2].map((a) => a.name).filter(Boolean))];
}
