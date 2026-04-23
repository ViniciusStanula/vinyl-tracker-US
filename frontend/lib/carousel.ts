import { prisma } from "./prisma";
import { slugifyArtist } from "./slugify";
import { fetchTopArtists } from "./lastfm";
import type { ProcessedDisco } from "./queryDiscos";

type CarouselRow = {
  id: string;
  title: string;
  artist: string;
  slug: string;
  style: string | null;
  imgUrl: string | null;
  url: string;
  rating: string | null;
  reviewCount: string | null;
  dealScore: number | null;
  confidenceLevel: string | null;
  lastCrawledAt: Date | null;
  lastfmTags: string | null;
  currentPrice: string;
  avgPrice: string;
  discount: string;
  sparkline: unknown;
};

const DEAL_STALE_MS = 4 * 60 * 60 * 1000;

/**
 * Returns up to 40 deals — best deal per artist — for artists that appear
 * in the Last.fm top-1000 chart. Results are sorted best-deal-first.
 * Returns [] when LASTFM_API_KEY is unset or no matches are found.
 */
export async function queryCarouselDiscos(): Promise<ProcessedDisco[]> {
  try {
  const topArtists = await fetchTopArtists();
  if (topArtists.length === 0) return [];

  const lastfmSlugs = new Set(topArtists.map(slugifyArtist));

  // Fetch all distinct artist names from available records (small payload)
  const dbArtists = await prisma.$queryRaw<{ artist: string }[]>`
    SELECT DISTINCT artist FROM "Record" WHERE available = TRUE
  `;

  // One representative DB artist string per unique slug — prevents duplicate
  // carousel cards when the same artist appears as both "X" and "Y, X" forms.
  const slugToArtist = new Map<string, string>();
  for (const { artist } of dbArtists) {
    const s = slugifyArtist(artist);
    if (lastfmSlugs.has(s) && !slugToArtist.has(s)) {
      slugToArtist.set(s, artist);
    }
  }

  const matchedArtists = [...slugToArtist.values()];
  if (matchedArtists.length === 0) return [];

  // Best deal per matched artist, then sorted globally best-deal-first.
  const rows = await prisma.$queryRaw<CarouselRow[]>`
    WITH best_per_artist AS (
      SELECT DISTINCT ON (d.artist)
        d.id,
        d.title,
        d.artist,
        d.slug,
        d.style,
        d."imgUrl",
        d.url,
        d.rating::text            AS rating,
        d."reviewCount"::text     AS "reviewCount",
        d.deal_score              AS "dealScore",
        d.confidence_level        AS "confidenceLevel",
        d.last_crawled_at         AS "lastCrawledAt",
        d.lastfm_tags             AS "lastfmTags",
        hp_latest."price"                                      AS "currentPrice",
        COALESCE(hp_avg.media, hp_latest."price")              AS "avgPrice",
        (
          SELECT COALESCE(json_agg(sp."price"::float ORDER BY sp."capturedAt"), '[]'::json)
          FROM (
            SELECT "price", "capturedAt"
            FROM   "PriceHistory"
            WHERE  "recordId" = d.id
              AND  "capturedAt" >= NOW() - INTERVAL '30 days'
            ORDER  BY "capturedAt" DESC
            LIMIT  10
          ) sp
        ) AS sparkline
      FROM   "Record" d
      INNER JOIN LATERAL (
        SELECT "price"
        FROM   "PriceHistory"
        WHERE  "recordId" = d.id
        ORDER  BY "capturedAt" DESC
        LIMIT  1
      ) hp_latest ON true
      LEFT JOIN (
        SELECT "recordId", AVG("price") AS media
        FROM   "PriceHistory"
        WHERE  "capturedAt" >= NOW() - INTERVAL '30 days'
        GROUP  BY "recordId"
      ) hp_avg ON hp_avg."recordId" = d.id
      WHERE  d.available = TRUE
        AND  d.artist = ANY(${matchedArtists})
      ORDER  BY d.artist,
               d.deal_score DESC NULLS LAST,
               (COALESCE(hp_avg.media, hp_latest."price") - hp_latest."price")
               / NULLIF(COALESCE(hp_avg.media, hp_latest."price"), 0) DESC NULLS LAST
    )
    SELECT
      *,
      CASE WHEN "avgPrice" > 0
        THEN ("avgPrice" - "currentPrice") / "avgPrice"
        ELSE 0
      END AS discount
    FROM   best_per_artist
    ORDER  BY "dealScore" DESC NULLS LAST, discount DESC NULLS LAST
    LIMIT  40
  `;

  return rows.flatMap((row): ProcessedDisco[] => {
    const currentPrice = Number(row.currentPrice);
    const avgPrice     = Number(row.avgPrice);
    const discount     = Number(row.discount);
    if (isNaN(currentPrice) || isNaN(avgPrice) || isNaN(discount)) return [];

    let sparkline: number[] = [];
    if (Array.isArray(row.sparkline)) {
      sparkline = (row.sparkline as unknown[]).map(Number).filter((n) => !isNaN(n));
    } else if (typeof row.sparkline === "string") {
      try {
        sparkline = (JSON.parse(row.sparkline) as unknown[]).map(Number).filter((n) => !isNaN(n));
      } catch {
        sparkline = [];
      }
    }

    const rawDealScore = row.dealScore != null ? Number(row.dealScore) : null;
    const crawledAt    = row.lastCrawledAt ? new Date(row.lastCrawledAt).getTime() : null;
    const dealIsStale  = crawledAt === null || Date.now() - crawledAt > DEAL_STALE_MS;
    const dealScore    = rawDealScore !== null && !dealIsStale ? rawDealScore : null;

    return [{
      id:              row.id,
      slug:            row.slug,
      title:           row.title,
      artist:          row.artist,
      style:           row.style,
      imgUrl:          row.imgUrl,
      url:             row.url,
      rating:          row.rating != null ? Number(row.rating) : null,
      reviewCount:     row.reviewCount != null ? Number(row.reviewCount) : null,
      currentPrice,
      avgPrice,
      onSale:          dealScore !== null,
      discount,
      sparkline,
      dealScore,
      confidenceLevel: row.confidenceLevel ?? null,
      historyDays:     null,
      lastfmTags:      row.lastfmTags ?? null,
    }];
  });
  } catch {
    return [];
  }
}
