import { prisma } from "./prisma";
import { Prisma } from "@prisma/client";

export const PAGE_SIZE = 24;

type Sort = "discount" | "lowest-price" | "highest-price" | "top-rated" | "az" | "deals";

/** Escape LIKE meta-characters in user-supplied text. */
function likePct(term: string): string {
  return `%${term.replace(/[%_\\]/g, "\\$&")}%`;
}

function buildOrderBy(sort: string): Prisma.Sql {
  switch (sort as Sort) {
    case "lowest-price":  return Prisma.sql`"currentPrice" ASC`;
    case "highest-price": return Prisma.sql`"currentPrice" DESC`;
    case "top-rated":     return Prisma.sql`COALESCE(rating::numeric, 0) DESC`;
    case "az":            return Prisma.sql`title ASC`;
    case "deals":         return Prisma.sql`deal_score DESC NULLS LAST, discount DESC NULLS LAST`;
    case "discount":
    default:              return Prisma.sql`discount DESC NULLS LAST, COALESCE("reviewCount", 0) DESC`;
  }
}

type RecordRow = {
  id: string;
  title: string;
  artist: string;
  slug: string;
  style: string | null;
  imgUrl: string | null;
  url: string;
  rating: string | null;
  reviewCount: string | null;
  currentPrice: string;
  avgPrice: string;
  priceCount: string;
  discount: string;
  sparkline: unknown; // json_agg → JS array or string depending on pg driver version
  dealScore: string | null;        // SMALLINT from Record.deal_score
  confidenceLevel: string | null;  // VARCHAR from Record.confidence_level
  historyDays: string | null;      // INTEGER from Record.history_days
  lastCrawledAt: Date | null;      // TIMESTAMPTZ from Record.last_crawled_at
  lastfmTags: string | null;       // TEXT from Record.lastfm_tags
};

export type ProcessedDisco = {
  id: string;
  slug: string;
  title: string;
  artist: string;
  style: string | null;
  imgUrl: string | null;
  url: string;
  rating: number | null;
  reviewCount: number | null;
  currentPrice: number;
  avgPrice: number;
  onSale: boolean;
  discount: number;
  sparkline: number[];
  /** Scoring tier: 1 = Good Deal, 2 = Great Deal, 3 = Best Price, null = no deal */
  dealScore: number | null;
  /** Backend confidence tier identifier; use CONFIDENCE_LABELS in the frontend for display */
  confidenceLevel: string | null;
  /** Days of price history available (used to render trust indicators) */
  historyDays: number | null;
  /** Comma-separated Last.fm genre tags, e.g. "rock, classic rock, hard rock" */
  lastfmTags: string | null;
};

export async function queryDiscos(params: {
  searchTerm: string;
  sort: string;
  artist?: string;
  precoMax: number | null;
  page: number;
}): Promise<{ items: ProcessedDisco[]; total: number; totalPages: number }> {
  const { searchTerm, sort, artist, precoMax, page } = params;

  const whereSearch = searchTerm
    ? Prisma.sql`AND (d.title ILIKE ${likePct(searchTerm)} OR d.artist ILIKE ${likePct(searchTerm)})`
    : Prisma.sql``;
  const whereArtist = artist
    ? Prisma.sql`AND d.artist = ${artist}`
    : Prisma.sql``;
  const wherePrecoMax =
    precoMax !== null && !isNaN(precoMax)
      ? Prisma.sql`AND hp_latest."price" <= ${precoMax}`
      : Prisma.sql``;
  const order = buildOrderBy(sort);

  const [countResult, rows] = await Promise.all([
    prisma.$queryRaw<[{ total: bigint }]>`
      SELECT COUNT(*) AS total
      FROM   "Record" d
      INNER JOIN LATERAL (
        SELECT "price"
        FROM   "PriceHistory"
        WHERE  "recordId" = d.id
          AND  "price" >= 30
        ORDER  BY "capturedAt" DESC
        LIMIT  1
      ) hp_latest ON true
      WHERE  d.available = TRUE ${whereSearch} ${whereArtist} ${wherePrecoMax}
    `,

    prisma.$queryRaw<RecordRow[]>`
      WITH base AS (
        SELECT
          d.id,
          d.title,
          d.artist,
          d.slug,
          d.style,
          d."imgUrl",
          d.url,
          d.rating,
          d."reviewCount",
          d.deal_score        AS "dealScore",
          d.confidence_level  AS "confidenceLevel",
          d.history_days      AS "historyDays",
          d.last_crawled_at   AS "lastCrawledAt",
          d.lastfm_tags       AS "lastfmTags",
          hp_latest."price"                                     AS "currentPrice",
          COALESCE(hp_avg.media, hp_latest."price")             AS "avgPrice",
          COALESCE(hp_avg.cnt, 0)::INTEGER                      AS "priceCount",
          (
            SELECT COALESCE(
              json_agg(sp."price"::float ORDER BY sp."capturedAt"),
              '[]'::json
            )
            FROM (
              SELECT "price", "capturedAt"
              FROM   "PriceHistory"
              WHERE  "recordId" = d.id
                AND  "capturedAt" >= NOW() - INTERVAL '30 days'
                AND  "price" >= 30
              ORDER  BY "capturedAt" DESC
              LIMIT  10
            ) sp
          ) AS sparkline
        FROM   "Record" d
        INNER JOIN LATERAL (
          SELECT "price"
          FROM   "PriceHistory"
          WHERE  "recordId" = d.id
            AND  "price" >= 30
          ORDER  BY "capturedAt" DESC
          LIMIT  1
        ) hp_latest ON true
        LEFT JOIN (
          SELECT
            "recordId",
            AVG("price")         AS media,
            COUNT(*)::INTEGER    AS cnt
          FROM   "PriceHistory"
          WHERE  "capturedAt" >= NOW() - INTERVAL '30 days'
            AND  "price" >= 30
          GROUP  BY "recordId"
        ) hp_avg ON hp_avg."recordId" = d.id
        WHERE d.available = TRUE ${whereSearch} ${whereArtist} ${wherePrecoMax}
      )
      SELECT
        *,
        CASE WHEN "avgPrice" > 0
          THEN ("avgPrice" - "currentPrice") / "avgPrice"
          ELSE 0
        END AS discount
      FROM  base
      ORDER BY ${order}
      LIMIT  ${PAGE_SIZE}
      OFFSET ${(page - 1) * PAGE_SIZE}
    `,
  ]);

  const total = Number(countResult[0].total);
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const items = rows.flatMap((row): ProcessedDisco[] => {
    const currentPrice = Number(row.currentPrice);
    const avgPrice     = Number(row.avgPrice);
    const discount     = Number(row.discount);

    // Guard against NaN from corrupted DB values — Number("abc") === NaN which
    // would propagate silently through all price calculations and UI rendering.
    if (isNaN(currentPrice) || isNaN(avgPrice) || isNaN(discount)) {
      // eslint-disable-next-line no-console
      console.warn("[queryDiscos] NaN numeric field for record id=%s — skipping row", row.id);
      return [];
    }

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

    const rawDealScore =
      row.dealScore !== null && row.dealScore !== undefined
        ? Number(row.dealScore)
        : null;

    // Suppress deal badge if the crawler hasn't confirmed this product in the
    // last 4 hours. Protects against stale data when the crawler hasn't run or
    // failed to re-validate an active deal in Phase 0.
    const DEAL_STALE_MS = 4 * 60 * 60 * 1000;
    const crawledAt = row.lastCrawledAt ? new Date(row.lastCrawledAt).getTime() : null;
    const dealIsStale = crawledAt === null || Date.now() - crawledAt > DEAL_STALE_MS;
    const dealScore = rawDealScore !== null && !dealIsStale ? rawDealScore : null;

    return [{
      id:             row.id,
      slug:           row.slug,
      title:          row.title,
      artist:         row.artist,
      style:          row.style,
      imgUrl:         row.imgUrl,
      url:            row.url,
      rating:         row.rating !== null && row.rating !== undefined ? Number(row.rating) : null,
      reviewCount:    row.reviewCount !== null && row.reviewCount !== undefined ? Number(row.reviewCount) : null,
      currentPrice,
      avgPrice,
      onSale:         dealScore !== null,
      discount,
      sparkline,
      dealScore,
      confidenceLevel: row.confidenceLevel ?? null,
      historyDays:    row.historyDays !== null && row.historyDays !== undefined ? Number(row.historyDays) : null,
      lastfmTags:     row.lastfmTags ?? null,
    }];
  });

  return { items, total, totalPages };
}
