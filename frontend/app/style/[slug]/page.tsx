import { prisma } from "@/lib/prisma";
import DiscoCard from "@/components/DiscoCard";
import SortBar from "@/components/SortBar";
import BackToTop from "@/components/BackToTop";
import Link from "next/link";
import { notFound } from "next/navigation";
import { Suspense, cache } from "react";
import { truncateTitle, truncateDesc } from "@/lib/seo";
import { unstable_cache } from "next/cache";

const ACCENT_FROM = "áàâãäåéèêëíìîïóòôõöúùûüçñý";
const ACCENT_TO   = "aaaaaaeeeeiiiioooouuuucny";

type Sort = "discount" | "lowest-price" | "highest-price" | "top-rated" | "az";

type SerializedStyleData = {
  canonical: string;
  discos: {
    id: string;
    title: string;
    artist: string;
    slug: string;
    imgUrl: string | null;
    url: string;
    style: string | null;
    rating: string | null;
    currentPrice: number;
    avgPrice: number;
    discount: number;
    sparkline: number[];
    dealScore: number | null;
    confidenceLevel: string | null;
    lastCrawledAt: string | null;
  }[];
};

const _getStylePageData = unstable_cache(
  async (slug: string): Promise<SerializedStyleData | null> => {
    const canonicalRow = await prisma.$queryRaw<{ tag: string }[]>`
      WITH tags AS (
        SELECT DISTINCT unnest(string_to_array(lastfm_tags, ', ')) AS tag
        FROM "Record"
        WHERE lastfm_tags IS NOT NULL AND lastfm_tags != ''
      )
      SELECT tag FROM tags
      WHERE regexp_replace(
              regexp_replace(
                translate(lower(tag), ${ACCENT_FROM}, ${ACCENT_TO}),
                '[^a-z0-9]+', '-', 'g'
              ),
              '^-+|-+$', '', 'g'
            ) = ${slug}
      LIMIT 1
    `;

    if (canonicalRow.length === 0) return null;
    const canonical = canonicalRow[0].tag;

    const rows = await prisma.$queryRaw<{
      id: string;
      title: string;
      artist: string;
      slug: string;
      imgUrl: string | null;
      url: string;
      style: string | null;
      rating: string | null;
      dealScore: number | null;
      confidenceLevel: string | null;
      lastCrawledAt: Date | null;
      currentPrice: number;
      avgPrice: number;
      discount: number;
      sparkline: unknown;
    }[]>`
      WITH latest AS (
        SELECT DISTINCT ON ("recordId")
          "recordId", price::float AS preco
        FROM "PriceHistory"
        ORDER BY "recordId", "capturedAt" DESC
      ),
      avgd AS (
        SELECT "recordId", AVG(price)::float AS media
        FROM "PriceHistory"
        WHERE "capturedAt" >= NOW() - INTERVAL '30 days'
        GROUP BY "recordId"
      )
      SELECT
        d.id,
        d.title,
        d.artist,
        d.slug,
        d."imgUrl",
        d.url,
        d.style,
        d.rating::text,
        d.deal_score       AS "dealScore",
        d.confidence_level AS "confidenceLevel",
        d.last_crawled_at  AS "lastCrawledAt",
        l.preco            AS "currentPrice",
        COALESCE(a.media, l.preco) AS "avgPrice",
        CASE
          WHEN COALESCE(a.media, 0) > 0
          THEN (COALESCE(a.media, l.preco) - l.preco) / COALESCE(a.media, l.preco)
          ELSE 0
        END AS discount,
        (
          SELECT COALESCE(
            json_agg(sp.price::float ORDER BY sp."capturedAt"),
            '[]'::json
          )
          FROM (
            SELECT price, "capturedAt"
            FROM "PriceHistory"
            WHERE "recordId" = d.id
              AND "capturedAt" >= NOW() - INTERVAL '30 days'
            ORDER BY "capturedAt" ASC
            LIMIT 10
          ) sp
        ) AS sparkline
      FROM "Record" d
      INNER JOIN latest l ON l."recordId" = d.id
      LEFT  JOIN avgd   a ON a."recordId" = d.id
      WHERE LOWER(${canonical}) = ANY(string_to_array(LOWER(d.lastfm_tags), ', '))
        AND d.available = TRUE
      ORDER BY d.deal_score DESC NULLS LAST, discount DESC NULLS LAST
      LIMIT 96
    `;

    return {
      canonical,
      discos: rows.map((row) => {
        let sparkline: number[] = [];
        if (Array.isArray(row.sparkline)) {
          sparkline = (row.sparkline as unknown[]).map(Number).filter((n) => !isNaN(n));
        } else if (typeof row.sparkline === "string") {
          try {
            sparkline = (JSON.parse(row.sparkline) as unknown[])
              .map(Number)
              .filter((n) => !isNaN(n));
          } catch {
            sparkline = [];
          }
        }
        return {
          id: row.id,
          title: row.title,
          artist: row.artist,
          slug: row.slug,
          imgUrl: row.imgUrl,
          url: row.url,
          style: row.style,
          rating: row.rating ?? null,
          currentPrice: Number(row.currentPrice),
          avgPrice: Number(row.avgPrice),
          discount: Number(row.discount),
          sparkline,
          dealScore:
            row.dealScore !== null && row.dealScore !== undefined
              ? Number(row.dealScore)
              : null,
          confidenceLevel: row.confidenceLevel ?? null,
          lastCrawledAt: row.lastCrawledAt
            ? new Date(row.lastCrawledAt).toISOString()
            : null,
        };
      }),
    };
  },
  ["style-page"],
  { tags: ["prices"] }
);

const getStylePageData = cache(_getStylePageData);

export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  let data;
  try {
    data = await getStylePageData(slug);
  } catch {
    return {};
  }
  if (!data) return {};
  const { canonical } = data;
  const displayName = canonical.replace(/\b\w/g, (c) => c.toUpperCase());
  const title = truncateTitle(`${displayName} — Vinyl Deals & Price History | Vinyl Tracker`);
  const description = truncateDesc(`Best ${displayName} vinyl deals: track price history and find the right record at the lowest price.`);
  return {
    title,
    description,
    alternates: { canonical: `/style/${slug}` },
    openGraph: {
      title,
      description,
      url: `/style/${slug}`,
      type: "website",
    },
    twitter: {
      card: "summary",
      title,
      description,
    },
  };
}

export default async function StylePage({
  params,
  searchParams,
}: {
  params: Promise<{ slug: string }>;
  searchParams: Promise<{ sort?: string; precoMax?: string }>;
}) {
  const { slug } = await params;
  const { sort = "discount", precoMax: precoMaxStr } = await searchParams;
  const precoMax =
    precoMaxStr !== undefined && precoMaxStr !== "" ? Number(precoMaxStr) : null;

  let data: SerializedStyleData | null = null;
  try {
    data = await getStylePageData(slug);
  } catch (err) {
    console.error("[StylePage] getStylePageData failed for slug=%s", slug);
    if (process.env.NODE_ENV === "development") console.error(err);
    return (
      <main className="max-w-7xl mx-auto px-4 py-24 text-center">
        <p className="font-display text-parchment text-lg font-semibold mb-2">
          Error loading style page
        </p>
        <p className="text-dust text-sm">Please try again in a moment.</p>
      </main>
    );
  }
  if (!data) notFound();

  const { canonical, discos } = data;
  const displayName = canonical.replace(/\b\w/g, (c) => c.toUpperCase());

  const DEAL_STALE_MS = 4 * 60 * 60 * 1000;

  const discosProcessados = discos.map((disco) => {
    const crawledAt = disco.lastCrawledAt
      ? new Date(disco.lastCrawledAt).getTime()
      : null;
    const dealIsStale =
      crawledAt === null || Date.now() - crawledAt > DEAL_STALE_MS;
    const dealScore =
      disco.dealScore !== null && !dealIsStale ? disco.dealScore : null;

    return {
      ...disco,
      rating: disco.rating ? Number(disco.rating) : null,
      onSale: dealScore !== null,
      dealScore,
    };
  });

  const fmt = (v: number) =>
    v.toLocaleString("en-US", { style: "currency", currency: "USD" });

  const filtrados =
    precoMax !== null && !isNaN(precoMax)
      ? discosProcessados.filter((d) => d.currentPrice <= precoMax)
      : discosProcessados;

  const sorted = [...filtrados].sort((a, b) => {
    switch (sort as Sort) {
      case "lowest-price":
        return a.currentPrice - b.currentPrice;
      case "highest-price":
        return b.currentPrice - a.currentPrice;
      case "top-rated":
        return (b.rating ?? 0) - (a.rating ?? 0);
      case "az":
        return a.title.localeCompare(b.title, "en-US");
      case "discount":
      default:
        return b.discount - a.discount;
    }
  });

  const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? "http://localhost:3000";

  const breadcrumbJsonLd = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: [
      { "@type": "ListItem", position: 1, name: "Home", item: `${siteUrl}/` },
      {
        "@type": "ListItem",
        position: 2,
        name: displayName,
        item: `${siteUrl}/style/${slug}`,
      },
    ],
  });

  return (
    <main className="max-w-7xl mx-auto px-4 py-8">
      {/* eslint-disable-next-line react/no-danger */}
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: breadcrumbJsonLd }} />
      <nav className="flex items-center gap-1.5 text-sm text-dust mb-6 flex-wrap">
        <Link href="/" className="hover:text-cream transition-colors">
          Home
        </Link>
        <span>›</span>
        <span className="text-parchment">{displayName}</span>
      </nav>

      <header className="mb-6">
        <h1 className="font-display text-3xl font-bold text-cream">
          {displayName}
        </h1>
        <p className="mt-1 text-dust text-sm">
          {sorted.length}{" "}
          {sorted.length === 1 ? "record" : "records"}
          {precoMax !== null && !isNaN(precoMax)
            ? ` up to ${fmt(precoMax)}`
            : " tracked"}
        </p>
      </header>

      <div className="mb-4">
        <Suspense>
          <SortBar />
        </Suspense>
      </div>

      {sorted.length > 0 ? (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
          {sorted.map((disco, index) => (
            <DiscoCard key={disco.id} disco={disco} priority={index < 4} />
          ))}
        </div>
      ) : (
        <div className="text-center py-24 text-dust">
          <div className="inline-block mb-5 opacity-40">
            <svg viewBox="0 0 64 64" fill="none" className="w-16 h-16 mx-auto">
              <circle cx="32" cy="32" r="30" className="fill-gold" opacity="0.3" />
              <circle cx="32" cy="32" r="20" className="fill-record" opacity="0.8" />
              <circle cx="32" cy="32" r="5"  className="fill-gold" opacity="0.4" />
              <circle cx="32" cy="32" r="2"  className="fill-record" />
            </svg>
          </div>
          <p className="font-display text-parchment text-lg font-semibold mb-2">
            No records found
          </p>
          <p className="text-dust text-sm">Try adjusting the filters.</p>
        </div>
      )}

      <BackToTop />
    </main>
  );
}
