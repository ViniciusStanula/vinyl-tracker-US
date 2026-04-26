import { prisma } from "@/lib/prisma";
import Image from "next/image";
import Link from "next/link";
import { notFound } from "next/navigation";
import { cache } from "react";
import GraficoPreco from "@/components/GraficoPreco";
import DiscoCard from "@/components/DiscoCard";
import BackToTop from "@/components/BackToTop";
import StyleTags from "@/components/StyleTags";
import { slugifyArtist } from "@/lib/slugify";
import { parseStyleTags } from "@/lib/styleUtils";
import { truncateTitle, truncateDesc } from "@/lib/seo";

export const revalidate = 7200;

const getRecord = cache(async (slug: string) => {
  const oneYearAgo = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);
  return prisma.record.findUnique({
    where: { slug },
    include: {
      prices: {
        where: { capturedAt: { gte: oneYearAgo } },
        orderBy: { capturedAt: "asc" },
      },
    },
  });
});

export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  try {
    const disco = await getRecord(slug);
    if (!disco) return {};
    const title = truncateTitle(`${disco.title} — ${disco.artist} on Vinyl | Price History`);
    const description = truncateDesc(`Buy ${disco.title} by ${disco.artist} at the best price. View full price history and the best deals available now.`);
    return {
      title,
      description,
      alternates: { canonical: `/record/${slug}` },
      openGraph: {
        title,
        description,
        url: `/record/${slug}`,
        type: "website",
        ...(disco.imgUrl ? { images: [{ url: disco.imgUrl, alt: disco.title }] } : {}),
      },
      twitter: {
        card: disco.imgUrl ? "summary_large_image" : "summary",
        title,
        description,
      },
    };
  } catch {
    return {};
  }
}

type RelatedDeal = {
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
  sparkline: unknown;
  dealScore: number | null;
  confidenceLevel: string | null;
};

export default async function RecordPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;

  const disco = await getRecord(slug);

  if (!disco) notFound();

  const metaRow = await prisma.$queryRaw<[{ available: boolean; lastfmTags: string | null }]>`
    SELECT available, lastfm_tags AS "lastfmTags" FROM "Record" WHERE slug = ${slug}
  `;
  const available = metaRow[0]?.available ?? true;
  const artistLower = disco.artist.toLowerCase();
  const styleTags = parseStyleTags(metaRow[0]?.lastfmTags ?? null)
    .filter((t) => t.toLowerCase() !== artistLower)
    .slice(0, 5);

  const values = disco.prices.map((p) => Number(p.price));
  const currentPrice = values.at(-1) ?? 0;
  const priceMin = values.length ? Math.min(...values) : currentPrice;
  const priceMax = values.length ? Math.max(...values) : currentPrice;
  const avgPrice =
    values.length > 0
      ? values.reduce((a, b) => a + b, 0) / values.length
      : currentPrice;
  const discount = avgPrice > 0 ? ((avgPrice - currentPrice) / avgPrice) * 100 : 0;

  const minRecord =
    disco.prices.length > 0
      ? disco.prices.reduce((a, b) =>
          Number(a.price) < Number(b.price) ? a : b
        )
      : null;
  const maxRecord =
    disco.prices.length > 0
      ? disco.prices.reduce((a, b) =>
          Number(a.price) > Number(b.price) ? a : b
        )
      : null;

  const priceStatus: "all-time-low" | "increase" | "stable" | null =
    values.length >= 2
      ? currentPrice <= priceMin
        ? "all-time-low"
        : currentPrice > avgPrice * 1.03
        ? "increase"
        : "stable"
      : null;

  const EST = "America/New_York";

  const fmt = (v: number) =>
    v.toLocaleString("en-US", { style: "currency", currency: "USD" });

  const fmtDate = (d: Date) =>
    d.toLocaleDateString("en-US", { timeZone: EST });

  const fmtTime = (d: Date) =>
    d.toLocaleTimeString("en-US", { timeZone: EST, hour: "2-digit", minute: "2-digit" });

  const fmtDateTime = (d: Date) => `${fmtDate(d)}, ${fmtTime(d)}`;

  const latestCapturedAt = disco.prices.at(-1)?.capturedAt;

  const isToday =
    latestCapturedAt
      ? latestCapturedAt.toLocaleDateString("en-US", { timeZone: EST }) ===
        new Date().toLocaleDateString("en-US", { timeZone: EST })
      : false;
  const latestLabel = isToday
    ? `Today, ${fmtTime(latestCapturedAt!)}`
    : latestCapturedAt
    ? fmtDateTime(latestCapturedAt)
    : "—";

  const rating = disco.rating ? Number(disco.rating) : null;
  const stars = rating ? Math.round(rating) : 0;

  const chartPoints = disco.prices.map((p) => ({
    date: p.capturedAt.toLocaleDateString("en-US", {
      timeZone: EST,
      month: "2-digit",
      day: "2-digit",
    }),
    dateFull: fmtDateTime(p.capturedAt),
    value: Number(p.price),
  }));

  const pricesDisplay = [...disco.prices].reverse();

  const relatedDeals = await prisma.$queryRaw<RelatedDeal[]>`
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
      d.rating,
      d.deal_score                                         AS "dealScore",
      d.confidence_level                                   AS "confidenceLevel",
      l.preco                                              AS "currentPrice",
      COALESCE(a.media, l.preco)                           AS "avgPrice",
      CASE
        WHEN COALESCE(a.media, 0) > 0
        THEN (COALESCE(a.media, l.preco) - l.preco) / COALESCE(a.media, l.preco)
        ELSE 0
      END                                                  AS discount,
      (
        SELECT COALESCE(
          json_agg(sp.price::float ORDER BY sp."capturedAt"),
          '[]'::json
        )
        FROM (
          SELECT price, "capturedAt"
          FROM   "PriceHistory"
          WHERE  "recordId" = d.id
            AND  "capturedAt" >= NOW() - INTERVAL '30 days'
          ORDER  BY "capturedAt" ASC
          LIMIT  10
        ) sp
      ) AS sparkline
    FROM "Record" d
    INNER JOIN latest l ON l."recordId" = d.id
    LEFT  JOIN avgd   a ON a."recordId" = d.id
    WHERE d.id != ${disco.id}
      AND d.deal_score IS NOT NULL
      AND d.available = TRUE
    ORDER BY d.deal_score DESC, RANDOM()
    LIMIT 4
  `;

  const processedDeals = relatedDeals.map((deal) => {
    let sparkline: number[] = [];
    if (Array.isArray(deal.sparkline)) {
      sparkline = (deal.sparkline as unknown[]).map(Number).filter((n) => !isNaN(n));
    } else if (typeof deal.sparkline === "string") {
      try {
        sparkline = (JSON.parse(deal.sparkline) as unknown[]).map(Number).filter((n) => !isNaN(n));
      } catch {
        sparkline = [];
      }
    }
    return {
      ...deal,
      rating:          deal.rating ? Number(deal.rating) : null,
      onSale:          true,
      dealScore:       deal.dealScore !== null && deal.dealScore !== undefined ? Number(deal.dealScore) : null,
      confidenceLevel: deal.confidenceLevel ?? null,
      sparkline,
    };
  });

  const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? "http://localhost:3000";

  const productJsonLd = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "Product",
    name: disco.title,
    image: disco.imgUrl ?? undefined,
    brand: { "@type": "Brand", name: disco.artist },
    offers: {
      "@type": "Offer",
      url: disco.url,
      priceCurrency: "USD",
      price: currentPrice.toFixed(2),
      availability: available
        ? "https://schema.org/InStock"
        : "https://schema.org/OutOfStock",
      seller: { "@type": "Organization", name: "Amazon" },
    },
    ...(rating && disco.reviewCount && disco.reviewCount > 0
      ? {
          aggregateRating: {
            "@type": "AggregateRating",
            ratingValue: rating.toFixed(1),
            reviewCount: disco.reviewCount,
            bestRating: "5",
            worstRating: "1",
          },
        }
      : {}),
  });

  const breadcrumbJsonLd = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: [
      { "@type": "ListItem", position: 1, name: "Home", item: `${siteUrl}/` },
      {
        "@type": "ListItem",
        position: 2,
        name: disco.artist,
        item: `${siteUrl}/artist/${slugifyArtist(disco.artist)}`,
      },
      { "@type": "ListItem", position: 3, name: disco.title },
    ],
  });

  return (
    <main className="max-w-3xl mx-auto px-4 py-8">
      {/* eslint-disable-next-line react/no-danger */}
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: productJsonLd }} />
      {/* eslint-disable-next-line react/no-danger */}
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: breadcrumbJsonLd }} />
      <nav className="flex items-center gap-1.5 text-sm text-dust mb-6 flex-wrap">
        <Link href="/" className="hover:text-cream transition-colors">
          Home
        </Link>
        <span>›</span>
        <Link
          href={`/artist/${slugifyArtist(disco.artist)}`}
          className="hover:text-cream transition-colors"
        >
          {disco.artist}
        </Link>
        <span>›</span>
        <span className="text-parchment truncate max-w-[200px] sm:max-w-xs">
          {disco.title}
        </span>
      </nav>

      <div className="flex flex-col sm:flex-row gap-6 mb-8">
        {disco.imgUrl && (
          <div className="relative w-full sm:w-72 sm:h-72 aspect-square sm:aspect-auto shrink-0 bg-label rounded-2xl overflow-hidden">
            <Image
              src={disco.imgUrl}
              alt={disco.title}
              fill
              sizes="(max-width: 640px) 100vw, 288px"
              className="object-cover"
              unoptimized
              priority
            />
          </div>
        )}

        <div className="flex-1 flex flex-col justify-between">
          <div>
            <Link
              href={`/artist/${slugifyArtist(disco.artist)}`}
              className="text-parchment hover:text-gold text-sm transition-colors font-medium"
            >
              {disco.artist}
            </Link>
            <h1 className="font-display text-2xl font-bold text-cream mt-1 leading-tight">
              {disco.title}
            </h1>
            {rating && (
              <p className="text-sm mt-2 flex items-center gap-1">
                <span className="text-gold" aria-hidden="true">
                  {"★".repeat(stars)}
                  {"☆".repeat(5 - stars)}
                </span>
                <span className="text-dust ml-0.5" aria-label={`Rating: ${rating.toFixed(1)} out of 5`}>{rating.toFixed(1)}</span>
              </p>
            )}
            <StyleTags tags={styleTags} />
          </div>

          <div className="mt-5">
            <div className="flex flex-wrap items-center gap-2 mb-1">
              <span className="font-display text-4xl sm:text-5xl font-black text-gold leading-none tabular-nums">
                {fmt(currentPrice)}
              </span>
              {Math.abs(discount) >= 1 && (
                <span
                  className={`text-sm font-bold px-2.5 py-1 rounded-lg ${
                    discount >= 10
                      ? "bg-deal/20 text-deallit"
                      : discount > 0
                      ? "bg-groove text-parchment"
                      : "bg-cut/20 text-cut"
                  }`}
                >
                  {discount >= 0 ? "▼" : "▲"} {Math.abs(discount).toFixed(1)}%
                </span>
              )}
            </div>

            {priceStatus === "all-time-low" && (
              <span className="inline-block text-xs bg-deal text-cream font-bold px-3 py-1 rounded-full mb-1">
                ↓ All-Time Low
              </span>
            )}
            {priceStatus === "increase" && (
              <span className="inline-block text-xs bg-cut/20 text-cut font-bold px-3 py-1 rounded-full border border-cut/40 mb-1">
                ↑ Price Increase
              </span>
            )}
            {priceStatus === "stable" && (
              <span className="inline-block text-xs bg-groove text-parchment font-semibold px-3 py-1 rounded-full border border-wax/50 mb-1">
                → Stable Price
              </span>
            )}

            <p className="text-dust text-sm">
              vs. historical average{" "}
              <span className="line-through text-ash">{fmt(avgPrice)}</span>
            </p>

            <div className="flex flex-wrap items-center gap-3 mt-5">
              {available ? (
                <a
                  href={disco.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-2 bg-gold hover:bg-goldlit text-record font-bold text-sm px-6 py-3 rounded-full transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40 focus-visible:ring-offset-2 focus-visible:ring-offset-record"
                >
                  View on Amazon
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                  </svg>
                </a>
              ) : (
                <span className="inline-flex items-center gap-2 bg-groove text-dust font-bold text-sm px-6 py-3 rounded-full cursor-not-allowed border border-wax/50">
                  Unavailable
                </span>
              )}
            </div>
            <p className="text-ash text-xs mt-2">
              Prices may vary
            </p>
          </div>
        </div>
      </div>

      <section className="bg-sleeve rounded-xl border border-groove p-5 mb-6">
        <h2 className="font-display text-base font-semibold text-cream mb-4">
          Price History
          <span className="text-dust text-sm font-normal ml-2">· {latestLabel}</span>
        </h2>

        <div className="grid grid-cols-2 gap-3 mb-5">
          <div className="bg-groove rounded-lg p-3 border-l-4 border-deal">
            <p className="text-[11px] text-dust mb-1 flex items-center gap-1">
              Lowest <span className="text-deallit text-[10px] font-bold">↓</span>
            </p>
            <p className="font-bold text-deallit text-sm tabular-nums">{fmt(priceMin)}</p>
            {minRecord && (
              <p className="text-[10px] text-dust mt-0.5">{fmtDateTime(minRecord.capturedAt)}</p>
            )}
          </div>

          <div className="bg-groove rounded-lg p-3 border-l-4 border-cut">
            <p className="text-[11px] text-dust mb-1 flex items-center gap-1">
              Highest <span className="text-cut text-[10px] font-bold">↑</span>
            </p>
            <p className="font-bold text-cut text-sm tabular-nums">{fmt(priceMax)}</p>
            {maxRecord && (
              <p className="text-[10px] text-dust mt-0.5">{fmtDateTime(maxRecord.capturedAt)}</p>
            )}
          </div>
        </div>

        <GraficoPreco points={chartPoints} />

        {values.length > 1 && (
          <details className="mt-4">
            <summary className="text-xs text-dust cursor-pointer hover:text-cream select-none transition-colors">
              View all entries
            </summary>
            <div className="mt-3 max-h-52 overflow-y-auto">
              <table className="w-full text-xs text-left">
                <thead>
                  <tr className="text-ash border-b border-groove">
                    <th className="pb-2 font-medium">Date</th>
                    <th className="pb-2 font-medium text-right">Price</th>
                    <th className="pb-2 font-medium text-right">Change</th>
                  </tr>
                </thead>
                <tbody>
                  {pricesDisplay.map((p, i) => {
                    const prev = pricesDisplay[i + 1];
                    const curr = Number(p.price);
                    const delta = prev ? curr - Number(prev.price) : null;
                    return (
                      <tr key={i} className="border-b border-groove/50">
                        <td className="py-1.5 text-dust">{fmtDateTime(p.capturedAt)}</td>
                        <td className="py-1.5 text-right font-medium text-cream tabular-nums">
                          {curr.toLocaleString("en-US", { style: "currency", currency: "USD" })}
                        </td>
                        <td
                          className={`py-1.5 text-right tabular-nums ${
                            delta === null
                              ? "text-ash"
                              : delta > 0
                              ? "text-cut"
                              : delta < 0
                              ? "text-deallit"
                              : "text-dust"
                          }`}
                        >
                          {delta === null
                            ? "—"
                            : delta === 0
                            ? "="
                            : `${delta > 0 ? "▲" : "▼"} ${Math.abs(delta).toLocaleString("en-US", { style: "currency", currency: "USD" })}`}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </details>
        )}
      </section>

      {processedDeals.length > 0 && (
        <section className="mt-10">
          <h2 className="font-display text-lg font-semibold text-cream mb-4">
            More records on sale
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {processedDeals.map((deal) => (
              <DiscoCard key={deal.id} disco={deal} />
            ))}
          </div>
        </section>
      )}

      <BackToTop />
    </main>
  );
}
