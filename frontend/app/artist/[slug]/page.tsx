import { prisma } from "@/lib/prisma";
import DiscoCard from "@/components/DiscoCard";
import SortBar from "@/components/SortBar";
import BackToTop from "@/components/BackToTop";
import StyleTags from "@/components/StyleTags";
import Link from "next/link";
import { notFound } from "next/navigation";
import { slugifyArtist } from "@/lib/slugify";
import { truncateTitle, truncateDesc } from "@/lib/seo";
import { getTopStyles } from "@/lib/styleUtils";
import { Suspense, cache } from "react";
import { unstable_cache } from "next/cache";

const ACCENT_FROM = "áàâãäåéèêëíìîïóòôõöúùûüçñý";
const ACCENT_TO   = "aaaaaaeeeeiiiioooouuuucny";

type Sort = "discount" | "lowest-price" | "highest-price" | "top-rated" | "az";

type SerializedPageData = {
  canonical: string;
  discos: {
    id: string;
    asin: string;
    title: string;
    artist: string;
    slug: string;
    style: string | null;
    lastfmTags: string | null;
    imgUrl: string | null;
    url: string;
    rating: string | null;
    reviewCount: number | null;
    prices: { price: string; capturedAt: number }[];
  }[];
  dealMeta: Record<string, {
    id: string;
    deal_score: number | null;
    confidence_level: string | null;
    last_crawled_at: string | null;
    available: boolean;
  }>;
};

const _getArtistPageData = unstable_cache(
  async (slug: string): Promise<SerializedPageData | null> => {
    const candidates = await prisma.$queryRaw<{ artist: string }[]>`
      SELECT DISTINCT artist FROM "Record"
      WHERE left(
              regexp_replace(
                regexp_replace(translate(lower(artist), ${ACCENT_FROM}, ${ACCENT_TO}), '[^a-z0-9]+', '-', 'g'),
                '^-+|-+$', '', 'g'
              ), 60) = ${slug}
         OR left(
              regexp_replace(
                regexp_replace(
                  translate(
                    lower(trim(split_part(artist, ',', 2)) || ' ' || trim(split_part(artist, ',', 1))),
                    ${ACCENT_FROM}, ${ACCENT_TO}
                  ),
                  '[^a-z0-9]+', '-', 'g'
                ),
                '^-+|-+$', '', 'g'
              ), 60) = ${slug}
    `;

    const variants = candidates
      .map((r) => r.artist)
      .filter((a) => slugifyArtist(a) === slug);

    if (variants.length === 0) return null;

    const canonical = variants.slice().sort((a, b) => {
      const aScore = (a.includes(",") ? 1 : 0) + (a === a.toUpperCase() ? 1 : 0);
      const bScore = (b.includes(",") ? 1 : 0) + (b === b.toUpperCase() ? 1 : 0);
      return aScore - bScore || a.length - b.length;
    })[0];

    const oneYearAgo = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000);

    const discos = await prisma.record.findMany({
      where: { artist: { in: variants } },
      include: {
        prices: {
          where: { capturedAt: { gte: oneYearAgo } },
          orderBy: { capturedAt: "desc" },
          take: 60,
        },
      },
    });

    if (discos.length === 0) return null;

    const discoIds = discos.map((d) => d.id);

    const [dealMetaRows, lastfmTagsRows] = await Promise.all([
      prisma.$queryRaw<{
        id: string;
        deal_score: number | null;
        confidence_level: string | null;
        last_crawled_at: Date | null;
        available: boolean;
      }[]>`
        SELECT id::text, deal_score, confidence_level, last_crawled_at, available
        FROM "Record"
        WHERE id::text = ANY(${discoIds})
      `,
      prisma.$queryRaw<{ id: string; lastfmTags: string | null }[]>`
        SELECT id::text, lastfm_tags AS "lastfmTags"
        FROM "Record"
        WHERE id::text = ANY(${discoIds})
      `,
    ]);
    const lastfmTagsById = Object.fromEntries(
      lastfmTagsRows.map((r) => [r.id, r.lastfmTags])
    );

    return {
      canonical,
      discos: discos.map((d) => ({
        id: d.id,
        asin: d.asin,
        title: d.title,
        artist: d.artist,
        slug: d.slug,
        style: d.style,
        lastfmTags: lastfmTagsById[d.id] ?? null,
        imgUrl: d.imgUrl,
        url: d.url,
        rating: d.rating ? String(d.rating) : null,
        reviewCount: d.reviewCount,
        prices: d.prices.map((p) => ({
          price: String(p.price),
          capturedAt: p.capturedAt.getTime(),
        })),
      })),
      dealMeta: Object.fromEntries(
        dealMetaRows.map((r) => [r.id, {
          id: r.id,
          deal_score: r.deal_score !== null ? Number(r.deal_score) : null,
          confidence_level: r.confidence_level,
          last_crawled_at: r.last_crawled_at ? new Date(r.last_crawled_at).toISOString() : null,
          available: r.available,
        }])
      ),
    };
  },
  ["artist-page"],
  { tags: ["prices"] }
);

const getArtistPageData = cache(_getArtistPageData);

export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  let data;
  try {
    data = await getArtistPageData(slug);
  } catch {
    return {};
  }
  if (!data) return {};
  const { canonical } = data;
  const title = truncateTitle(`${canonical} — Vinyl Deals & Price History | The Groove Hunter`);
  const description = truncateDesc(`Best vinyl deals for ${canonical}: track price history and find the right record at the lowest price.`);
  return {
    title,
    description,
    alternates: { canonical: `/artist/${slug}` },
    openGraph: {
      title,
      description,
      url: `/artist/${slug}`,
      type: "website",
    },
    twitter: {
      card: "summary",
      title,
      description,
    },
  };
}

export default async function ArtistPage({
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

  let data: SerializedPageData | null = null;
  try {
    data = await getArtistPageData(slug);
  } catch (err) {
    console.error("[ArtistPage] getArtistPageData failed for slug=%s", slug);
    if (process.env.NODE_ENV === "development") console.error(err);
    return (
      <main className="max-w-7xl mx-auto px-4 py-24 text-center">
        <p className="font-display text-parchment text-lg font-semibold mb-2">
          Error loading artist page
        </p>
        <p className="text-dust text-sm">Please try again in a moment.</p>
      </main>
    );
  }
  if (!data) notFound();

  const { canonical: artista, discos, dealMeta } = data;
  const topStyles = getTopStyles(discos.map((d) => d.lastfmTags), 5, artista);

  const discosDisponiveis = discos.filter((d) => dealMeta[d.id]?.available !== false);

  const thirtyDaysAgoMs = Date.now() - 30 * 24 * 60 * 60 * 1000;

  const discosProcessados = discosDisponiveis.map((disco) => {
    const prices = disco.prices.map((p) => Number(p.price));
    const currentPrice = prices[0] ?? 0;
    const avgPrice =
      prices.length > 0
        ? prices.reduce((a, b) => a + b, 0) / prices.length
        : currentPrice;
    const discount = avgPrice > 0 ? (avgPrice - currentPrice) / avgPrice : 0;

    const sparkline = [...disco.prices]
      .filter((p) => p.capturedAt >= thirtyDaysAgoMs)
      .sort((a, b) => a.capturedAt - b.capturedAt)
      .slice(-10)
      .map((p) => Number(p.price));

    const meta = dealMeta[disco.id];
    const rawDealScore = meta?.deal_score !== null && meta?.deal_score !== undefined
      ? Number(meta.deal_score)
      : null;

    const DEAL_STALE_MS = 4 * 60 * 60 * 1000;
    const crawledAt = meta?.last_crawled_at ? new Date(meta.last_crawled_at).getTime() : null;
    const dealIsStale = crawledAt === null || Date.now() - crawledAt > DEAL_STALE_MS;
    const dealScore = rawDealScore !== null && !dealIsStale ? rawDealScore : null;

    return {
      ...disco,
      rating:          disco.rating ? Number(disco.rating) : null,
      currentPrice,
      avgPrice,
      onSale:          dealScore !== null,
      discount,
      sparkline,
      dealScore,
      confidenceLevel: meta?.confidence_level ?? null,
    };
  });

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
        name: artista,
        item: `${siteUrl}/artist/${slug}`,
      },
    ],
  });

  const fmt = (v: number) =>
    v.toLocaleString("en-US", { style: "currency", currency: "USD" });

  return (
    <main className="max-w-7xl mx-auto px-4 py-8">
      {/* eslint-disable-next-line react/no-danger */}
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: breadcrumbJsonLd }} />
      <nav className="flex items-center gap-1.5 text-sm text-dust mb-6 flex-wrap">
        <Link href="/" className="hover:text-cream transition-colors">
          Home
        </Link>
        <span>›</span>
        <span className="text-parchment">{artista}</span>
      </nav>

      <header className="mb-6">
        <h1 className="font-display text-3xl font-bold text-cream">
          {artista}
        </h1>
        <p className="mt-1 text-dust text-sm">
          {sorted.length}{" "}
          {sorted.length === 1 ? "record" : "records"}
          {precoMax !== null && !isNaN(precoMax)
            ? ` up to ${fmt(precoMax)}`
            : " tracked"}
        </p>
        <StyleTags tags={topStyles} />
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
