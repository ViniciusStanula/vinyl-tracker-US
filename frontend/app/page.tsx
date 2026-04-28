import { cachedQueryDiscos } from "@/lib/queryDiscos";
import { cachedQueryCarouselDiscos } from "@/lib/carousel";
import SortBar from "@/components/SortBar";
import InfiniteGrid from "@/components/InfiniteGrid";
import ArtistasCarousel from "@/components/ArtistasCarousel";
import BackToTop from "@/components/BackToTop";
import Link from "next/link";
import { Suspense } from "react";

export const revalidate = 7200;

export const metadata = {
  title: "The Groove Hunter — Best Deals on Vinyl Records",
  description:
    "Track vinyl record prices on Amazon. Full price history updated every 3 hours.",
  alternates: { canonical: "/" },
  openGraph: {
    title: "The Groove Hunter — Best Deals on Vinyl Records",
    description:
      "Track vinyl record prices on Amazon. Full price history updated every 3 hours.",
    url: "/",
    type: "website",
    images: [{ url: "/og-default.png", width: 1200, height: 630, alt: "The Groove Hunter" }],
  },
  twitter: {
    card: "summary_large_image",
    title: "The Groove Hunter — Best Deals on Vinyl Records",
    description:
      "Track vinyl record prices on Amazon. Full price history updated every 3 hours.",
    images: ["/og-default.png"],
  },
};

export default async function HomePage({
  searchParams,
}: {
  searchParams: Promise<{
    q?: string;
    sort?: string;
    artist?: string;
    page?: string;
    precoMax?: string;
  }>;
}) {
  const {
    q,
    sort = "discount",
    artist,
    page: pageStr,
    precoMax: precoMaxStr,
  } = await searchParams;

  const page       = Math.max(1, parseInt(pageStr ?? "1", 10));
  const searchTerm = q?.trim() ?? "";
  const precoMax   = precoMaxStr ? Number(precoMaxStr) : null;

  let items: Awaited<ReturnType<typeof cachedQueryDiscos>>["items"] = [];
  let total = 0, totalPages = 0, carouselItems: Awaited<ReturnType<typeof cachedQueryCarouselDiscos>> = [];
  try {
    ([{ items, total, totalPages }, carouselItems] = await Promise.all([
      cachedQueryDiscos({ searchTerm, sort, artist: artist, precoMax, page }),
      searchTerm || artist ? Promise.resolve([]) : cachedQueryCarouselDiscos(),
    ]));
  } catch {
    // DB unavailable — render empty state
  }

  const currentPage = Math.min(page, totalPages);

  return (
    <main className="max-w-7xl mx-auto px-4 py-8">

      {/* ── Hero ────────────────────────────────────────────────── */}
      <header className="relative mb-8 overflow-hidden rounded-2xl bg-sleeve border border-groove px-6 py-7 vinyl-grooves">
        <h1 className="font-display text-3xl sm:text-4xl font-black text-cream leading-tight">
          Best deals on
          <br />
          <span className="text-gold">vinyl records</span>
        </h1>
        <p className="mt-3 text-parchment text-sm max-w-md leading-relaxed">
          Full price history. Find the best time to buy.
        </p>
      </header>

      {/* ── Most Listened Artists carousel ──────────────────────── */}
      <ArtistasCarousel items={carouselItems} />

      {/* ── Sort bar ────────────────────────────────────────────── */}
      <div className="mb-5">
        <Suspense>
          <SortBar />
        </Suspense>
      </div>

      {/* ── Result count + active artist badge ──────────────────── */}
      <div className="flex items-center gap-3 mb-5 flex-wrap">
        <p className="text-dust text-sm">
          {total === 0
            ? "No records found"
            : `${total} ${total === 1 ? "record found" : "records found"}`}
          {searchTerm && (
            <span className="text-parchment">
              {" "}for{" "}
              <span className="text-cream">&ldquo;{q}&rdquo;</span>
            </span>
          )}
        </p>
        {artist && (
          <span className="inline-flex items-center gap-1.5 bg-groove border border-wax/60 text-parchment text-xs px-3 py-1 rounded-full">
            {artist}
            <Link
              href="/"
              className="text-dust hover:text-cream transition-colors leading-none"
              aria-label="Remove artist filter"
            >
              ×
            </Link>
          </span>
        )}
      </div>

      {/* ── Grid + Pagination ───────────────────────────────────── */}
      {items.length > 0 ? (
        <InfiniteGrid
          initialItems={items}
          currentPage={currentPage}
          totalPages={totalPages}
          searchParams={{ q, sort, artist, precoMax: precoMaxStr }}
          animationKey={`${sort}-${q ?? ""}-${artist ?? ""}-${currentPage}`}
          basePath="/record"
        />
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
          <p className="text-dust text-sm mb-6">
            Try adjusting the filters or searching for a different artist.
          </p>
          <Link
            href="/"
            className="inline-flex items-center gap-2 bg-gold hover:bg-goldlit text-record font-bold text-sm px-6 py-2.5 rounded-full transition-colors"
          >
            View all records
          </Link>
        </div>
      )}

      <BackToTop />
    </main>
  );
}
