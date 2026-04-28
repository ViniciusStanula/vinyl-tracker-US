import Image from "next/image";
import Link from "next/link";
import { slugifyArtist } from "@/lib/slugify";

export interface DiscoCardProps {
  id: string;
  slug: string;
  title: string;
  artist: string;
  style: string | null;
  imgUrl: string | null;
  url: string;
  rating: number | null;
  currentPrice: number;
  avgPrice: number;
  onSale: boolean;
  discount: number;
  sparkline?: number[];
  /** Scoring tier: 1 = Good Deal, 2 = Great Deal, 3 = Best Price, null = no deal */
  dealScore?: number | null;
  /** Backend confidence tier; "low_confidence" triggers a data-warning indicator */
  confidenceLevel?: string | null;
  /** Comma-separated Last.fm genre tags, e.g. "rock, classic rock" */
  lastfmTags?: string | null;
}

/** 44×18 px SVG sparkline showing the 30-day price trend. */
function Sparkline({ values }: { values: number[] }) {
  if (values.length < 2) return null;
  const dataMin = Math.min(...values);
  const dataMax = Math.max(...values);
  const mid = (dataMin + dataMax) / 2;
  const minRange = mid * 0.10;
  const min = Math.min(dataMin, mid - minRange / 2);
  const max = Math.max(dataMax, mid + minRange / 2);
  const range = max - min || 1;
  const W = 44, H = 18, PAD = 1;
  const pts = values
    .map((v, i) => {
      const x = PAD + (i / (values.length - 1)) * (W - PAD * 2);
      const y = H - PAD - ((v - min) / range) * (H - PAD * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const trending = values[values.length - 1] <= values[0];
  return (
    <svg width={W} height={H} aria-hidden="true" className="shrink-0 opacity-80">
      <polyline
        points={pts}
        fill="none"
        className={trending ? "stroke-deallit" : "stroke-cut"}
        strokeWidth="1.5"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  );
}

export default function DiscoCard({
  disco,
  priority = false,
  headingLevel = "h2",
}: {
  disco: DiscoCardProps;
  priority?: boolean;
  headingLevel?: "h2" | "h3";
}) {
  const fmt = (v: number) =>
    v.toLocaleString("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 2,
    });

  const discountPercent    = Math.round(disco.discount * 100);
  const showOriginalPrice  = discountPercent > 0;
  const dealScore          = disco.dealScore ?? null;
  const confidenceLevel    = disco.confidenceLevel ?? null;
  const artistSlug         = slugifyArtist(disco.artist);
  const sparkline          = disco.sparkline ?? [];
  const H                  = headingLevel;

  // Score-3 gets a subtle gold ring
  const cardRing = dealScore === 3 ? " ring-1 ring-gold/40" : "";

  return (
    <div className={`relative group bg-sleeve rounded-xl overflow-hidden flex flex-col border border-groove hover:border-wax transition-colors duration-200${cardRing}`}>
      {/* Full-card link */}
      <Link
        href={`/record/${disco.slug}`}
        className="absolute inset-0 z-10"
        aria-label={`View price history for ${disco.title}`}
      />

      {/* ── Album art ─────────────────────────────────────────────── */}
      <div className="relative aspect-square bg-label shrink-0 overflow-hidden">
        {disco.imgUrl ? (
          <Image
            src={disco.imgUrl}
            alt={disco.title}
            fill
            sizes="(max-width: 767px) 50vw, (max-width: 1199px) 33vw, 25vw"
            className="object-cover transition-transform duration-500 ease-out group-hover:scale-[1.06]"
            unoptimized
            priority={priority}
            loading={priority ? undefined : "lazy"}
          />
        ) : (
          <div className="absolute inset-0 flex items-center justify-center text-patina text-5xl select-none">
            ♪
          </div>
        )}

        {/* Subtle gradient overlay — bottom fade for legibility */}
        <div className="absolute inset-0 bg-gradient-to-t from-record/50 via-transparent to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300 pointer-events-none" />

        {/* Discount badge — pill, top-left */}
        {discountPercent > 0 && (
          <div className="absolute top-2 left-2 z-20 bg-cut text-cream text-xs font-black px-2.5 py-1 rounded-md shadow-lg shadow-cut/30 tabular-nums">
            -{discountPercent}%
          </div>
        )}

        {/* Deal tier badges — bottom-left, clear of the discount badge */}
        {dealScore === 3 && (
          <div className="absolute bottom-2 left-2 z-20 bg-gold text-record text-[10px] font-black px-2 py-0.5 rounded-md shadow-md flex items-center gap-1">
            ✦ Best Price
          </div>
        )}
        {dealScore === 2 && (
          <div className="absolute bottom-2 left-2 z-20 bg-deal text-cream text-[10px] font-bold px-2 py-0.5 rounded-md shadow-md">
            ✓ Great Deal
          </div>
        )}
        {dealScore === 1 && (
          <div className="absolute bottom-2 left-2 z-20 bg-record/70 text-parchment text-[10px] font-medium px-2 py-0.5 rounded-md border border-wax/60 backdrop-blur-sm">
            Good Deal
          </div>
        )}

        {/* Amazon quick-link — hover only */}
        <a
          href={disco.url}
          target="_blank"
          rel="noopener noreferrer"
          className="absolute top-2 right-2 z-20 opacity-0 group-hover:opacity-100 [@media(hover:none)]:opacity-100 transition-opacity bg-record/80 text-cream text-[10px] font-medium px-2 py-1 rounded-md backdrop-blur-sm"
          aria-label={`View ${disco.title} on Amazon`}
        >
          Amazon ↗
        </a>
      </div>

      {/* ── Info ──────────────────────────────────────────────────── */}
      <div className="p-3 flex flex-col flex-1">
        {/* Artist */}
        <Link
          href={`/artist/${artistSlug}`}
          className="relative z-20 block text-parchment hover:text-gold text-xs truncate transition-colors font-medium"
        >
          {disco.artist}
        </Link>

        {/* Title — Fraunces for editorial character */}
        <H
          className="font-display text-cream text-sm font-semibold leading-snug line-clamp-2 min-h-[2.5rem] mt-0.5"
          title={disco.title}
        >
          {disco.title}
        </H>

        {/* ── Price section ──────────────────────────────────────── */}
        <div className="mt-auto pt-2">
          {(sparkline.length >= 2 || showOriginalPrice) && (
            <div className="flex items-center gap-2 mb-1">
              {sparkline.length >= 2 && <Sparkline values={sparkline} />}
              {showOriginalPrice && (
                <p className="text-dust text-xs line-through ml-auto tabular-nums">
                  {fmt(disco.avgPrice)}
                </p>
              )}
            </div>
          )}

          {/* Current price — bold, gold, large */}
          <p className="font-display text-gold font-black text-xl leading-tight tabular-nums">
            {fmt(disco.currentPrice)}
          </p>

          {/* Low-confidence warning */}
          {confidenceLevel === "low_confidence" && dealScore !== null && (
            <p className="text-[10px] mt-0.5 text-goldmute">
              ⚠ Limited data available
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
