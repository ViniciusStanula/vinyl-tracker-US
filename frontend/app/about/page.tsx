import BackToTop from "@/components/BackToTop";
import GraficoPreco from "@/components/GraficoPreco";
import Link from "next/link";

const EXAMPLE_PRICES = [
  { date: "03/23", dateFull: "03/23/2026", value: 29.99 },
  { date: "03/24", dateFull: "03/24/2026", value: 29.99 },
  { date: "03/25", dateFull: "03/25/2026", value: 31.99 },
  { date: "03/26", dateFull: "03/26/2026", value: 31.99 },
  { date: "03/27", dateFull: "03/27/2026", value: 32.99 },
  { date: "03/28", dateFull: "03/28/2026", value: 32.99 },
  { date: "03/29", dateFull: "03/29/2026", value: 32.99 },
  { date: "03/30", dateFull: "03/30/2026", value: 30.99 },
  { date: "03/31", dateFull: "03/31/2026", value: 30.99 },
  { date: "04/01", dateFull: "04/01/2026", value: 29.99 },
  { date: "04/02", dateFull: "04/02/2026", value: 29.99 },
  { date: "04/03", dateFull: "04/03/2026", value: 31.49 },
  { date: "04/04", dateFull: "04/04/2026", value: 31.49 },
  { date: "04/05", dateFull: "04/05/2026", value: 31.49 },
  { date: "04/06", dateFull: "04/06/2026", value: 29.99 },
  { date: "04/07", dateFull: "04/07/2026", value: 28.49 },
  { date: "04/08", dateFull: "04/08/2026", value: 28.49 },
  { date: "04/09", dateFull: "04/09/2026", value: 27.99 },
  { date: "04/10", dateFull: "04/10/2026", value: 27.99 },
  { date: "04/11", dateFull: "04/11/2026", value: 27.99 },
  { date: "04/12", dateFull: "04/12/2026", value: 25.99 },
  { date: "04/13", dateFull: "04/13/2026", value: 25.99 },
  { date: "04/14", dateFull: "04/14/2026", value: 26.99 },
  { date: "04/15", dateFull: "04/15/2026", value: 26.99 },
  { date: "04/16", dateFull: "04/16/2026", value: 23.99 },
  { date: "04/17", dateFull: "04/17/2026", value: 23.99 },
  { date: "04/18", dateFull: "04/18/2026", value: 23.99 },
  { date: "04/19", dateFull: "04/19/2026", value: 21.99 },
  { date: "04/20", dateFull: "04/20/2026", value: 21.99 },
  { date: "04/21", dateFull: "04/21/2026", value: 19.99 },
];

export const metadata = {
  title: "About — The Groove Hunter",
  description:
    "How The Groove Hunter works: vinyl record price monitoring on Amazon, full price history, and automatic deal detection.",
  alternates: { canonical: "/about" },
  openGraph: {
    title: "About — The Groove Hunter",
    description:
      "How The Groove Hunter works: vinyl record price monitoring on Amazon, full price history, and automatic deal detection.",
    url: "/about",
    type: "website",
  },
  twitter: {
    card: "summary",
    title: "About — The Groove Hunter",
    description:
      "How The Groove Hunter works: vinyl record price monitoring on Amazon, full price history, and automatic deal detection.",
  },
};

export default function AboutPage() {
  return (
    <main className="max-w-3xl mx-auto px-4 py-8">

      {/* ── Breadcrumbs ─────────────────────────────────────────── */}
      <nav className="mb-6 text-sm text-dust flex gap-2">
        <Link href="/" className="hover:text-gold transition-colors">Home</Link>
        <span>›</span>
        <span className="text-parchment">About</span>
      </nav>

      {/* ── Hero ────────────────────────────────────────────────── */}
      <header className="relative mb-8 overflow-hidden rounded-2xl bg-sleeve border border-groove px-6 py-7 vinyl-grooves">
        <h1 className="font-display text-3xl sm:text-4xl font-black text-cream leading-tight">
          What is{" "}
          <span className="text-gold">The Groove Hunter</span>
        </h1>
        <p className="mt-3 text-parchment text-sm max-w-lg leading-relaxed">
          A vinyl record price tracker for Amazon. It monitors hundreds of titles
          every hour, stores the full price history, and surfaces real deals —
          not just when Amazon slaps a red banner on the listing.
        </p>
      </header>

      {/* ── How it works ────────────────────────────────────────── */}
      <section className="mb-6 bg-sleeve border border-groove rounded-xl p-6">
        <h2 className="font-display text-xl font-bold text-cream mb-3">
          What happens behind the scenes
        </h2>
        <p className="text-parchment text-sm leading-relaxed mb-3">
          Every hour, a crawler visits each tracked product page on Amazon and
          records the current price with an exact timestamp. That value goes into
          a database alongside the date and time of capture.
        </p>
        <p className="text-parchment text-sm leading-relaxed mb-3">
          With that history, we can calculate real averages and minimums — not
          estimates, but values based on prices the site has actually recorded
          over time. That history is what powers deal detection.
        </p>
        <p className="text-parchment text-sm leading-relaxed">
          The full history is visible in the chart on each record page, with the
          recorded minimum and maximum annotated. That way you can see whether
          that &ldquo;discount&rdquo; is real or the price has always been there.
        </p>
      </section>

      {/* ── Deal detection ──────────────────────────────────────── */}
      <section className="mb-6 bg-sleeve border border-groove rounded-xl p-6">
        <h2 className="font-display text-xl font-bold text-cream mb-3">
          How deal detection works
        </h2>
        <p className="text-parchment text-sm leading-relaxed mb-4">
          A low price isn&apos;t enough on its own — it has to be low{" "}
          <em>relative to that specific record&apos;s own history</em>. A vinyl
          that normally costs $35 at $28 can be a great deal. The same discount
          on a record that swings between $20 and $50 means nothing.
        </p>
        <p className="text-parchment text-sm leading-relaxed mb-5">
          For a record to show as a deal, two conditions must be true at once:
          the current price must be at least{" "}
          <span className="text-cream font-medium">10% below the 30-day average</span>{" "}
          and the drop in dollars must be{" "}
          <span className="text-cream font-medium">at least $2</span>. This
          prevents tiny cent-level swings from triggering false alerts.
        </p>

        <div className="flex flex-col gap-3">
          {/* Tier 1 */}
          <div className="flex gap-3 items-start rounded-lg border border-groove bg-label p-4">
            <span className="mt-0.5 shrink-0 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold bg-deal/20 text-deallit border border-deal/30">
              Good Deal
            </span>
            <p className="text-parchment text-sm leading-relaxed">
              Price is at least 10% below the 30-day average and has dropped by
              at least $2. The baseline condition — confirms a real discount
              against recent price behavior.
            </p>
          </div>

          {/* Tier 2 */}
          <div className="flex gap-3 items-start rounded-lg border border-groove bg-label p-4">
            <span className="mt-0.5 shrink-0 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold bg-deal/30 text-deallit border border-deal/40">
              Great Deal
            </span>
            <p className="text-parchment text-sm leading-relaxed">
              Everything in Good Deal, plus the current price is also below the{" "}
              <span className="text-cream font-medium">90-day average</span>.
              This second filter only applies when the record has enough
              history — at least 30 data points and 45 days of data — so when
              it appears, it&apos;s a more reliable signal.
            </p>
          </div>

          {/* Tier 3 */}
          <div className="flex gap-3 items-start rounded-lg border border-groove bg-label p-4">
            <span className="mt-0.5 shrink-0 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold bg-gold/20 text-gold border border-gold/30">
              Best Price
            </span>
            <p className="text-parchment text-sm leading-relaxed">
              The current price equals or is very close (within 2%) of the{" "}
              <span className="text-cream font-medium">30-day lowest price</span>.
              When this happens, the record is at the bottom of its recent range
              — regardless of the average. This is the strongest badge.
            </p>
          </div>
        </div>

        <p className="mt-4 text-dust text-xs leading-relaxed">
          Records with limited history get more conservative badges or none at
          all — the system prefers to stay silent over signaling incorrectly.
        </p>
      </section>

      {/* ── Price chart ─────────────────────────────────────────── */}
      <section className="mb-6 bg-sleeve border border-groove rounded-xl p-6">
        <h2 className="font-display text-xl font-bold text-cream mb-3">
          The price chart
        </h2>
        <p className="text-parchment text-sm leading-relaxed mb-5">
          Every record page includes a chart showing price over time. Hover or
          tap to see the exact value at each date. Green and red dots mark the
          recorded minimum and maximum for the period.
        </p>

        <div className="rounded-xl border border-groove bg-label px-4 pt-4 pb-2">
          <GraficoPreco points={EXAMPLE_PRICES} />
        </div>
        <p className="mt-2 text-center text-dust text-xs">
          Example: 30-day price trend for a vinyl record
        </p>
      </section>

      <BackToTop />
    </main>
  );
}
