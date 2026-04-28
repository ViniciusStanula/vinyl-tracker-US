import Link from "next/link";

const TOP_GENRES = [
  { name: "Rock",       slug: "rock" },
  { name: "Jazz",       slug: "jazz" },
  { name: "Pop",        slug: "pop" },
  { name: "Classical",  slug: "classical" },
  { name: "Hip-Hop",    slug: "hip-hop" },
  { name: "Blues",      slug: "blues" },
  { name: "Electronic", slug: "electronic" },
  { name: "Soul",       slug: "soul" },
  { name: "Folk",       slug: "folk" },
  { name: "Metal",      slug: "metal" },
  { name: "R&B",        slug: "rnb" },
  { name: "Country",    slug: "country" },
];

export default function Footer() {
  return (
    <footer className="mt-16 border-t border-groove bg-record">
      <div className="max-w-7xl mx-auto px-4 py-10 grid grid-cols-2 sm:grid-cols-4 gap-8 text-sm">
        <div className="col-span-2 sm:col-span-2">
          <p className="font-display font-bold text-cream mb-1 text-base">The Groove Hunter</p>
          <p className="text-dust text-xs leading-relaxed max-w-xs">
            Tracks vinyl record prices on Amazon twice daily. Full price history and automatic deal detection.
          </p>
          <div className="mt-4 flex flex-col gap-1">
            <Link href="/"        className="text-dust hover:text-cream transition-colors">Home</Link>
            <Link href="/record"  className="text-dust hover:text-cream transition-colors">All Records</Link>
            <Link href="/about"   className="text-dust hover:text-cream transition-colors">About</Link>
          </div>
        </div>

        <div className="col-span-2 sm:col-span-2">
          <p className="font-semibold text-cream mb-3">Browse by Genre</p>
          <ul className="grid grid-cols-2 gap-x-4 gap-y-1">
            {TOP_GENRES.map(({ name, slug }) => (
              <li key={slug}>
                <Link
                  href={`/genre/${slug}`}
                  className="text-dust hover:text-cream transition-colors"
                >
                  {name}
                </Link>
              </li>
            ))}
          </ul>
        </div>
      </div>

      <div className="border-t border-groove/50 px-4 py-4 text-center text-xs text-dust">
        <p>
          As an Amazon Associate we earn from qualifying purchases. Prices shown are from Amazon and may vary.{" "}
          <Link href="/about" className="hover:text-parchment transition-colors underline underline-offset-2">
            Learn more
          </Link>
        </p>
      </div>
    </footer>
  );
}
