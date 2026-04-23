import Link from "next/link";
import { Suspense } from "react";
import SearchBar from "./SearchBar";

function VinylLogo() {
  return (
    <svg viewBox="0 0 40 40" fill="none" className="w-10 h-10 shrink-0">
      {/* Outer disc */}
      <circle cx="20" cy="20" r="19" className="fill-gold" />
      {/* Groove rings on the disc */}
      <circle cx="20" cy="20" r="16.5" fill="none" className="stroke-record" strokeWidth="1"   opacity="0.35" />
      <circle cx="20" cy="20" r="14"   fill="none" className="stroke-record" strokeWidth="0.7" opacity="0.30" />
      <circle cx="20" cy="20" r="11.5" fill="none" className="stroke-record" strokeWidth="0.6" opacity="0.25" />
      {/* Center label */}
      <circle cx="20" cy="20" r="9" className="fill-record" />
      <circle cx="20" cy="20" r="8"   fill="none" className="stroke-gold" strokeWidth="0.5" opacity="0.4" />
      <circle cx="20" cy="20" r="5.5" fill="none" className="stroke-gold" strokeWidth="0.4" opacity="0.25" />
      {/* Spindle hole */}
      <circle cx="20" cy="20" r="2.2" className="fill-gold" opacity="0.85" />
      <circle cx="20" cy="20" r="0.9" className="fill-record" />
    </svg>
  );
}

export default function Navbar() {
  return (
    <nav className="sticky top-0 z-50 bg-record/95 backdrop-blur-md border-b border-groove/60">
      <div className="max-w-7xl mx-auto px-4 h-[62px] flex items-center gap-5">

        {/* ── Brand ── */}
        <Link href="/" className="flex items-center gap-3 shrink-0 group">
          <VinylLogo />
          <div className="hidden sm:flex flex-col leading-none">
            <span className="font-display text-[21px] font-black text-cream tracking-tight">
              The Groove
            </span>
            <span className="text-gold text-[9px] tracking-[0.38em] uppercase font-semibold mt-px">
              Hunter
            </span>
          </div>
        </Link>

        {/* ── Search ── */}
        <div className="flex-1 max-w-2xl">
          <Suspense>
            <SearchBar />
          </Suspense>
        </div>

        {/* ── Nav links ── */}
        <Link
          href="/about"
          className="shrink-0 text-dust hover:text-gold text-sm transition-colors min-w-[44px] min-h-[44px] flex items-center justify-center sm:min-w-0 sm:min-h-0 sm:inline"
        >
          About
        </Link>
      </div>
    </nav>
  );
}
