"use client";

import { useRef, useState, useEffect, useCallback } from "react";
import DiscoCard from "./DiscoCard";
import type { ProcessedDisco } from "@/lib/queryDiscos";

const SCROLL_AMOUNT = 640;

export default function ArtistasCarousel({ items }: { items: ProcessedDisco[] }) {
  const ref                       = useRef<HTMLDivElement>(null);
  const [canLeft,  setCanLeft ]   = useState(false);
  const [canRight, setCanRight]   = useState(false);

  const sync = useCallback(() => {
    const el = ref.current;
    if (!el) return;
    setCanLeft(el.scrollLeft > 4);
    setCanRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 4);
  }, []);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    sync();
    const ro = new ResizeObserver(sync);
    ro.observe(el);
    el.addEventListener("scroll", sync, { passive: true });
    return () => {
      ro.disconnect();
      el.removeEventListener("scroll", sync);
    };
  }, [sync]);

  if (items.length === 0) return null;

  return (
    <section className="mb-10">
      <div className="flex items-center justify-between mb-3">
        <h2 className="font-display text-xl font-bold text-cream">
          Most Listened Artists
        </h2>
        <div className="flex gap-1.5">
          <button
            onClick={() => ref.current?.scrollBy({ left: -SCROLL_AMOUNT, behavior: "smooth" })}
            disabled={!canLeft}
            className="w-11 h-11 flex items-center justify-center rounded-full border border-groove hover:border-wax text-cream text-lg disabled:opacity-20 disabled:cursor-default transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40"
            aria-label="Scroll left"
          >
            ‹
          </button>
          <button
            onClick={() => ref.current?.scrollBy({ left: SCROLL_AMOUNT, behavior: "smooth" })}
            disabled={!canRight}
            className="w-11 h-11 flex items-center justify-center rounded-full border border-groove hover:border-wax text-cream text-lg disabled:opacity-20 disabled:cursor-default transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40"
            aria-label="Scroll right"
          >
            ›
          </button>
        </div>
      </div>

      <div
        ref={ref}
        className="flex gap-3 overflow-x-auto scroll-smooth snap-x snap-mandatory pb-2"
      >
        {items.map((disco, i) => (
          <div key={disco.id} className="snap-start shrink-0 w-44 sm:w-52">
            <DiscoCard disco={disco} priority={i < 6} />
          </div>
        ))}
      </div>
    </section>
  );
}
