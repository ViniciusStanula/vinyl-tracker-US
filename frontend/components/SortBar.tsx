"use client";

import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { useTransition, useState, useEffect } from "react";

const SORT_OPTIONS = [
  { label: "Best Deals",       value: "deals"         },
  { label: "Biggest Discount", value: "discount"      },
  { label: "Lowest Price",     value: "lowest-price"  },
  { label: "Highest Price",    value: "highest-price" },
  { label: "Top Rated",        value: "top-rated"     },
  { label: "A–Z",              value: "az"            },
];

const PRECO_MAX = 200;

export default function SortBar() {
  const router       = useRouter();
  const pathname     = usePathname();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const current      = searchParams.get("sort") ?? "discount";
  const precoMaxParam = searchParams.get("precoMax");

  const [sliderValue, setSliderValue] = useState(
    precoMaxParam ? Math.min(Number(precoMaxParam), PRECO_MAX) : PRECO_MAX
  );

  useEffect(() => {
    const v = searchParams.get("precoMax");
    setSliderValue(v ? Math.min(Number(v), PRECO_MAX) : PRECO_MAX);
  }, [searchParams]);

  function handleSort(value: string) {
    const params = new URLSearchParams(searchParams.toString());
    params.set("sort", value);
    params.delete("page");
    startTransition(() => router.push(`${pathname}?${params.toString()}`));
  }

  function commitPreco(value: number) {
    const params = new URLSearchParams(searchParams.toString());
    if (value < PRECO_MAX) {
      params.set("precoMax", String(value));
    } else {
      params.delete("precoMax");
    }
    params.delete("page");
    startTransition(() => router.push(`${pathname}?${params.toString()}`));
  }

  const fmt = (v: number) =>
    v.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });

  return (
    <div
      className={`bg-sleeve border border-groove rounded-xl px-5 py-3.5 transition-opacity ${
        isPending ? "opacity-55" : ""
      }`}
    >
      <div className="flex items-center gap-5 flex-wrap">

        {/* ── Price range ── */}
        <div className="flex items-center gap-3">
          <span className="text-[11px] font-bold text-dust uppercase tracking-widest shrink-0">
            Price
          </span>
          <input
            type="range"
            min={0}
            max={PRECO_MAX}
            step={5}
            value={sliderValue}
            onChange={(e) => setSliderValue(Number(e.target.value))}
            onPointerUp={(e) =>
              commitPreco(Number((e.target as HTMLInputElement).value))
            }
            className="w-32 sm:w-44 accent-gold cursor-pointer"
          />
          <span className="text-sm text-cream font-semibold w-[6rem] shrink-0 tabular-nums">
            {sliderValue >= PRECO_MAX
              ? `Up to ${fmt(PRECO_MAX)}`
              : `Up to ${fmt(sliderValue)}`}
          </span>
        </div>

        {/* Divider */}
        <div className="hidden sm:block h-5 w-px bg-wax/60 shrink-0" />

        {/* ── Sort ── */}
        <div className="flex items-center gap-2.5">
          <span className="text-[11px] font-bold text-dust uppercase tracking-widest shrink-0">
            Sort
          </span>
          <select
            value={current}
            onChange={(e) => handleSort(e.target.value)}
            className="bg-groove text-cream text-sm border border-wax/60 rounded-lg px-3 py-1.5 focus:outline-none focus:border-gold focus:ring-2 focus:ring-gold/20 cursor-pointer transition-colors"
          >
            {SORT_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>
      </div>
    </div>
  );
}
