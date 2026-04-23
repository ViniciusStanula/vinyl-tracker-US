import Link from "next/link";

type SearchParams = {
  q?: string;
  sort?: string;
  artista?: string;
  precoMax?: string;
};

function buildUrl(page: number, sp: SearchParams, basePath: string): string {
  const params = new URLSearchParams();
  if (sp.q) params.set("q", sp.q);
  if (sp.sort && sp.sort !== "desconto") params.set("sort", sp.sort);
  if (sp.artista) params.set("artista", sp.artista);
  if (sp.precoMax) params.set("precoMax", sp.precoMax);
  if (page > 1) params.set("page", String(page));
  const qs = params.toString();
  return qs ? `${basePath}?${qs}` : basePath;
}

/** Returns a mixed array of page numbers and ellipsis markers. */
function pageRange(current: number, total: number): (number | "...")[] {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }
  const pages: (number | "...")[] = [1];
  if (current > 3) pages.push("...");
  const lo = Math.max(2, current - 2);
  const hi = Math.min(total - 1, current + 2);
  for (let p = lo; p <= hi; p++) pages.push(p);
  if (current < total - 2) pages.push("...");
  pages.push(total);
  return pages;
}

export default function Pagination({
  currentPage,
  totalPages,
  searchParams,
  basePath = "/record",
}: {
  currentPage: number;
  totalPages: number;
  searchParams: SearchParams;
  basePath?: string;
}) {
  const pages = pageRange(currentPage, totalPages);

  const btnBase =
    "flex items-center justify-center text-sm rounded-lg transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/30";
  const btnActive =
    "bg-gold text-record font-semibold";
  const btnIdle =
    "bg-groove text-parchment hover:bg-wax hover:text-cream";
  const btnDisabled =
    "bg-sleeve text-ash cursor-not-allowed select-none";

  return (
    <nav
      aria-label="Page navigation"
      className="flex items-center justify-center gap-1 mt-10 flex-wrap"
    >
      {/* Previous */}
      {currentPage > 1 ? (
        <Link
          href={buildUrl(currentPage - 1, searchParams, basePath)}
          className={`${btnBase} ${btnIdle} px-4 py-2.5`}
        >
          ← Previous
        </Link>
      ) : (
        <span className={`${btnBase} ${btnDisabled} px-4 py-2.5`}>
          ← Previous
        </span>
      )}

      {/* Page numbers */}
      {pages.map((p, i) =>
        p === "..." ? (
          <span
            key={`ellipsis-${i}`}
            className="px-1.5 text-dust text-sm select-none"
          >
            …
          </span>
        ) : (
          <Link
            key={p}
            href={buildUrl(p, searchParams, basePath)}
            aria-current={p === currentPage ? "page" : undefined}
            className={`${btnBase} ${p === currentPage ? btnActive : btnIdle} w-11 h-11`}
          >
            {p}
          </Link>
        )
      )}

      {/* Next */}
      {currentPage < totalPages ? (
        <Link
          href={buildUrl(currentPage + 1, searchParams, basePath)}
          className={`${btnBase} ${btnIdle} px-4 py-2.5`}
        >
          Next →
        </Link>
      ) : (
        <span className={`${btnBase} ${btnDisabled} px-4 py-2.5`}>
          Next →
        </span>
      )}
    </nav>
  );
}
