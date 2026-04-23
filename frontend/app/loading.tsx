/** Shown by Next.js App Router while the homepage is loading. */
export default function Loading() {
  return (
    <main className="max-w-7xl mx-auto px-4 py-8">
      {/* Hero skeleton */}
      <div className="mb-8 bg-sleeve border border-groove rounded-2xl px-6 py-7 animate-pulse">
        <div className="h-3 w-40 bg-groove rounded mb-3" />
        <div className="h-9 w-72 bg-groove rounded-lg mb-2" />
        <div className="h-5 w-48 bg-groove rounded" />
      </div>

      {/* SortBar skeleton */}
      <div className="h-14 bg-sleeve border border-groove rounded-xl mb-5 animate-pulse" />

      {/* Result count skeleton */}
      <div className="h-4 w-36 bg-groove rounded animate-pulse mb-5" />

      {/* Card grid skeleton — matches the 4-col layout */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
        {Array.from({ length: 8 }).map((_, i) => (
          <div
            key={i}
            className="bg-sleeve border border-groove rounded-xl overflow-hidden animate-pulse"
          >
            <div className="aspect-square bg-label" />
            <div className="p-3 space-y-2">
              <div className="h-3 bg-groove rounded w-1/2" />
              <div className="h-4 bg-groove rounded" />
              <div className="h-4 bg-groove rounded w-3/4" />
              <div className="h-3 bg-groove rounded w-2/5 mt-1" />
              <div className="h-6 bg-wax/40 rounded w-1/3 mt-2" />
              <div className="h-7 bg-groove rounded mt-3" />
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
