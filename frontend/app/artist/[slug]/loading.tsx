export default function Loading() {
  return (
    <main className="max-w-7xl mx-auto px-4 py-8">
      <div className="h-4 w-32 bg-zinc-800 rounded animate-pulse mb-6" />
      <div className="mb-6">
        <div className="h-10 w-56 bg-zinc-800 rounded-lg animate-pulse mb-2" />
        <div className="h-4 w-28 bg-zinc-800 rounded animate-pulse" />
      </div>
      <div className="h-14 bg-zinc-900 border border-zinc-800 rounded-xl mb-4 animate-pulse" />
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} className="bg-zinc-900 rounded-xl overflow-hidden animate-pulse">
            <div className="aspect-square bg-zinc-800" />
            <div className="p-3 space-y-2">
              <div className="h-3 bg-zinc-800 rounded w-1/2" />
              <div className="h-4 bg-zinc-800 rounded" />
              <div className="h-4 bg-zinc-800 rounded w-3/4" />
              <div className="h-3 bg-zinc-800 rounded w-2/5 mt-1" />
              <div className="h-6 bg-zinc-700 rounded w-1/3 mt-2" />
              <div className="h-7 bg-zinc-800 rounded mt-3" />
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
