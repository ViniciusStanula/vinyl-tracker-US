export default function Loading() {
  return (
    <main className="max-w-3xl mx-auto px-4 py-8">
      <div className="h-4 w-48 bg-zinc-800 rounded animate-pulse mb-6" />
      <div className="flex flex-col sm:flex-row gap-6 mb-8">
        <div className="w-full sm:w-72 sm:h-72 aspect-square bg-zinc-800 rounded-2xl animate-pulse shrink-0" />
        <div className="flex-1 flex flex-col justify-between">
          <div className="space-y-2">
            <div className="h-3 w-24 bg-zinc-800 rounded animate-pulse" />
            <div className="h-8 w-full bg-zinc-800 rounded animate-pulse" />
            <div className="h-8 w-3/4 bg-zinc-800 rounded animate-pulse" />
          </div>
          <div className="mt-6 space-y-3">
            <div className="h-12 w-40 bg-zinc-700 rounded-full animate-pulse" />
            <div className="h-3 w-52 bg-zinc-800 rounded animate-pulse" />
          </div>
        </div>
      </div>
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 mb-6 animate-pulse">
        <div className="h-5 w-40 bg-zinc-800 rounded mb-4" />
        <div className="grid grid-cols-3 gap-3 mb-5">
          {[0, 1, 2].map((i) => (
            <div key={i} className="bg-zinc-800 rounded-lg p-3 h-16" />
          ))}
        </div>
        <div className="h-24 bg-zinc-800 rounded" />
      </div>
      <div className="mt-10">
        <div className="h-6 w-48 bg-zinc-800 rounded animate-pulse mb-4" />
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="bg-zinc-900 rounded-xl overflow-hidden animate-pulse">
              <div className="aspect-square bg-zinc-800" />
              <div className="p-3 space-y-2">
                <div className="h-3 bg-zinc-800 rounded w-1/2" />
                <div className="h-4 bg-zinc-800 rounded" />
                <div className="h-5 bg-zinc-700 rounded w-1/3 mt-2" />
                <div className="h-7 bg-zinc-800 rounded mt-2" />
              </div>
            </div>
          ))}
        </div>
      </div>
    </main>
  );
}
