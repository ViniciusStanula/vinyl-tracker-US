export function slugifyStyle(tag: string): string {
  return tag
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function parseStyleTags(lastfmTags: string | null): string[] {
  if (!lastfmTags) return [];
  return lastfmTags
    .split(", ")
    .map((t) => t.trim())
    .filter(Boolean);
}

/** Returns the N most frequent tags across an array of lastfmTags strings.
 *  Tags that exactly match `exclude` (case-insensitive) are skipped. */
export function getTopStyles(
  allTags: (string | null)[],
  limit = 5,
  exclude?: string
): string[] {
  const excludeLower = exclude?.toLowerCase();
  const counts = new Map<string, number>();
  for (const tags of allTags) {
    for (const tag of parseStyleTags(tags)) {
      if (excludeLower && tag.toLowerCase() === excludeLower) continue;
      counts.set(tag, (counts.get(tag) ?? 0) + 1);
    }
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([tag]) => tag);
}
