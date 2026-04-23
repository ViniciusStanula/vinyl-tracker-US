/**
 * Normalizes an inverted "LAST, FIRST" artist name to "First Last".
 * Amazon sometimes stores names in this format (e.g., "SWIFT,TAYLOR").
 */
function uninvertName(name: string): string {
  if (!name.includes(",")) return name;
  const [last, ...rest] = name.split(",");
  const first = rest.join(",").trim();
  return first ? `${first} ${last.trim()}` : name;
}

/**
 * Generates a URL-friendly slug from an artist name.
 * Must produce consistent results on every call with the same input
 * since it's used both to build links and to resolve them in the artist page.
 *
 * Normalizes inverted "LAST,FIRST" format before slugifying so that
 * "SWIFT,TAYLOR" and "Taylor Swift" both produce "taylor-swift".
 */
export function slugifyArtist(name: string): string {
  return uninvertName(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // strip accents
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-") // non-alphanumeric → hyphens
    .replace(/^-+|-+$/g, "") // trim leading/trailing hyphens
    .substring(0, 60);
}
