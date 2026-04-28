const TITLE_LIMIT = 60;
const DESC_LIMIT  = 155;

export const SITE_URL = (() => {
  const v = process.env.NEXT_PUBLIC_SITE_URL;
  if (!v && process.env.NODE_ENV === "production")
    throw new Error("NEXT_PUBLIC_SITE_URL must be set in production");
  return v ?? "http://localhost:3000";
})();

/**
 * If title exceeds Google's display limit, drop the suffix after the last " | ".
 * Falls back to a hard slice if no pipe separator exists.
 */
export function truncateTitle(title: string, limit = TITLE_LIMIT): string {
  if (title.length <= limit) return title;
  const pipeIdx = title.lastIndexOf(" | ");
  return pipeIdx !== -1 ? title.slice(0, pipeIdx) : title.slice(0, limit);
}

/**
 * Truncates description to Google's snippet limit, breaking at a word boundary.
 */
export function truncateDesc(desc: string, limit = DESC_LIMIT): string {
  if (desc.length <= limit) return desc;
  const cut = desc.slice(0, limit);
  const lastSpace = cut.lastIndexOf(" ");
  return (lastSpace > 0 ? cut.slice(0, lastSpace) : cut) + "…";
}
