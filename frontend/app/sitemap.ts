import type { MetadataRoute } from "next";
import { prisma } from "@/lib/prisma";
import { slugifyArtist } from "@/lib/slugify";
import { slugifyStyle } from "@/lib/styleUtils";

export const revalidate = 21600; // regenerate every 6 hours

const base =
  process.env.NEXT_PUBLIC_SITE_URL ?? "http://localhost:3000";

export async function generateSitemaps() {
  return [{ id: "static" }, { id: "artists" }, { id: "records" }, { id: "genres" }];
}

export default async function sitemap(props: {
  id: Promise<string>;
}): Promise<MetadataRoute.Sitemap> {
  const id = await props.id;

  if (id === "static") {
    return [{ url: base, changeFrequency: "daily", priority: 1 }];
  }

  if (id === "artists") {
    try {
      const artistRows = await prisma.record.findMany({
        select: { artist: true },
        distinct: ["artist"],
      });

      const seenSlugs = new Set<string>();
      const routes: MetadataRoute.Sitemap = [];

      for (const { artist } of artistRows) {
        const slug = slugifyArtist(artist);
        if (!slug || seenSlugs.has(slug)) continue;
        seenSlugs.add(slug);
        routes.push({
          url: `${base}/artist/${slug}`,
          changeFrequency: "weekly",
          priority: 0.6,
        });
      }

      return routes;
    } catch {
      return [];
    }
  }

  if (id === "records") {
    try {
      const records = await prisma.record.findMany({
        select: { slug: true, updatedAt: true },
      });

      return records.map((r) => ({
        url: `${base}/record/${r.slug}`,
        lastModified: r.updatedAt,
        changeFrequency: "daily",
        priority: 0.8,
      }));
    } catch {
      return [];
    }
  }

  if (id === "genres") {
    try {
      const rows = await prisma.$queryRaw<{ tag: string }[]>`
        SELECT DISTINCT unnest(string_to_array(lastfm_tags, ', ')) AS tag
        FROM "Record"
        WHERE lastfm_tags IS NOT NULL AND lastfm_tags != ''
      `;

      const seenSlugs = new Set<string>();
      const routes: MetadataRoute.Sitemap = [];

      for (const { tag } of rows) {
        const slug = slugifyStyle(tag);
        if (!slug || seenSlugs.has(slug)) continue;
        seenSlugs.add(slug);
        routes.push({
          url: `${base}/genre/${slug}`,
          changeFrequency: "weekly",
          priority: 0.5,
        });
      }

      return routes;
    } catch {
      return [];
    }
  }

  return [];
}
