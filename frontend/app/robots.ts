import type { MetadataRoute } from "next";

export default function robots(): MetadataRoute.Robots {
  const base = process.env.NEXT_PUBLIC_SITE_URL ?? "http://localhost:3000";

  return {
    rules: {
      userAgent: "*",
      allow: "/",
    },
    sitemap: [
      `${base}/sitemap/static.xml`,
      `${base}/sitemap/artists.xml`,
      `${base}/sitemap/records.xml`,
      `${base}/sitemap/styles.xml`,
    ],
  };
}
