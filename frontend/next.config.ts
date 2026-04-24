import type { NextConfig } from "next";

const SECURITY_HEADERS = [
  { key: "X-Frame-Options", value: "DENY" },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=()" },
  {
    key: "Content-Security-Policy",
    value: [
      "default-src 'self'",
      // GTM_PLACEHOLDER — add https://www.googletagmanager.com https://www.google-analytics.com when GTM is configured
      "script-src 'self' 'unsafe-inline'",
      "style-src 'self' 'unsafe-inline'",
      "img-src 'self' https: data:",
      "font-src 'self'",
      // GTM_PLACEHOLDER — add https://www.google-analytics.com https://www.googletagmanager.com to connect-src when GTM is configured
      "connect-src 'self' https://*.supabase.co https://ws.audioscrobbler.com",
      // GTM_PLACEHOLDER — add https://www.googletagmanager.com to frame-src when GTM is configured
      "frame-src 'none'",
      "object-src 'none'",
      "base-uri 'self'",
    ].join("; "),
  },
];

const nextConfig: NextConfig = {
  compress: true,
  poweredByHeader: false,
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "m.media-amazon.com" },
      { protocol: "https", hostname: "images-na.ssl-images-amazon.com" },
      { protocol: "https", hostname: "images-fe.ssl-images-amazon.com" },
      { protocol: "https", hostname: "*.media-amazon.com" },
    ],
    // Allow moderate compression quality — album art is already CDN-optimized
    // unoptimized is set per-image in DiscoCard to avoid Vercel quota consumption
  },
  async redirects() {
    return [
      { source: "/artista/:slug", destination: "/artist/:slug", permanent: true },
      { source: "/disco/:slug",   destination: "/record/:slug",  permanent: true },
      { source: "/disco",         destination: "/record",         permanent: true },
      { source: "/estilo/:slug",  destination: "/genre/:slug",   permanent: true },
      { source: "/style/:slug",   destination: "/genre/:slug",   permanent: true },
      { source: "/sobre",         destination: "/about",          permanent: true },
    ];
  },
  async headers() {
    return [
      {
        source: "/(.*)",
        headers: SECURITY_HEADERS,
      },
    ];
  },
};

export default nextConfig;
