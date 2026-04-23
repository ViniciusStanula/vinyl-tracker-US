import type { Metadata, Viewport } from "next";
import { Fraunces, DM_Sans } from "next/font/google";
import "./globals.css";
import Navbar from "@/components/Navbar";

/* Fraunces — optical-size variable serif; editorial, distinctive */
const fraunces = Fraunces({
  variable: "--font-fraunces",
  subsets: ["latin"],
  display: "swap",
});

/* DM Sans — clean, humanist sans-serif for body copy */
const dmSans = DM_Sans({
  variable: "--font-dm-sans",
  subsets: ["latin"],
  display: "swap",
});

const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL ?? "http://localhost:3000";

const DEFAULT_TITLE = "Vinyl Tracker — Best Deals on Vinyl Records";
const DEFAULT_DESC  =
  "Track vinyl record prices on Amazon. Full price history and deal alerts to find the best time to buy.";

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
};

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: DEFAULT_TITLE,
  description: DEFAULT_DESC,
  openGraph: {
    type: "website",
    locale: "en_US",
    siteName: "Vinyl Tracker",
    title: DEFAULT_TITLE,
    description: DEFAULT_DESC,
  },
  twitter: {
    card: "summary",
    title: DEFAULT_TITLE,
    description: DEFAULT_DESC,
  },
};

const organizationJsonLd = JSON.stringify({
  "@context": "https://schema.org",
  "@type": "Organization",
  name: "Vinyl Tracker",
  url: SITE_URL,
  description: "Vinyl record price tracker on Amazon.",
});

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${fraunces.variable} ${dmSans.variable}`}>
      <head>
        {/* GTM_PLACEHOLDER — insert your Google Tag Manager <script> snippet here */}
      </head>
      <body className="min-h-screen bg-record text-cream antialiased">
        {/* GTM_NOSCRIPT_PLACEHOLDER — insert your GTM <noscript><iframe> snippet here */}
        {/* eslint-disable-next-line react/no-danger */}
        <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: organizationJsonLd }} />
        <Navbar />
        {children}
      </body>
    </html>
  );
}
