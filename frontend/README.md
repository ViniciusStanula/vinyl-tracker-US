# Vinyl Tracker — Frontend

Next.js 15 frontend for the Vinyl Tracker price-tracking site.

## Configuration

Copy `.env.example` to `.env.local` and fill in the values:

```bash
cp .env.example .env.local
```

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string (Transaction Pooler URL from Supabase, port 6543) |
| `NEXT_PUBLIC_SITE_URL` | Yes | Production domain, e.g. `https://your-domain.com` — used for canonical URLs and sitemaps |
| `REVALIDATE_SECRET` | Yes | Long random string — authorizes ISR cache revalidation requests from the crawler |

## Development

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## Production

```bash
npm run build
npm start
```

Deploy to Vercel: connect the repo and set the environment variables in the Vercel dashboard. The `NEXT_PUBLIC_SITE_URL` must be set to your actual domain for sitemaps and canonical tags to work correctly.

## Tracking integrations (GTM / IndexNow / GSC)

Placeholders are marked with `GTM_PLACEHOLDER` / `INDEXNOW_PLACEHOLDER` / `GSC_PLACEHOLDER` in the source. Search for those strings to find where to insert your new credentials:

- **Google Tag Manager**: `app/layout.tsx` — insert your GTM container snippet
- **CSP headers**: `next.config.ts` — uncomment the GTM/GA entries when GTM is configured
- **IndexNow**: `crawler/indexnow.py` — set `INDEXNOW_KEY` and `INDEXNOW_HOST` env vars; rename `public/a42e4483f1d942f99203b177055c71a4.txt` to `public/<your-key>.txt` with just the key as its content
- **Google Search Console**: add the GSC verification meta tag inside the `<head>` in `app/layout.tsx`
