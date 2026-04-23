import { defineConfig } from 'prisma/config'
import 'dotenv/config'

// DIRECT_URL is used locally for migrations (db push).
// On Vercel only DATABASE_URL is set, so we fall back to it.
export default defineConfig({
  datasource: {
    url: process.env.DIRECT_URL ?? process.env.DATABASE_URL,
  },
})
