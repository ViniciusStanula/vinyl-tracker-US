import { NextRequest, NextResponse } from "next/server";
import { cachedQueryDiscos } from "@/lib/queryDiscos";

const ALLOWED_SORTS = new Set(["discount", "lowest-price", "highest-price", "top-rated", "az", "deals"]);

export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;

  const q           = (sp.get("q") ?? "").slice(0, 200);
  const sortRaw     = sp.get("sort") ?? "discount";
  const sort        = ALLOWED_SORTS.has(sortRaw) ? sortRaw : "discount";
  const artista     = sp.get("artista") || undefined;
  const page        = Math.max(1, parseInt(sp.get("page") ?? "1", 10));
  const precoMaxStr = sp.get("precoMax");
  const precoMax    = precoMaxStr ? Number(precoMaxStr) : null;

  try {
    const { items, total, totalPages } = await cachedQueryDiscos({
      searchTerm: q.trim(),
      sort,
      artist: artista,
      precoMax,
      page,
    });
    return NextResponse.json({ items, total, totalPages, currentPage: page });
  } catch {
    return NextResponse.json(
      { error: "Internal error fetching records" },
      { status: 500 }
    );
  }
}
