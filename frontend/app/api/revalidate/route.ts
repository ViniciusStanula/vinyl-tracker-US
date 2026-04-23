import { revalidatePath, revalidateTag } from "next/cache";
import { NextRequest, NextResponse } from "next/server";

export async function POST(request: NextRequest) {
  const body = await request.json().catch(() => null);
  if (!body || body.secret !== process.env.REVALIDATE_SECRET) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  revalidatePath("/", "layout");
  revalidateTag("prices", { expire: 0 });
  return NextResponse.json({ revalidated: true, at: new Date().toISOString() });
}
