import Link from "next/link";
import { slugifyStyle } from "@/lib/styleUtils";

export default function StyleTags({ tags }: { tags: string[] }) {
  if (tags.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1.5 mt-2">
      {tags.map((tag) => (
        <Link
          key={tag}
          href={`/estilo/${slugifyStyle(tag)}`}
          className="inline-flex items-center text-xs px-2.5 py-0.5 rounded-full bg-groove border border-wax/40 text-dust hover:text-parchment hover:border-wax/70 transition-colors"
        >
          {tag}
        </Link>
      ))}
    </div>
  );
}
