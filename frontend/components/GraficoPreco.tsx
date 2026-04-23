"use client";

import { useState } from "react";

interface PricePoint {
  date: string;      // e.g. "04/13"
  dateFull: string;  // e.g. "04/13/2026, 10:00 AM"
  value: number;
}

interface Props {
  points: PricePoint[];
}

const W = 500;
const H = 160;
const PAD = { top: 12, right: 16, bottom: 32, left: 70 };
const cW = W - PAD.left - PAD.right;
const cH = H - PAD.top - PAD.bottom;

export default function GraficoPreco({ points }: Props) {
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);

  if (points.length < 2) {
    return (
      <p className="text-center py-6 text-dust text-sm">
        Waiting for more data points to display the chart.
      </p>
    );
  }

  const values = points.map((p) => p.value);
  const vMin = Math.min(...values);
  const vMax = Math.max(...values);
  const yMin = vMin * 0.80;
  const yMax = vMax * 1.10;
  const yRange = yMax - yMin;

  const tx = (i: number) =>
    PAD.left + (i / (points.length - 1)) * cW;
  const ty = (v: number) =>
    PAD.top + (1 - (v - yMin) / yRange) * cH;

  const linePath = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${tx(i).toFixed(1)} ${ty(p.value).toFixed(1)}`)
    .join(" ");
  const fillPath =
    linePath +
    ` L ${tx(points.length - 1).toFixed(1)} ${(H - PAD.bottom).toFixed(1)}` +
    ` L ${tx(0).toFixed(1)} ${(H - PAD.bottom).toFixed(1)} Z`;

  const yTicks = Array.from({ length: 5 }, (_, i) => {
    const v = yMin + (yRange * i) / 4;
    return {
      y: ty(v),
      label: `$${Math.round(v)}`,
    };
  }).reverse();

  const xTickCount = Math.min(4, points.length);
  const xTicks = Array.from({ length: xTickCount }, (_, i) => {
    const idx = Math.round((i / (xTickCount - 1)) * (points.length - 1));
    return { x: tx(idx), label: points[idx].date };
  });

  const minIdx = values.reduce((best, v, i) => v <= values[best] ? i : best, 0);
  const maxIdx = values.reduce((best, v, i) => v >= values[best] ? i : best, 0);
  const hasRange = vMin !== vMax;

  const hovered = hoveredIdx !== null ? points[hoveredIdx] : null;
  const fmt = (v: number) =>
    v.toLocaleString("en-US", { style: "currency", currency: "USD" });

  return (
    <div className="select-none">
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full overflow-visible"
        role="img"
        aria-label="Price history chart"
      >
        <defs>
          <linearGradient id="pg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" style={{ stopColor: "var(--color-gold)", stopOpacity: 0.30 }} />
            <stop offset="100%" style={{ stopColor: "var(--color-gold)", stopOpacity: 0.02 }} />
          </linearGradient>
        </defs>

        {/* Grid lines + Y labels */}
        {yTicks.map((tick, i) => (
          <g key={i}>
            <line
              x1={PAD.left}
              y1={tick.y}
              x2={W - PAD.right}
              y2={tick.y}
              className="stroke-groove"
              strokeWidth="0.5"
              strokeDasharray="3,3"
            />
            <text
              x={PAD.left - 6}
              y={tick.y + 3.5}
              textAnchor="end"
              className="fill-dust"
              fontSize="8.5"
            >
              {tick.label}
            </text>
          </g>
        ))}

        {/* X labels */}
        {xTicks.map((tick, i) => (
          <text
            key={i}
            x={tick.x}
            y={H - PAD.bottom + 14}
            textAnchor="middle"
            className="fill-patina"
            fontSize="8"
          >
            {tick.label}
          </text>
        ))}

        {/* Area fill */}
        <path d={fillPath} fill="url(#pg)" />

        {/* Line */}
        <path
          d={linePath}
          fill="none"
          className="stroke-gold"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />

        {/* Min annotation — green dot + label */}
        {hasRange && (
          <g>
            <circle
              cx={tx(minIdx)}
              cy={ty(vMin)}
              r="5"
              className="fill-deallit stroke-record"
              strokeWidth="1.5"
            />
            <text
              x={tx(minIdx)}
              y={ty(vMin) + 15}
              textAnchor="middle"
              className="fill-deallit"
              fontSize="8"
              fontWeight="600"
            >
              Min.
            </text>
          </g>
        )}

        {/* Max annotation — red dot + label */}
        {hasRange && (
          <g>
            <circle
              cx={tx(maxIdx)}
              cy={ty(vMax)}
              r="5"
              className="fill-cut stroke-record"
              strokeWidth="1.5"
            />
            <text
              x={tx(maxIdx)}
              y={ty(vMax) - 9}
              textAnchor="middle"
              className="fill-cut"
              fontSize="8"
              fontWeight="600"
            >
              Max.
            </text>
          </g>
        )}

        {/* Hover crosshair */}
        {hoveredIdx !== null && (
          <>
            <line
              x1={tx(hoveredIdx)}
              y1={PAD.top}
              x2={tx(hoveredIdx)}
              y2={H - PAD.bottom}
              className="stroke-gold"
              strokeWidth="1"
              strokeDasharray="3,2"
              opacity="0.5"
            />
            <circle
              cx={tx(hoveredIdx)}
              cy={ty(points[hoveredIdx].value)}
              r="4.5"
              className="fill-gold stroke-record"
              strokeWidth="2"
            />
          </>
        )}

        {/* Invisible hit rect for hover detection */}
        <rect
          x={PAD.left}
          y={PAD.top}
          width={cW}
          height={cH}
          fill="transparent"
          style={{ cursor: "crosshair" }}
          onMouseMove={(e) => {
            const rect = e.currentTarget.getBoundingClientRect();
            const ratio = (e.clientX - rect.left) / rect.width;
            const idx = Math.max(0, Math.min(points.length - 1, Math.round(ratio * (points.length - 1))));
            setHoveredIdx(idx);
          }}
          onMouseLeave={() => setHoveredIdx(null)}
          onTouchMove={(e) => {
            e.preventDefault();
            const touch = e.touches[0];
            const rect = e.currentTarget.getBoundingClientRect();
            const ratio = (touch.clientX - rect.left) / rect.width;
            const idx = Math.max(0, Math.min(points.length - 1, Math.round(ratio * (points.length - 1))));
            setHoveredIdx(idx);
          }}
          onTouchEnd={() => setHoveredIdx(null)}
        />
      </svg>

      {/* Tooltip row */}
      <div className="h-6 flex items-center justify-center gap-2 text-xs">
        {hovered ? (
          <>
            <span className="text-gold font-bold tabular-nums">{fmt(hovered.value)}</span>
            <span className="text-dust">·</span>
            <span className="text-parchment">{hovered.dateFull}</span>
          </>
        ) : (
          <span className="text-ash">Tap or hover to see the price</span>
        )}
      </div>
    </div>
  );
}
