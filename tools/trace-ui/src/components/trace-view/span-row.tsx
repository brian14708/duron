import { ChevronDown, ChevronRight, Link2 } from "lucide-react";

import type { Span } from "@/lib/trace";
import { cn } from "@/lib/utils";

import { formatDuration, formatSpanName, getSpanKindMeta } from "./span-utils";
import { HEADING_WIDTH } from "./timeline-utils";

interface SpanRowProps {
  span: Span;
  traceId: string;
  depth: number;
  traceDuration: number;
  orderIndex: Map<string, number>;
  matchActive: boolean;
  matchedIds: Set<string>;
  selectedSpan: Span | null | undefined;
  linkedSpanIds: Set<string>;
  onSpanClick: ((span: Span | null) => void) | undefined;
  timeToPixel: (time: number) => number;
  isSpanCollapsed: (traceId: string, spanId: string) => boolean;
  toggleSpanCollapse: (traceId: string, spanId: string) => void;
}

export function SpanRow({
  span,
  traceId,
  depth,
  traceDuration,
  orderIndex,
  matchActive,
  matchedIds,
  selectedSpan,
  linkedSpanIds,
  onSpanClick,
  timeToPixel,
  isSpanCollapsed,
  toggleSpanCollapse,
}: SpanRowProps) {
  const duration = span.endTime - span.startTime;
  const meta = getSpanKindMeta(span);
  const Icon = meta.icon;
  const kindVar = `var(--kind-${meta.key})`;

  const isSelected = selectedSpan?.id === span.id;
  const hasChildren = span.children && span.children.length > 0;
  const hasLinks = span.links && span.links.length > 0;
  const collapsed = isSpanCollapsed(traceId, span.id);

  const isLinkedToSelected = linkedSpanIds.has(span.id);
  const isRelated = isSelected || isLinkedToSelected;

  // Dim rules: selection dims unrelated spans; an active search dims non-matches.
  const dimmedBySelection = selectedSpan && !isRelated;
  const dimmedBySearch = matchActive && !matchedIds.has(span.id);
  const dimmed = dimmedBySelection || dimmedBySearch;

  const leftPosition = timeToPixel(span.startTime);
  const width = Math.max(timeToPixel(span.endTime) - leftPosition, 3);
  const pct = traceDuration > 0 ? (duration / traceDuration) * 100 : 0;

  // Staggered load reveal, capped so deep traces don't animate forever.
  const order = orderIndex.get(span.id) ?? 0;
  const delay = `${Math.min(order, 40) * 18}ms`;

  return (
    <>
      <div
        className={cn(
          "group animate-reveal border-border/40 relative flex cursor-pointer items-stretch border-b transition-colors",
          "hover:bg-primary/[0.04]",
          isSelected && "bg-primary/[0.07]",
          dimmed && "opacity-35 hover:opacity-70",
        )}
        style={{ animationDelay: delay }}
        onClick={(e) => {
          e.stopPropagation();
          onSpanClick?.(isSelected ? null : span);
        }}
      >
        {/* Selection / relation rail */}
        <span
          aria-hidden
          className={cn(
            "absolute top-0 left-0 h-full w-[2px] transition-opacity",
            isSelected
              ? "opacity-100"
              : isLinkedToSelected
                ? "opacity-60"
                : "opacity-0",
          )}
          style={{
            background: kindVar,
            boxShadow: isSelected
              ? `0 0 10px color-mix(in oklch, ${kindVar} 70%, transparent)`
              : "none",
          }}
        />

        {/* Left: tree + identity */}
        <div
          className="flex min-w-0 flex-shrink-0 items-center gap-1.5 py-1.5 pr-3"
          style={{ width: HEADING_WIDTH, paddingLeft: `${12 + depth * 16}px` }}
        >
          {hasChildren ? (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                toggleSpanCollapse(traceId, span.id);
              }}
              className="text-muted-foreground hover:bg-secondary hover:text-foreground flex-shrink-0 rounded p-0.5"
              aria-label={collapsed ? "Expand" : "Collapse"}
            >
              {collapsed ? (
                <ChevronRight className="h-3.5 w-3.5" />
              ) : (
                <ChevronDown className="h-3.5 w-3.5" />
              )}
            </button>
          ) : (
            <span className="w-[18px] flex-shrink-0" />
          )}

          <Icon
            className="h-3.5 w-3.5 flex-shrink-0"
            style={{ color: kindVar }}
            aria-hidden
          />

          <span className="text-foreground truncate font-mono text-[13px]">
            {formatSpanName(span.name)}
          </span>

          {hasLinks && (
            <Link2 className="text-primary h-3 w-3 flex-shrink-0" aria-hidden />
          )}
          {span.incomplete && (
            <span
              className="text-kind-barrier ml-0.5 flex-shrink-0 text-[10px] tracking-wide uppercase"
              title="Incomplete span (no end event)"
            >
              live
            </span>
          )}
        </div>

        {/* Right: timeline track */}
        <div className="relative flex-1 self-center pr-6">
          <div className="relative h-6">
            {/* The span bar */}
            <div
              className="absolute top-1/2 h-[9px] -translate-y-1/2 rounded-[3px] transition-[filter,box-shadow]"
              style={{
                left: `${leftPosition}px`,
                width: `${width}px`,
                background: span.incomplete
                  ? `linear-gradient(90deg, ${kindVar}, color-mix(in oklch, ${kindVar} 15%, transparent))`
                  : kindVar,
                boxShadow: isRelated
                  ? `0 0 12px color-mix(in oklch, ${kindVar} 60%, transparent)`
                  : `0 0 0 1px color-mix(in oklch, ${kindVar} 25%, transparent)`,
              }}
            />

            {/* Duration / share readout, placed just past the bar */}
            <span
              className="text-muted-foreground pointer-events-none absolute top-1/2 -translate-y-1/2 font-mono text-[10.5px] whitespace-nowrap tabular-nums"
              style={{ left: `${leftPosition + width + 8}px` }}
            >
              {formatDuration(duration)}
              {pct >= 1 && (
                <span className="text-muted-foreground/60 ml-1.5">
                  {pct.toFixed(0)}%
                </span>
              )}
            </span>

            {/* Instant events as vertical ticks */}
            {span.events?.map((event, eventIndex) => {
              const eventLeft = timeToPixel(event.time);
              return (
                <div
                  key={`${span.id}-event-${eventIndex}`}
                  className="group/ev absolute top-1/2 h-4 w-px -translate-x-1/2 -translate-y-1/2"
                  style={{ left: `${eventLeft}px` }}
                >
                  <div className="bg-kind-event h-full w-full" />
                  <div className="border-border bg-popover text-popover-foreground pointer-events-none absolute bottom-full left-1/2 z-50 mb-1 -translate-x-1/2 rounded border px-2 py-1 font-mono text-[10px] whitespace-nowrap opacity-0 shadow-lg transition-opacity group-hover/ev:opacity-100">
                    {event.name}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Hover timing tooltip anchored to the row */}
        <div className="border-border bg-popover text-popover-foreground pointer-events-none absolute top-full right-6 z-40 mt-0.5 hidden rounded border px-2 py-1 font-mono text-[10px] shadow-lg group-hover:block">
          {meta.label} · {formatDuration(duration)}
        </div>
      </div>

      {hasChildren &&
        !collapsed &&
        span.children!.map((child) => (
          <SpanRow
            key={child.id}
            span={child}
            traceId={traceId}
            depth={depth + 1}
            traceDuration={traceDuration}
            orderIndex={orderIndex}
            matchActive={matchActive}
            matchedIds={matchedIds}
            selectedSpan={selectedSpan}
            linkedSpanIds={linkedSpanIds}
            onSpanClick={onSpanClick}
            timeToPixel={timeToPixel}
            isSpanCollapsed={isSpanCollapsed}
            toggleSpanCollapse={toggleSpanCollapse}
          />
        ))}
    </>
  );
}
