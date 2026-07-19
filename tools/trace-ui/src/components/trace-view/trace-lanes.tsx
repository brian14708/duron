import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { Span, Trace } from "@/lib/trace";

import { SpanRow } from "./span-row";
import { formatDuration } from "./span-utils";
import {
  HEADING_WIDTH,
  calculateTimelineBounds,
  createTimeToPixel,
  generateTicks,
} from "./timeline-utils";

const TRACK_PADDING = 24; // must match SpanRow track `pr-6`
const RULER_HEIGHT = 32;

interface TraceLanesProps {
  traces?: Trace[];
  selectedSpan?: Span | null;
  searchQuery?: string;
  onSpanClick?: (span: Span | null) => void;
}

interface FlatSpan {
  span: Span;
  traceId: string;
  depth: number;
}

// Depth-first walk over the visible tree, honoring collapse state.
function flatten(
  spans: Span[],
  traceId: string,
  depth: number,
  isCollapsed: (traceId: string, spanId: string) => boolean,
  out: FlatSpan[],
): void {
  for (const span of spans) {
    out.push({ span, traceId, depth });
    if (span.children?.length && !isCollapsed(traceId, span.id)) {
      flatten(span.children, traceId, depth + 1, isCollapsed, out);
    }
  }
}

export function TraceLanes({
  traces = [],
  selectedSpan,
  searchQuery = "",
  onSpanClick,
}: TraceLanesProps) {
  const [collapsedSpans, setCollapsedSpans] = useState<
    Map<string, Set<string>>
  >(new Map());

  const timelineRef = useRef<HTMLDivElement>(null);
  const [timelineWidth, setTimelineWidth] = useState(600);

  useEffect(() => {
    const updateWidth = () => {
      if (timelineRef.current) {
        const containerWidth = timelineRef.current.offsetWidth;
        setTimelineWidth(
          Math.max(containerWidth - HEADING_WIDTH - TRACK_PADDING, 100),
        );
      }
    };
    updateWidth();
    const resizeObserver = new ResizeObserver(updateWidth);
    if (timelineRef.current) resizeObserver.observe(timelineRef.current);
    window.addEventListener("resize", updateWidth);
    return () => {
      resizeObserver.disconnect();
      window.removeEventListener("resize", updateWidth);
    };
  }, []);

  const toggleSpanCollapse = (traceId: string, spanId: string) => {
    setCollapsedSpans((prev) => {
      const newMap = new Map(prev);
      const traceCollapsed = new Set(newMap.get(traceId) ?? new Set<string>());
      if (traceCollapsed.has(spanId)) traceCollapsed.delete(spanId);
      else traceCollapsed.add(spanId);
      newMap.set(traceId, traceCollapsed);
      return newMap;
    });
  };

  const isSpanCollapsed = useCallback(
    (traceId: string, spanId: string): boolean =>
      collapsedSpans.get(traceId)?.has(spanId) ?? false,
    [collapsedSpans],
  );

  // Flattened visible order — powers stagger delays and keyboard navigation.
  const flatVisible = useMemo(() => {
    const out: FlatSpan[] = [];
    for (const trace of traces) {
      flatten(trace.rootSpans, trace.id, 0, isSpanCollapsed, out);
    }
    return out;
  }, [traces, isSpanCollapsed]);

  const orderIndex = useMemo(() => {
    const map = new Map<string, number>();
    flatVisible.forEach((f, i) => map.set(f.span.id, i));
    return map;
  }, [flatVisible]);

  // Search: match on name/kind and keep the ancestor path lit.
  const query = searchQuery.trim().toLowerCase();
  const matchedIds = useMemo(() => {
    const ids = new Set<string>();
    if (!query) return ids;
    const walk = (spans: Span[], ancestors: string[]) => {
      for (const span of spans) {
        const hit =
          span.name.toLowerCase().includes(query) ||
          (span.kind ?? "").toLowerCase().includes(query);
        if (hit) {
          ids.add(span.id);
          for (const a of ancestors) ids.add(a);
        }
        if (span.children?.length) walk(span.children, [...ancestors, span.id]);
      }
    };
    for (const trace of traces) walk(trace.rootSpans, []);
    return ids;
  }, [traces, query]);

  // Arrow-key navigation across visible spans.
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      if (
        target &&
        (target.tagName === "INPUT" || target.tagName === "TEXTAREA")
      )
        return;

      if (e.key === "Escape" && selectedSpan) {
        onSpanClick?.(null);
        return;
      }
      if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
      if (flatVisible.length === 0) return;
      e.preventDefault();

      const current = selectedSpan
        ? flatVisible.findIndex((f) => f.span.id === selectedSpan.id)
        : -1;
      const delta = e.key === "ArrowDown" ? 1 : -1;
      const next =
        current === -1
          ? e.key === "ArrowDown"
            ? 0
            : flatVisible.length - 1
          : Math.min(Math.max(current + delta, 0), flatVisible.length - 1);
      onSpanClick?.(flatVisible[next].span);
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedSpan, onSpanClick, flatVisible]);

  // Reverse link index: spanId -> spans that link to it.
  const reverseLinkIndex = useMemo(() => {
    const index = new Map<string, Set<string>>();
    for (const trace of traces) {
      for (const span of trace.allSpans) {
        for (const link of span.links ?? []) {
          if (!index.has(link.span_id)) index.set(link.span_id, new Set());
          index.get(link.span_id)!.add(span.id);
        }
      }
    }
    return index;
  }, [traces]);

  const linkedSpanIds = useMemo(() => {
    if (!selectedSpan) return new Set<string>();
    const ids = new Set<string>();
    for (const link of selectedSpan.links ?? []) ids.add(link.span_id);
    for (const id of reverseLinkIndex.get(selectedSpan.id) ?? []) ids.add(id);
    return ids;
  }, [selectedSpan, reverseLinkIndex]);

  const timelineBounds = useMemo(
    () => calculateTimelineBounds(traces),
    [traces],
  );
  const timeToPixel = useMemo(
    () => createTimeToPixel(timelineBounds, timelineWidth),
    [timelineBounds, timelineWidth],
  );
  const ticks = useMemo(
    () => generateTicks(timelineBounds, timelineWidth),
    [timelineBounds, timelineWidth],
  );

  if (traces.length === 0) {
    return (
      <div className="flex h-full items-center justify-center p-6">
        <p className="text-muted-foreground font-mono text-sm">
          No traces to display
        </p>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col" onClick={() => onSpanClick?.(null)}>
      <div className="relative flex-1 overflow-auto" ref={timelineRef}>
        <div className="relative min-h-full">
          {/* Vertical gridlines + heading divider (behind rows) */}
          <div className="pointer-events-none absolute inset-0 z-0">
            <div
              className="bg-border absolute top-0 bottom-0 w-px"
              style={{ left: HEADING_WIDTH }}
            />
            {ticks.map((tick) => (
              <div
                key={`grid-${tick.time}`}
                className="bg-grid absolute top-0 bottom-0 w-px"
                style={{ left: HEADING_WIDTH + tick.x }}
              />
            ))}
          </div>

          {/* Sticky time-axis ruler */}
          <div
            className="border-border bg-background/85 sticky top-0 z-30 flex border-b backdrop-blur"
            style={{ height: RULER_HEIGHT }}
          >
            <div
              className="text-muted-foreground flex flex-shrink-0 items-center px-3 font-mono text-[10px] tracking-[0.18em] uppercase"
              style={{ width: HEADING_WIDTH }}
            >
              Span
            </div>
            <div
              className="relative flex-1"
              style={{ marginRight: TRACK_PADDING }}
            >
              {ticks.map((tick) => (
                <div
                  key={`tick-${tick.time}`}
                  className="absolute top-0 flex h-full flex-col justify-center"
                  style={{ left: tick.x }}
                >
                  <span className="border-border/70 text-muted-foreground border-l pl-1 font-mono text-[10px] tabular-nums">
                    {tick.label}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Lanes */}
          {traces.map((trace) => {
            const traceDuration = trace.endTime - trace.startTime;
            return (
              <div
                key={trace.id}
                className="border-border relative z-10 border-b"
              >
                <div
                  className="border-border bg-surface/90 sticky z-20 flex items-center justify-between border-b px-3 py-1.5 backdrop-blur"
                  style={{ top: RULER_HEIGHT }}
                >
                  <div className="flex items-center gap-2">
                    <span
                      className="bg-primary h-1.5 w-1.5 rounded-full"
                      aria-hidden
                    />
                    <span className="text-foreground font-mono text-xs font-semibold tracking-wide">
                      {trace.id}
                    </span>
                    <span className="text-muted-foreground font-mono text-[11px]">
                      {trace.allSpans.length} spans
                    </span>
                  </div>
                  <span className="text-muted-foreground font-mono text-[11px] tabular-nums">
                    {formatDuration(traceDuration)}
                  </span>
                </div>

                <div>
                  {trace.rootSpans.map((span) => (
                    <SpanRow
                      key={span.id}
                      span={span}
                      traceId={trace.id}
                      depth={0}
                      traceDuration={traceDuration}
                      orderIndex={orderIndex}
                      matchActive={query.length > 0}
                      matchedIds={matchedIds}
                      selectedSpan={selectedSpan}
                      linkedSpanIds={linkedSpanIds}
                      onSpanClick={onSpanClick}
                      timeToPixel={timeToPixel}
                      isSpanCollapsed={isSpanCollapsed}
                      toggleSpanCollapse={toggleSpanCollapse}
                    />
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
