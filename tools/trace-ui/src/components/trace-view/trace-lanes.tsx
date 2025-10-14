import { useEffect, useMemo, useRef, useState } from "react";

import type { Span, Trace } from "@/lib/trace";

import { SpanRow } from "./span-row";
import { formatDuration } from "./span-utils";
import {
  HEADING_WIDTH,
  calculateTimelineBounds,
  createTimeToPixel,
} from "./timeline-utils";

interface TraceLanesProps {
  traces?: Trace[];
  selectedSpan?: Span | null;
  onSpanClick?: (span: Span | null) => void;
}

export function TraceLanes({
  traces = [],
  selectedSpan,
  onSpanClick,
}: TraceLanesProps) {
  // Track collapsed spans per trace: Map<traceId, Set<spanId>>
  const [collapsedSpans, setCollapsedSpans] = useState<
    Map<string, Set<string>>
  >(new Map());

  // Track timeline width dynamically
  const timelineRef = useRef<HTMLDivElement>(null);
  const [timelineWidth, setTimelineWidth] = useState(600);

  // Update timeline width on resize using ResizeObserver
  useEffect(() => {
    const updateWidth = () => {
      if (timelineRef.current) {
        // Timeline width = container width - heading width - padding
        const containerWidth = timelineRef.current.offsetWidth;
        const padding = 16; // pr-4 padding on timeline section
        setTimelineWidth(
          Math.max(containerWidth - HEADING_WIDTH - padding, 100),
        );
      }
    };

    // Initial measurement
    updateWidth();

    // Use ResizeObserver for more accurate resize tracking
    const resizeObserver = new ResizeObserver(() => {
      updateWidth();
    });

    if (timelineRef.current) {
      resizeObserver.observe(timelineRef.current);
    }

    // Fallback to window resize for older browsers
    window.addEventListener("resize", updateWidth);

    return () => {
      resizeObserver.disconnect();
      window.removeEventListener("resize", updateWidth);
    };
  }, []);

  // Toggle collapse state for a specific span
  const toggleSpanCollapse = (traceId: string, spanId: string) => {
    setCollapsedSpans((prev) => {
      const newMap = new Map(prev);
      const traceCollapsed = newMap.get(traceId) || new Set();
      const newTraceCollapsed = new Set(traceCollapsed);

      if (newTraceCollapsed.has(spanId)) {
        newTraceCollapsed.delete(spanId);
      } else {
        newTraceCollapsed.add(spanId);
      }

      newMap.set(traceId, newTraceCollapsed);
      return newMap;
    });
  };

  // Check if a span is collapsed
  const isSpanCollapsed = (traceId: string, spanId: string): boolean => {
    return collapsedSpans.get(traceId)?.has(spanId) ?? false;
  };

  // Handle escape key to deselect
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" && selectedSpan) {
        onSpanClick?.(null);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedSpan, onSpanClick]);

  // Build a reverse link index: spanId -> Set of spanIds that link to it
  const reverseLinkIndex = useMemo(() => {
    const index = new Map<string, Set<string>>();
    for (const trace of traces) {
      for (const span of trace.allSpans) {
        if (span.links) {
          for (const link of span.links) {
            if (!index.has(link.span_id)) {
              index.set(link.span_id, new Set());
            }
            index.get(link.span_id)!.add(span.id);
          }
        }
      }
    }
    return index;
  }, [traces]);

  // Calculate related spans for the selected span (forward and reverse links)
  const linkedSpanIds = useMemo(() => {
    if (!selectedSpan) return new Set<string>();
    const ids = new Set<string>();

    // Add forward links (spans that the selected span links to)
    if (selectedSpan.links) {
      for (const link of selectedSpan.links) {
        ids.add(link.span_id);
      }
    }

    // Add reverse links (spans that link to the selected span)
    const reverseLinks = reverseLinkIndex.get(selectedSpan.id);
    if (reverseLinks) {
      for (const spanId of reverseLinks) {
        ids.add(spanId);
      }
    }

    return ids;
  }, [selectedSpan, reverseLinkIndex]);

  // Calculate timeline bounds across all traces
  const timelineBounds = useMemo(() => {
    return calculateTimelineBounds(traces);
  }, [traces]);

  // Create time to pixel converter
  const timeToPixel = useMemo(() => {
    return createTimeToPixel(timelineBounds, timelineWidth);
  }, [timelineBounds, timelineWidth]);

  return (
    <div className="flex h-full flex-col" onClick={() => onSpanClick?.(null)}>
      {traces.length === 0 ? (
        // Placeholder state
        <div className="flex h-full items-center justify-center p-6">
          <p className="text-sm text-slate-500 dark:text-slate-400">
            No traces to display
          </p>
        </div>
      ) : (
        <div className="flex-1 overflow-auto" ref={timelineRef}>
          {traces.map((trace) => (
            <div
              key={trace.id}
              className="border-b-2 border-slate-300 dark:border-slate-700"
            >
              {/* Lane header */}
              <div className="sticky top-0 z-20 border-b border-slate-200 bg-slate-100 px-4 py-2 dark:border-slate-800 dark:bg-slate-900">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-sm font-semibold text-slate-900 dark:text-slate-50">
                      {trace.id}
                    </span>
                  </div>
                  <div className="flex items-center gap-4 text-xs text-slate-500 dark:text-slate-400">
                    <span>
                      {formatDuration(trace.endTime - trace.startTime)}
                    </span>
                  </div>
                </div>
              </div>

              {/* Span tree */}
              <div className="bg-white dark:bg-slate-950">
                {trace.rootSpans.map((span) => (
                  <SpanRow
                    key={span.id}
                    span={span}
                    traceId={trace.id}
                    depth={0}
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
          ))}
        </div>
      )}
    </div>
  );
}
