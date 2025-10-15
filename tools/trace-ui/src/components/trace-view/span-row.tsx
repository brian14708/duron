import { ChevronDown, ChevronRight, Link } from "lucide-react";

import type { Span } from "@/lib/trace";

import { formatDuration, getSpanColor, getSpanIcon } from "./span-utils";
import { HEADING_WIDTH } from "./timeline-utils";

interface SpanRowProps {
  span: Span;
  traceId: string;
  depth: number;
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
  selectedSpan,
  linkedSpanIds,
  onSpanClick,
  timeToPixel,
  isSpanCollapsed,
  toggleSpanCollapse,
}: SpanRowProps) {
  const duration = span.endTime - span.startTime;
  const spanColor = getSpanColor(span);
  const spanIcon = getSpanIcon(span);
  const isSelected = selectedSpan?.id === span.id;
  const hasChildren = span.children && span.children.length > 0;
  const hasLinks = span.links && span.links.length > 0;
  const collapsed = isSpanCollapsed(traceId, span.id);

  // Check if this span is related to the selected span
  const isLinkedToSelected = linkedSpanIds.has(span.id);
  const isRelated = isSelected || isLinkedToSelected;

  // Gray out unrelated spans when something is selected
  const shouldGrayOut = selectedSpan && !isRelated;

  // Calculate timeline bar position and width
  const leftPosition = timeToPixel(span.startTime);
  const width = Math.max(timeToPixel(span.endTime) - leftPosition, 2);

  return (
    <>
      <div
        className={`flex cursor-pointer items-center border-b border-l-2 border-slate-100 hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-900 ${
          isSelected
            ? "border-l-blue-500 bg-blue-50 dark:bg-blue-950"
            : "border-l-transparent"
        } ${shouldGrayOut ? "opacity-40" : ""}`}
        onClick={(e) => {
          e.stopPropagation();
          onSpanClick?.(isSelected ? null : span);
        }}
      >
        {/* Left side: Tree structure with span info */}
        <div
          className="flex min-w-0 flex-shrink-0 items-center gap-1 px-3 py-1.5"
          style={{ width: HEADING_WIDTH, paddingLeft: `${12 + depth * 20}px` }}
        >
          {/* Collapse/expand button */}
          {hasChildren ? (
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                toggleSpanCollapse(traceId, span.id);
              }}
              className="flex-shrink-0 rounded p-0.5 hover:bg-slate-200 dark:hover:bg-slate-700"
            >
              {collapsed ? (
                <ChevronRight className="h-3 w-3 text-slate-600 dark:text-slate-400" />
              ) : (
                <ChevronDown className="h-3 w-3 text-slate-600 dark:text-slate-400" />
              )}
            </button>
          ) : (
            <div className="w-4 flex-shrink-0" />
          )}

          {/* Icon */}
          <span className="flex-shrink-0 text-sm">{spanIcon}</span>

          {/* Span name */}
          <span className="truncate text-sm font-medium text-slate-900 dark:text-slate-50">
            {span.name}
          </span>

          {/* Links indicator */}
          {hasLinks && <Link className="h-3 w-3 flex-shrink-0 text-blue-500" />}

          {/* Duration */}
          <span className="ml-2 flex-shrink-0 text-xs text-slate-500 dark:text-slate-400">
            {formatDuration(duration)}
          </span>
        </div>

        {/* Right side: Timeline bar */}
        <div className="relative flex-1 pr-4">
          <div className="relative h-6 overflow-hidden rounded-sm bg-slate-100 dark:bg-slate-800">
            {/* Timeline bar */}
            <div
              className={`absolute top-0 h-full ${spanColor} ${
                isLinkedToSelected ? "opacity-80" : ""
              }`}
              style={{
                left: `${leftPosition}px`,
                width: `${width}px`,
              }}
              title={
                span.incomplete ? "Incomplete span (no end event)" : undefined
              }
            />

            {/* Instant events as vertical lines */}
            {span.events?.map((event, eventIndex) => {
              const eventLeftPosition = timeToPixel(event.time);
              return (
                <div
                  key={`${span.id}-event-${eventIndex}`}
                  className="group absolute top-0 h-full"
                  style={{
                    left: `${eventLeftPosition}px`,
                    transform: "translateX(-50%)",
                    width: "2px",
                  }}
                  title={event.name}
                >
                  <div className="h-full w-full bg-blue-300 dark:bg-blue-300" />
                  {/* Tooltip on hover */}
                  <div className="pointer-events-none absolute top-full left-1/2 z-50 mt-1 -translate-x-1/2 rounded bg-slate-900 px-2 py-1 text-xs whitespace-nowrap text-white opacity-0 shadow-lg transition-opacity group-hover:opacity-100 dark:bg-slate-100 dark:text-slate-900">
                    {event.name}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Render children if not collapsed */}
      {hasChildren &&
        !collapsed &&
        span.children!.map((child) => (
          <SpanRow
            key={child.id}
            span={child}
            traceId={traceId}
            depth={depth + 1}
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
