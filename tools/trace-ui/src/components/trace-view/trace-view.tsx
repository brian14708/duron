import { Search } from "lucide-react";
import { useCallback, useMemo, useRef, useState } from "react";

import {
  type Span,
  type TraceFile,
  extractSpansFromEntries,
  organizeSpansIntoTraces,
} from "@/lib/trace";

import { DetailPanel } from "./detail-panel";
import { KIND_LEGEND } from "./span-utils";
import { TraceLanes } from "./trace-lanes";

interface TraceViewProps {
  file: TraceFile;
}

export function TraceView({ file }: TraceViewProps) {
  const [selectedSpan, setSelectedSpan] = useState<Span | null>(null);
  const [search, setSearch] = useState("");

  const traces = useMemo(() => {
    const extractedSpans = extractSpansFromEntries(file.entries);
    return organizeSpansIntoTraces(extractedSpans, file.rootTraceId);
  }, [file.entries, file.rootTraceId]);

  const [sidebarWidth, setSidebarWidth] = useState(400);
  const isResizing = useRef(false);

  const handleSpanClick = (span: Span | null) => setSelectedSpan(span);
  const handleCloseDetail = () => setSelectedSpan(null);

  const startResizing = useCallback(() => {
    isResizing.current = true;
  }, []);
  const stopResizing = useCallback(() => {
    isResizing.current = false;
  }, []);
  const resize = useCallback((e: React.MouseEvent) => {
    if (isResizing.current) {
      const containerWidth = e.currentTarget.getBoundingClientRect().width;
      const newWidth = containerWidth - e.clientX;
      if (newWidth >= 280 && newWidth <= containerWidth - 280) {
        setSidebarWidth(newWidth);
      }
    }
  }, []);

  return (
    <div
      className="relative flex flex-1 flex-col overflow-hidden lg:flex-row"
      onMouseMove={resize}
      onMouseUp={stopResizing}
      onMouseLeave={stopResizing}
    >
      {/* Main panel — the waterfall */}
      <div
        className={`border-border flex flex-1 flex-col overflow-hidden border-b lg:border-r lg:border-b-0 ${
          selectedSpan ? "hidden lg:flex" : ""
        }`}
      >
        {/* Instrument strip: title, search, legend */}
        <div className="border-border bg-surface/50 flex flex-shrink-0 flex-wrap items-center justify-between gap-x-6 gap-y-2 border-b px-4 py-2.5">
          <div className="flex items-baseline gap-3">
            <h2 className="text-foreground font-mono text-xs font-semibold tracking-[0.2em] uppercase">
              Timeline
            </h2>
            <span className="text-muted-foreground font-mono text-[11px] tabular-nums">
              {traces.length} trace{traces.length !== 1 ? "s" : ""} ·{" "}
              {file.entries.length} events
            </span>
          </div>

          <div className="flex items-center gap-4">
            <div className="hidden items-center gap-3 xl:flex">
              {KIND_LEGEND.map((k) => (
                <span
                  key={k.key}
                  className="text-muted-foreground flex items-center gap-1.5 font-mono text-[10px] tracking-wide uppercase"
                >
                  <span
                    className="h-2 w-2 rounded-[2px]"
                    style={{ background: `var(--kind-${k.key})` }}
                  />
                  {k.label}
                </span>
              ))}
            </div>

            <div className="relative">
              <Search className="text-muted-foreground pointer-events-none absolute top-1/2 left-2.5 h-3.5 w-3.5 -translate-y-1/2" />
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="filter spans…"
                className="border-input bg-background/60 text-foreground placeholder:text-muted-foreground focus:border-primary focus:ring-primary/30 w-44 rounded-md border py-1.5 pr-2 pl-8 font-mono text-xs focus:ring-2 focus:outline-none"
              />
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-hidden">
          <TraceLanes
            traces={traces}
            selectedSpan={selectedSpan}
            searchQuery={search}
            onSpanClick={handleSpanClick}
          />
        </div>
      </div>

      {/* Resize handle */}
      <div
        className="lg:bg-border lg:hover:bg-primary hidden lg:block lg:w-1 lg:cursor-col-resize lg:transition-colors"
        onMouseDown={startResizing}
      />

      {/* Detail side panel */}
      <div
        className={`bg-background flex flex-col overflow-hidden ${
          selectedSpan
            ? "absolute inset-0 z-50 w-full lg:relative lg:z-auto lg:flex-shrink-0"
            : "hidden lg:flex lg:flex-shrink-0"
        }`}
        style={
          { "--sidebar-width": `${sidebarWidth}px` } as React.CSSProperties
        }
      >
        <DetailPanel
          selectedSpan={selectedSpan}
          allTraces={traces}
          onClose={handleCloseDetail}
        />
      </div>
    </div>
  );
}
