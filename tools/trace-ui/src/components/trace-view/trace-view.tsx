import { useCallback, useMemo, useRef, useState } from "react";

import {
  type Span,
  type TraceFile,
  extractSpansFromEntries,
  organizeSpansIntoTraces,
} from "@/lib/trace";

import { DetailPanel } from "./detail-panel";
import { TraceLanes } from "./trace-lanes";

interface TraceViewProps {
  file: TraceFile;
}

export function TraceView({ file }: TraceViewProps) {
  const [selectedSpan, setSelectedSpan] = useState<Span | null>(null);

  // Process spans from file entries into traces
  const traces = useMemo(() => {
    const extractedSpans = extractSpansFromEntries(file.entries);
    const organizedTraces = organizeSpansIntoTraces(
      extractedSpans,
      file.rootTraceId,
    );

    return organizedTraces;
  }, [file.entries, file.rootTraceId]);

  const [sidebarWidth, setSidebarWidth] = useState(384); // 384px = w-96
  const isResizing = useRef(false);

  const handleSpanClick = (span: Span | null) => {
    setSelectedSpan(span);
  };

  const handleCloseDetail = () => {
    setSelectedSpan(null);
  };

  const startResizing = useCallback(() => {
    isResizing.current = true;
  }, []);

  const stopResizing = useCallback(() => {
    isResizing.current = false;
  }, []);

  const resize = useCallback((e: React.MouseEvent) => {
    if (isResizing.current) {
      const newWidth = window.innerWidth - e.clientX;
      if (newWidth >= 256 && newWidth <= 600) {
        setSidebarWidth(newWidth);
      }
    }
  }, []);

  // Get all traces for passing to detail panel (for showing links)
  const allTraces = useMemo(() => traces, [traces]);

  return (
    <div
      className="relative flex flex-1 flex-col overflow-hidden bg-white lg:flex-row dark:bg-slate-950"
      onMouseMove={resize}
      onMouseUp={stopResizing}
      onMouseLeave={stopResizing}
    >
      {/* Main Panel - Lanes (one per trace) - Hidden on mobile when span is selected */}
      <div
        className={`flex flex-1 flex-col overflow-hidden border-b border-slate-200 lg:border-r lg:border-b-0 dark:border-slate-800 ${
          selectedSpan ? "hidden lg:flex" : ""
        }`}
      >
        <div className="border-b border-slate-200 px-6 py-4 dark:border-slate-800">
          <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-50">
            Trace Timeline
          </h2>
          <p className="text-sm text-slate-500 dark:text-slate-400">
            {traces.length} trace{traces.length !== 1 ? "s" : ""} •{" "}
            {file.entries.length} log entries
          </p>
        </div>
        <div className="flex-1 overflow-hidden">
          <TraceLanes
            traces={traces}
            selectedSpan={selectedSpan}
            onSpanClick={handleSpanClick}
          />
        </div>
      </div>

      {/* Resize Handle - Only on desktop */}
      <div
        className="hidden lg:block lg:w-1 lg:cursor-col-resize lg:bg-slate-200 lg:hover:bg-blue-500 lg:dark:bg-slate-800 lg:dark:hover:bg-blue-600"
        onMouseDown={startResizing}
      />

      {/* Side Panel - Details - Hidden on mobile when nothing is selected, full screen when selected */}
      <div
        className={`flex flex-col overflow-hidden bg-white dark:bg-slate-950 ${
          selectedSpan
            ? "absolute inset-0 z-50 w-full lg:relative lg:z-auto lg:flex-shrink-0"
            : "hidden lg:flex lg:flex-shrink-0"
        }`}
        style={{
          width: window.innerWidth >= 1024 ? `${sidebarWidth}px` : undefined,
          minWidth: window.innerWidth >= 1024 ? `${sidebarWidth}px` : undefined,
        }}
      >
        <DetailPanel
          selectedSpan={selectedSpan}
          allTraces={allTraces}
          onClose={handleCloseDetail}
        />
      </div>
    </div>
  );
}
