import {
  CheckCircle2,
  Clock,
  Hash,
  Layers,
  Link as LinkIcon,
  Tag,
  X,
  XCircle,
  Zap,
} from "lucide-react";

import type { Span, Trace } from "@/lib/trace";

interface DetailPanelProps {
  selectedSpan?: Span | null;
  allTraces?: Trace[];
  onClose?: () => void;
}

// Format duration for display
const formatDuration = (duration: number): string => {
  if (duration < 0.001) return `${(duration * 1_000_000).toFixed(0)}µs`;
  if (duration < 1) return `${(duration * 1000).toFixed(2)}ms`;
  return `${duration.toFixed(2)}s`;
};

// Format timestamp for display
const formatTime = (time: number): string => {
  return `${time.toFixed(6)}s`;
};

// Get type badge color
const getTypeBadgeColor = (type: string): string => {
  switch (type) {
    case "workflow":
      return "bg-purple-100 text-purple-800 border-purple-300 dark:bg-purple-900 dark:text-purple-200 dark:border-purple-700";
    case "http":
      return "bg-blue-100 text-blue-800 border-blue-300 dark:bg-blue-900 dark:text-blue-200 dark:border-blue-700";
    case "database":
      return "bg-green-100 text-green-800 border-green-300 dark:bg-green-900 dark:text-green-200 dark:border-green-700";
    case "ml":
      return "bg-pink-100 text-pink-800 border-pink-300 dark:bg-pink-900 dark:text-pink-200 dark:border-pink-700";
    case "cache":
      return "bg-orange-100 text-orange-800 border-orange-300 dark:bg-orange-900 dark:text-orange-200 dark:border-orange-700";
    case "notification":
      return "bg-yellow-100 text-yellow-800 border-yellow-300 dark:bg-yellow-900 dark:text-yellow-200 dark:border-yellow-700";
    default:
      return "bg-slate-100 text-slate-800 border-slate-300 dark:bg-slate-800 dark:text-slate-200 dark:border-slate-700";
  }
};

export function DetailPanel({
  selectedSpan,
  allTraces = [],
  onClose,
}: DetailPanelProps) {
  if (!selectedSpan) {
    return (
      <div className="flex h-full flex-col">
        {/* Header */}
        <div className="border-b border-slate-200 px-6 py-4 dark:border-slate-800">
          <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-50">
            Details
          </h2>
          <p className="text-sm text-slate-500 dark:text-slate-400">
            Span information and relationships
          </p>
        </div>
        {/* Empty state */}
        <div className="flex flex-1 items-center justify-center p-6">
          <div className="text-center">
            <Layers className="mx-auto mb-3 h-12 w-12 text-slate-300 dark:text-slate-700" />
            <p className="text-sm font-medium text-slate-600 dark:text-slate-400">
              No span selected
            </p>
            <p className="mt-1 text-xs text-slate-500 dark:text-slate-500">
              Click on a span in the timeline to view details
            </p>
          </div>
        </div>
      </div>
    );
  }

  const duration = selectedSpan.endTime - selectedSpan.startTime;
  const type = selectedSpan.attributes?.type as string | undefined;

  // Filter out internal fields for attributes display
  const displayAttributes = Object.entries(
    selectedSpan.attributes || {},
  ).filter(([key]) => key !== "type" && key !== "name");

  // Find the current trace
  const currentTrace = allTraces.find((t) => t.id === selectedSpan.traceId);

  // Get linked traces information
  const linkedTracesInfo =
    selectedSpan.links?.map((link) => {
      const linkedTrace = allTraces.find((t) => t.id === link.trace_id);
      return {
        spanId: link.span_id,
        traceId: link.trace_id,
        traceName: linkedTrace?.rootSpans[0]?.name,
      };
    }) || [];

  return (
    <div className="flex h-full flex-col">
      {/* Header with close button for mobile */}
      <div className="border-b border-slate-200 px-6 py-4 dark:border-slate-800">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-50">
              Details
            </h2>
            <p className="text-sm text-slate-500 dark:text-slate-400">
              Span information and relationships
            </p>
          </div>
          {/* Close button for mobile - only visible on small screens */}
          {onClose && (
            <button
              onClick={onClose}
              className="flex-shrink-0 rounded-lg p-2 hover:bg-slate-100 lg:hidden dark:hover:bg-slate-800"
              aria-label="Close details"
            >
              <X className="h-5 w-5 text-slate-600 dark:text-slate-400" />
            </button>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-auto">
        <div className="space-y-6 p-6">
          {/* Header with span name and type */}
          <div>
            <div className="mb-2 flex items-start justify-between gap-2">
              <h3 className="text-lg font-semibold break-words text-slate-900 dark:text-slate-50">
                {selectedSpan.name}
              </h3>
              <div className="flex flex-wrap gap-2">
                {selectedSpan.status && (
                  <span
                    className={`flex items-center gap-1 rounded border px-2 py-1 text-xs font-medium ${
                      selectedSpan.status === "ERROR"
                        ? "border-red-300 bg-red-50 text-red-800 dark:border-red-700 dark:bg-red-950 dark:text-red-200"
                        : "border-green-300 bg-green-50 text-green-800 dark:border-green-700 dark:bg-green-950 dark:text-green-200"
                    }`}
                  >
                    {selectedSpan.status === "ERROR" ? (
                      <XCircle className="h-3 w-3" />
                    ) : (
                      <CheckCircle2 className="h-3 w-3" />
                    )}
                    {selectedSpan.status}
                  </span>
                )}
                {selectedSpan.incomplete && (
                  <span className="rounded border border-red-300 bg-red-50 px-2 py-1 text-xs font-medium text-red-800 dark:border-red-700 dark:bg-red-950 dark:text-red-200">
                    Incomplete
                  </span>
                )}
                {type && (
                  <span
                    className={`rounded border px-2 py-1 text-xs font-medium ${getTypeBadgeColor(type)}`}
                  >
                    {type}
                  </span>
                )}
              </div>
            </div>
            <p className="font-mono text-xs text-slate-500 dark:text-slate-400">
              {selectedSpan.id}
            </p>
            {selectedSpan.incomplete && (
              <p className="mt-2 text-xs text-red-600 dark:text-red-400">
                ⚠️ This span only has a start event. The end time is estimated
                from the trace end.
              </p>
            )}
            {selectedSpan.status === "ERROR" && selectedSpan.statusMessage && (
              <p className="mt-2 text-xs text-red-600 dark:text-red-400">
                ❌ Error: {selectedSpan.statusMessage}
              </p>
            )}
          </div>

          {/* Timing Information */}
          <div className="overflow-hidden rounded-lg border border-slate-200 dark:border-slate-800">
            <div className="border-b border-slate-200 bg-slate-50 px-3 py-2 dark:border-slate-800 dark:bg-slate-900">
              <div className="flex items-center gap-2">
                <Clock className="h-4 w-4 text-slate-500 dark:text-slate-400" />
                <h4 className="text-sm font-semibold text-slate-900 dark:text-slate-50">
                  Timing
                </h4>
              </div>
            </div>
            <div className="space-y-2 p-3">
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  Duration
                </span>
                <span className="text-sm font-semibold text-slate-900 dark:text-slate-50">
                  {formatDuration(duration)}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  Start Time
                </span>
                <span className="font-mono text-xs text-slate-700 dark:text-slate-300">
                  {formatTime(selectedSpan.startTime)}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  End Time
                </span>
                <span className="font-mono text-xs text-slate-700 dark:text-slate-300">
                  {formatTime(selectedSpan.endTime)}
                </span>
              </div>
            </div>
          </div>

          {/* Hierarchy Information */}
          <div className="overflow-hidden rounded-lg border border-slate-200 dark:border-slate-800">
            <div className="border-b border-slate-200 bg-slate-50 px-3 py-2 dark:border-slate-800 dark:bg-slate-900">
              <div className="flex items-center gap-2">
                <Layers className="h-4 w-4 text-slate-500 dark:text-slate-400" />
                <h4 className="text-sm font-semibold text-slate-900 dark:text-slate-50">
                  Hierarchy
                </h4>
              </div>
            </div>
            <div className="space-y-2 p-3">
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  Lane
                </span>
                <span className="text-sm text-slate-900 dark:text-slate-50">
                  {selectedSpan.lane}
                </span>
              </div>
              {selectedSpan.depth !== undefined && (
                <div className="flex items-center justify-between">
                  <span className="text-xs text-slate-500 dark:text-slate-400">
                    Depth
                  </span>
                  <span className="text-sm text-slate-900 dark:text-slate-50">
                    {selectedSpan.depth}
                  </span>
                </div>
              )}
              {selectedSpan.parentId && (
                <div className="flex items-center justify-between">
                  <span className="text-xs text-slate-500 dark:text-slate-400">
                    Parent ID
                  </span>
                  <span className="font-mono text-xs text-slate-700 dark:text-slate-300">
                    {selectedSpan.parentId}
                  </span>
                </div>
              )}
              {selectedSpan.children && selectedSpan.children.length > 0 && (
                <div className="flex items-center justify-between">
                  <span className="text-xs text-slate-500 dark:text-slate-400">
                    Children
                  </span>
                  <span className="text-sm text-slate-900 dark:text-slate-50">
                    {selectedSpan.children.length}
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* Trace Information */}
          <div className="overflow-hidden rounded-lg border border-slate-200 dark:border-slate-800">
            <div className="border-b border-slate-200 bg-slate-50 px-3 py-2 dark:border-slate-800 dark:bg-slate-900">
              <div className="flex items-center gap-2">
                <Hash className="h-4 w-4 text-slate-500 dark:text-slate-400" />
                <h4 className="text-sm font-semibold text-slate-900 dark:text-slate-50">
                  Trace
                </h4>
              </div>
            </div>
            <div className="space-y-2 p-3">
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500 dark:text-slate-400">
                  Trace ID
                </span>
                <span className="font-mono text-xs text-slate-700 dark:text-slate-300">
                  {selectedSpan.traceId}
                </span>
              </div>
              {currentTrace && (
                <>
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-slate-500 dark:text-slate-400">
                      Total Spans
                    </span>
                    <span className="text-xs text-slate-700 dark:text-slate-300">
                      {currentTrace.allSpans.length}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-slate-500 dark:text-slate-400">
                      Trace Duration
                    </span>
                    <span className="text-xs text-slate-700 dark:text-slate-300">
                      {formatDuration(
                        currentTrace.endTime - currentTrace.startTime,
                      )}
                    </span>
                  </div>
                  {currentTrace.linkedTraces.length > 0 && (
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-slate-500 dark:text-slate-400">
                        Related Traces
                      </span>
                      <span className="text-xs text-blue-600 dark:text-blue-400">
                        {currentTrace.linkedTraces.length}
                      </span>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>

          {/* Links to Other Traces */}
          {linkedTracesInfo.length > 0 && (
            <div className="overflow-hidden rounded-lg border border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-950/20">
              <div className="border-b border-blue-200 bg-blue-100 px-3 py-2 dark:border-blue-800 dark:bg-blue-900/50">
                <div className="flex items-center gap-2">
                  <LinkIcon className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                  <h4 className="text-sm font-semibold text-blue-900 dark:text-blue-100">
                    Links to Other Traces
                  </h4>
                </div>
              </div>
              <div className="space-y-3 p-3">
                {linkedTracesInfo.map((link, index) => (
                  <div
                    key={index}
                    className="rounded border border-blue-200 bg-white p-2 dark:border-blue-800 dark:bg-slate-900"
                  >
                    <div className="space-y-1 text-xs">
                      <div className="flex items-start justify-between">
                        <span className="text-slate-500 dark:text-slate-400">
                          Target Span:
                        </span>
                        <span className="text-right font-mono text-slate-700 dark:text-slate-300">
                          {link.spanId}
                        </span>
                      </div>
                      <div className="flex items-start justify-between">
                        <span className="text-slate-500 dark:text-slate-400">
                          Target Trace:
                        </span>
                        <span className="text-right font-mono text-slate-700 dark:text-slate-300">
                          {link.traceId}
                        </span>
                      </div>
                      {link.traceName && (
                        <div className="flex items-start justify-between">
                          <span className="text-slate-500 dark:text-slate-400">
                            Trace Name:
                          </span>
                          <span className="text-right font-medium text-slate-900 dark:text-slate-50">
                            {link.traceName}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
                <div className="rounded bg-blue-100 p-2 text-xs text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">
                  💡 This span triggers or is triggered by spans in other
                  traces, showing distributed workflow relationships.
                </div>
              </div>
            </div>
          )}

          {/* Instant Events */}
          {selectedSpan.events && selectedSpan.events.length > 0 && (
            <div className="overflow-hidden rounded-lg border border-yellow-200 bg-yellow-50 dark:border-yellow-800 dark:bg-yellow-950/20">
              <div className="border-b border-yellow-200 bg-yellow-100 px-3 py-2 dark:border-yellow-800 dark:bg-yellow-900/50">
                <div className="flex items-center gap-2">
                  <Zap className="h-4 w-4 text-yellow-600 dark:text-yellow-400" />
                  <h4 className="text-sm font-semibold text-yellow-900 dark:text-yellow-100">
                    Instant Events ({selectedSpan.events.length})
                  </h4>
                </div>
              </div>
              <div className="space-y-2 p-3">
                {selectedSpan.events.map((event, index) => (
                  <div
                    key={index}
                    className="rounded border border-yellow-200 bg-white p-2 dark:border-yellow-800 dark:bg-slate-900"
                  >
                    <div className="mb-1 flex items-start justify-between gap-2">
                      <span className="text-sm font-medium text-slate-900 dark:text-slate-50">
                        {event.name}
                      </span>
                      <span className="font-mono text-xs text-slate-600 dark:text-slate-400">
                        {formatTime(event.time)}
                      </span>
                    </div>
                    {event.attributes &&
                      Object.keys(event.attributes).length > 0 && (
                        <div className="mt-2 space-y-1">
                          {Object.entries(event.attributes)
                            .filter(([key]) => key !== "name")
                            .map(([key, value]) => (
                              <div
                                key={key}
                                className="flex items-start justify-between text-xs"
                              >
                                <span className="text-slate-500 dark:text-slate-400">
                                  {key}:
                                </span>
                                <span className="ml-2 text-right font-mono text-slate-700 dark:text-slate-300">
                                  {typeof value === "string"
                                    ? value
                                    : JSON.stringify(value)}
                                </span>
                              </div>
                            ))}
                        </div>
                      )}
                  </div>
                ))}
                <div className="rounded bg-yellow-100 p-2 text-xs text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-300">
                  ⚡ These are point-in-time events that occurred during span
                  execution.
                </div>
              </div>
            </div>
          )}

          {/* Attributes */}
          {displayAttributes.length > 0 && (
            <div className="overflow-hidden rounded-lg border border-slate-200 dark:border-slate-800">
              <div className="border-b border-slate-200 bg-slate-50 px-3 py-2 dark:border-slate-800 dark:bg-slate-900">
                <div className="flex items-center gap-2">
                  <Tag className="h-4 w-4 text-slate-500 dark:text-slate-400" />
                  <h4 className="text-sm font-semibold text-slate-900 dark:text-slate-50">
                    Attributes
                  </h4>
                </div>
              </div>
              <div className="p-3">
                <div className="space-y-3">
                  {displayAttributes.map(([key, value]) => (
                    <div key={key}>
                      <div className="mb-1 text-xs font-medium text-slate-600 dark:text-slate-400">
                        {key}
                      </div>
                      <div className="rounded border border-slate-200 bg-white px-2 py-1.5 font-mono text-xs break-all text-slate-900 dark:border-slate-800 dark:bg-slate-950 dark:text-slate-50">
                        {typeof value === "string"
                          ? value
                          : JSON.stringify(value, null, 2)}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
