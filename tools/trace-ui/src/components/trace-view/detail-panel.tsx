import {
  CheckCircle2,
  Clock,
  Hash,
  Layers,
  Link as LinkIcon,
  type LucideIcon,
  Tag,
  X,
  XCircle,
  Zap,
} from "lucide-react";

import type { Span, Trace } from "@/lib/trace";

import { formatSpanName, getSpanKindMeta } from "./span-utils";

interface DetailPanelProps {
  selectedSpan?: Span | null;
  allTraces?: Trace[];
  onClose?: () => void;
}

const formatDuration = (duration: number): string => {
  if (duration < 0.001) return `${(duration * 1_000_000).toFixed(0)}µs`;
  if (duration < 1) return `${(duration * 1000).toFixed(2)}ms`;
  return `${duration.toFixed(2)}s`;
};

const formatTime = (time: number): string => `${time.toFixed(6)}s`;

const tryParse = (s: string): unknown => {
  try {
    return JSON.parse(s);
  } catch {
    return undefined;
  }
};

// Unwrap values that are JSON-encoded (possibly escaped or nested several
// times) and pretty-print the result. Bare strings are returned unquoted.
const formatValue = (value: unknown): string => {
  let current: unknown = value;
  for (let i = 0; i < 5; i++) {
    if (typeof current !== "string") break;
    const s = current.trim();
    if (!s) break;
    let parsed = tryParse(s);
    // Handle escaped fragments like {\"a\":1} by unescaping one string layer.
    if (parsed === undefined && /\\"/.test(s) && /^[[{]/.test(s)) {
      const unescaped = tryParse(`"${s}"`);
      if (typeof unescaped === "string") {
        parsed = tryParse(unescaped) ?? unescaped;
      }
    }
    if (parsed === undefined || parsed === current) break;
    current = parsed;
  }
  return typeof current === "string"
    ? current
    : JSON.stringify(current, null, 2);
};

// Key label above a wrapped, monospace value block — used for any attribute
// whose value may be long or structured.
function AttrBlock({ label, value }: { label: string; value: unknown }) {
  return (
    <div>
      <div className="text-muted-foreground mb-1 font-mono text-[10px] tracking-wide uppercase">
        {label}
      </div>
      <div className="border-border bg-background/50 text-foreground rounded border px-2 py-1.5 font-mono text-[11px] break-all whitespace-pre-wrap">
        {formatValue(value)}
      </div>
    </div>
  );
}

function Section({
  icon: Icon,
  title,
  accent,
  children,
}: {
  icon: LucideIcon;
  title: string;
  accent?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="border-border bg-card/40 overflow-hidden rounded-lg border">
      <div className="border-border bg-surface/60 flex items-center gap-2 border-b px-3 py-2">
        <Icon
          className="h-3.5 w-3.5"
          style={accent ? { color: accent } : undefined}
          aria-hidden
        />
        <h4 className="text-foreground font-mono text-[10px] font-semibold tracking-[0.16em] uppercase">
          {title}
        </h4>
      </div>
      <div className="p-3">{children}</div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-0.5">
      <span className="text-muted-foreground text-xs">{label}</span>
      <span className="text-foreground text-right font-mono text-xs tabular-nums">
        {value}
      </span>
    </div>
  );
}

function EmptyHeader() {
  return (
    <div className="border-border flex-shrink-0 border-b px-5 py-3">
      <h2 className="text-foreground font-mono text-xs font-semibold tracking-[0.2em] uppercase">
        Inspector
      </h2>
    </div>
  );
}

export function DetailPanel({
  selectedSpan,
  allTraces = [],
  onClose,
}: DetailPanelProps) {
  if (!selectedSpan) {
    return (
      <div className="flex h-full flex-col">
        <EmptyHeader />
        <div className="flex flex-1 items-center justify-center p-6">
          <div className="text-center">
            <Layers className="text-muted-foreground/40 mx-auto mb-3 h-10 w-10" />
            <p className="text-muted-foreground text-sm font-medium">
              No span selected
            </p>
            <p className="text-muted-foreground/70 mt-1 font-mono text-xs">
              select a span · ↑ ↓ to navigate
            </p>
          </div>
        </div>
      </div>
    );
  }

  const duration = selectedSpan.endTime - selectedSpan.startTime;
  const meta = getSpanKindMeta(selectedSpan);
  const kindVar = `var(--kind-${meta.key})`;

  const displayAttributes = Object.entries(
    selectedSpan.attributes || {},
  ).filter(([key]) => key !== "type" && key !== "name");

  const currentTrace = allTraces.find((t) => t.id === selectedSpan.traceId);

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
      <div className="border-border flex flex-shrink-0 items-center justify-between border-b px-5 py-3">
        <h2 className="text-foreground font-mono text-xs font-semibold tracking-[0.2em] uppercase">
          Inspector
        </h2>
        {onClose && (
          <button
            onClick={onClose}
            className="text-muted-foreground hover:bg-secondary hover:text-foreground rounded-md p-1.5 lg:hidden"
            aria-label="Close details"
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>

      <div className="flex-1 overflow-auto">
        <div className="space-y-5 p-5">
          {/* Identity */}
          <div>
            <div className="mb-2 flex items-start justify-between gap-2">
              <div className="flex min-w-0 items-center gap-2">
                <meta.icon
                  className="h-4 w-4 flex-shrink-0"
                  style={{ color: kindVar }}
                  aria-hidden
                />
                <h3 className="text-foreground font-mono text-base font-semibold break-words">
                  {formatSpanName(selectedSpan.name)}
                </h3>
              </div>
              <div className="flex flex-shrink-0 flex-wrap justify-end gap-1.5">
                {selectedSpan.status && (
                  <span
                    className={`flex items-center gap-1 rounded border px-2 py-0.5 font-mono text-[10px] font-medium ${
                      selectedSpan.status === "ERROR"
                        ? "border-kind-error/40 text-kind-error"
                        : "border-kind-stream/40 text-kind-stream"
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
                <span
                  className="rounded border px-2 py-0.5 font-mono text-[10px] font-medium"
                  style={{ borderColor: kindVar, color: kindVar }}
                >
                  {meta.label}
                </span>
              </div>
            </div>
            <p className="text-muted-foreground font-mono text-[11px] break-all">
              {selectedSpan.id}
            </p>
            {selectedSpan.incomplete && (
              <p className="text-kind-barrier mt-2 font-mono text-[11px]">
                ⚠ start-only span — end time estimated from trace end.
              </p>
            )}
            {selectedSpan.status === "ERROR" && selectedSpan.statusMessage && (
              <p className="text-kind-error mt-2 font-mono text-[11px]">
                ✕ {selectedSpan.statusMessage}
              </p>
            )}
          </div>

          <Section icon={Clock} title="Timing">
            <Row label="Duration" value={formatDuration(duration)} />
            <Row label="Start" value={formatTime(selectedSpan.startTime)} />
            <Row label="End" value={formatTime(selectedSpan.endTime)} />
          </Section>

          <Section icon={Layers} title="Hierarchy">
            <Row label="Lane" value={selectedSpan.lane} />
            {selectedSpan.depth !== undefined && (
              <Row label="Depth" value={selectedSpan.depth} />
            )}
            {selectedSpan.parentId && (
              <Row label="Parent" value={selectedSpan.parentId} />
            )}
            {selectedSpan.children && selectedSpan.children.length > 0 && (
              <Row label="Children" value={selectedSpan.children.length} />
            )}
          </Section>

          <Section icon={Hash} title="Trace">
            <Row label="Trace ID" value={selectedSpan.traceId} />
            {currentTrace && (
              <>
                <Row label="Total Spans" value={currentTrace.allSpans.length} />
                <Row
                  label="Trace Duration"
                  value={formatDuration(
                    currentTrace.endTime - currentTrace.startTime,
                  )}
                />
                {currentTrace.linkedTraces.length > 0 && (
                  <Row
                    label="Related Traces"
                    value={
                      <span className="text-primary">
                        {currentTrace.linkedTraces.length}
                      </span>
                    }
                  />
                )}
              </>
            )}
          </Section>

          {linkedTracesInfo.length > 0 && (
            <Section
              icon={LinkIcon}
              title="Cross-trace Links"
              accent="var(--primary)"
            >
              <div className="space-y-2">
                {linkedTracesInfo.map((link, index) => (
                  <div
                    key={index}
                    className="border-primary/25 bg-primary/[0.05] rounded border p-2"
                  >
                    <Row label="Target Span" value={link.spanId} />
                    <Row label="Target Trace" value={link.traceId} />
                    {link.traceName && (
                      <Row label="Trace Name" value={link.traceName} />
                    )}
                  </div>
                ))}
              </div>
            </Section>
          )}

          {selectedSpan.events && selectedSpan.events.length > 0 && (
            <Section
              icon={Zap}
              title={`Instant Events · ${selectedSpan.events.length}`}
              accent="var(--kind-event)"
            >
              <div className="space-y-2">
                {selectedSpan.events.map((event, index) => (
                  <div
                    key={index}
                    className="border-border bg-background/40 rounded border p-2"
                  >
                    <div className="flex items-start justify-between gap-2">
                      <span className="text-foreground font-mono text-xs font-medium">
                        {event.name}
                      </span>
                      <span className="text-muted-foreground font-mono text-[11px] tabular-nums">
                        {formatTime(event.time)}
                      </span>
                    </div>
                    {event.attributes &&
                      Object.keys(event.attributes).length > 0 && (
                        <div className="mt-2 space-y-2">
                          {Object.entries(event.attributes)
                            .filter(([key]) => key !== "name")
                            .map(([key, value]) => (
                              <AttrBlock key={key} label={key} value={value} />
                            ))}
                        </div>
                      )}
                  </div>
                ))}
              </div>
            </Section>
          )}

          {displayAttributes.length > 0 && (
            <Section icon={Tag} title="Attributes">
              <div className="space-y-2.5">
                {displayAttributes.map(([key, value]) => (
                  <AttrBlock key={key} label={key} value={value} />
                ))}
              </div>
            </Section>
          )}
        </div>
      </div>
    </div>
  );
}
