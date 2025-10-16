type JSONValue =
  | string
  | number
  | boolean
  | null
  | JSONValue[]
  | { [key: string]: JSONValue };

export interface InstantEvent {
  time: number; // in seconds (relative to first event)
  name: string;
  attributes?: Record<string, JSONValue>;
}

export interface Span {
  id: string;
  name: string;
  startTime: number; // in seconds (relative to first event)
  endTime: number; // in seconds (relative to first event)
  lane: number;
  depth?: number;
  verticalTrack?: number; // For stacking overlapping spans at the same depth
  parentId?: string;
  children?: Span[];
  traceId: string;
  attributes?: Record<string, JSONValue>;
  links?: Array<{ span_id: string; trace_id: string }>;
  events?: InstantEvent[]; // Instant events that occurred within this span
  incomplete?: boolean; // True if span only has start event, no end event
  status?: "OK" | "ERROR";
  statusMessage?: string;
}

// internal representation of a trace event from the log
interface TraceEvent {
  type: "span.start" | "span.end" | "event";
  trace_id: string;
  span_id: string;
  ts: number; // timestamp in microseconds
  parent_span_id?: string;
  name?: string;
  attributes?: Record<string, JSONValue>;
  links?: Array<{ span_id: string; trace_id: string }>;
  kind?: string;
  status?: "OK" | "ERROR";
  status_message?: string;
}

export interface TraceFile {
  name: string;
  entries: TraceEvent[];
  rootTraceId: string | null;
}

export interface Trace {
  id: string;
  lane: number;
  rootSpans: Span[];
  allSpans: Span[];
  startTime: number;
  endTime: number;
  linkedTraces: string[]; // IDs of traces linked to this one
}

interface RawTraceEvent {
  type: "span.start" | "span.end" | "event";
  span_id: string;
  ts: number;
  name?: string;
  parent_span_id?: string;
  attributes?: Record<string, JSONValue>;
  links?: Array<{ span_id: string; trace_id?: string }>;
  kind?: string;
  status?: "OK" | "ERROR";
  status_message?: string;
}

interface RawLog {
  ts: number;
  id: number;
  metadata?: Record<string, JSONValue>;
  type: string;
  [k: string]: JSONValue | undefined;
}

function isRawTraceEvent(value: unknown): value is RawTraceEvent {
  if (typeof value !== "object" || value === null) return false;
  const obj = value as Record<string, unknown>;
  return (
    typeof obj.span_id === "string" &&
    typeof obj.ts === "number" &&
    (obj.type === "span.start" ||
      obj.type === "span.end" ||
      obj.type === "event")
  );
}

function rawEventToTraceEvent(
  event: RawTraceEvent,
  traceId: string,
): TraceEvent {
  return {
    type: event.type,
    trace_id: traceId,
    span_id: event.span_id,
    ts: event.ts,
    parent_span_id: event.parent_span_id,
    name: event.name,
    attributes: event.attributes,
    links: event.links?.map((link) => ({
      span_id: link.span_id,
      trace_id: link.trace_id || traceId,
    })),
    kind: event.kind,
    status: event.status,
    status_message: event.status_message,
  };
}

export function parseTraceLog(filename: string, content: string): TraceFile {
  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  const result: TraceEvent[] = [];
  let rootTraceId: string | null = null;
  for (const line of lines) {
    try {
      const parsed = JSON.parse(line) as RawLog;
      const traceId = parsed.metadata?.["trace.id"];
      if (!traceId || typeof traceId !== "string") {
        continue;
      }
      if (
        parsed.type === "trace" &&
        parsed["events"] &&
        Array.isArray(parsed["events"])
      ) {
        // Ignore trace metadata lines for now
        const events = parsed["events"];
        for (const event of events) {
          if (isRawTraceEvent(event)) {
            result.push(rawEventToTraceEvent(event, traceId));
          }
        }
        continue;
      }

      if (parsed.metadata?.["trace.event"]) {
        rootTraceId = traceId;
        const rawEvent = parsed.metadata["trace.event"];
        if (!isRawTraceEvent(rawEvent)) {
          continue;
        }
        const event = rawEventToTraceEvent(rawEvent, traceId);
        event.attributes = event.attributes || {};
        for (const key of Object.keys(parsed)) {
          if (
            key !== "metadata" &&
            key !== "type" &&
            key !== "ts" &&
            key !== "id"
          ) {
            const value = parsed[key];
            if (value !== undefined) {
              event.attributes[key] = value;
            }
          }
        }
        result.push(event);
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown error parsing line";
      throw new Error(`Invalid JSONL format: ${message}`);
    }
  }

  return {
    name: filename,
    entries: result,
    rootTraceId,
  };
}

export function extractSpansFromEntries(entries: TraceEvent[]): Span[] {
  const spanMap = new Map<
    string,
    {
      id: string;
      name?: string;
      startTime?: number;
      endTime?: number;
      parentId?: string;
      traceId?: string;
      attributes?: Record<string, JSONValue>;
      links?: Array<{ span_id: string; trace_id: string }>;
      events?: Array<{
        time: number;
        name: string;
        attributes?: Record<string, JSONValue>;
      }>;
      status?: "OK" | "ERROR";
      statusMessage?: string;
    }
  >();

  // Find the minimum timestamp to use as the base time
  let minTimestamp = Infinity;

  // Track max timestamp per trace
  const traceMaxTimestamp = new Map<string, number>();

  // First pass: find min timestamp and max timestamp per trace
  for (const event of entries) {
    minTimestamp = Math.min(minTimestamp, event.ts);

    const currentMax = traceMaxTimestamp.get(event.trace_id) ?? -Infinity;
    traceMaxTimestamp.set(event.trace_id, Math.max(currentMax, event.ts));
  }

  // Second pass: collect spans and instant events
  for (const event of entries) {
    const spanId = event.span_id;
    const traceId = event.trace_id;

    if (!spanMap.has(spanId)) {
      spanMap.set(spanId, { id: spanId, traceId, events: [] });
    }

    const span = spanMap.get(spanId)!;

    if (event.type === "span.start") {
      span.startTime = event.ts;
      span.name = event.name ?? "Unknown";
      span.parentId = event.parent_span_id;
      span.attributes = event.attributes;
      span.links = event.links;
    } else if (event.type === "span.end") {
      span.endTime = event.ts;
      // Merge attributes from span.end event
      if (event.attributes) {
        span.attributes = { ...span.attributes, ...event.attributes };
      }
      // Capture status and status_message from span.end
      span.status = event.status;
      span.statusMessage = event.status_message;
    } else if (event.type === "event") {
      // Collect instant events
      const eventName = event.name ?? "event";
      span.events = span.events || [];
      span.events.push({
        time: event.ts,
        name: eventName,
        attributes: event.attributes,
      });
    }
  }

  // Convert timestamps to seconds relative to min timestamp
  const spans: Span[] = [];
  // Constant to add to trace end time for incomplete spans (in microseconds)
  const INCOMPLETE_SPAN_EXTENSION = 1_000_000; // 1 second

  for (const [spanId, spanData] of spanMap) {
    // Skip spans without a start time
    if (spanData.startTime === undefined) {
      continue;
    }

    // Skip spans without trace IDs
    if (!spanData.traceId) {
      continue;
    }

    // Mark as incomplete if no end time, use trace max timestamp + constant as end
    const incomplete = spanData.endTime === undefined;
    const endTime =
      spanData.endTime ??
      (traceMaxTimestamp.get(spanData.traceId) ?? spanData.startTime) +
        INCOMPLETE_SPAN_EXTENSION;

    // Convert instant event timestamps to relative seconds
    const convertedEvents =
      spanData.events?.map((event) => ({
        time: (event.time - minTimestamp) / 1_000_000,
        name: event.name,
        attributes: event.attributes,
      })) || [];

    const span: Span = {
      id: spanId,
      name: spanData.name ?? "Unknown",
      startTime: (spanData.startTime - minTimestamp) / 1_000_000, // Convert to seconds
      endTime: (endTime - minTimestamp) / 1_000_000,
      lane: 0, // Will be assigned later
      parentId: spanData.parentId,
      traceId: spanData.traceId,
      attributes: spanData.attributes,
      links: spanData.links,
      events: convertedEvents.length > 0 ? convertedEvents : undefined,
      status: spanData.status,
      statusMessage: spanData.statusMessage,
    };

    // Add incomplete flag if needed
    if (incomplete) {
      span.incomplete = true;
    }

    spans.push(span);
  }

  return spans;
}

export function assignLanesToSpans(spans: Span[]): Span[] {
  // Sort spans by start time
  const sortedSpans = [...spans].sort((a, b) => a.startTime - b.startTime);

  // Track the end time of the last span in each lane
  const lanes: number[] = [];

  // Assign each span to the first available lane
  for (const span of sortedSpans) {
    let assignedLane = -1;

    // Find the first lane where the span can fit
    for (let i = 0; i < lanes.length; i++) {
      if (lanes[i] <= span.startTime) {
        assignedLane = i;
        break;
      }
    }

    // If no lane is available, create a new one
    if (assignedLane === -1) {
      assignedLane = lanes.length;
      lanes.push(span.endTime);
    } else {
      lanes[assignedLane] = span.endTime;
    }

    span.lane = assignedLane;
  }

  return spans;
}

export function organizeSpanHierarchy(spans: Span[]): Span[] {
  const spanMap = new Map<string, Span>(
    spans.map((s) => [s.id, { ...s, children: [] as Span[] }]),
  );
  const rootSpans: Span[] = [];

  // Build parent-child relationships
  for (const span of spanMap.values()) {
    if (span.parentId && spanMap.has(span.parentId)) {
      const parent = spanMap.get(span.parentId)!;
      if (!parent.children) {
        parent.children = [];
      }
      parent.children.push(span);
    } else {
      rootSpans.push(span);
    }
  }

  return rootSpans;
}

/**
 * Organize spans into traces, where each trace gets its own lane
 * Traces are linked through span links
 * If rootTraceId is provided, it will be assigned lane 0
 */
export function organizeSpansIntoTraces(
  spans: Span[],
  rootTraceId?: string | null,
): Trace[] {
  // Group spans by trace ID
  const traceMap = new Map<string, Span[]>();

  for (const span of spans) {
    if (!traceMap.has(span.traceId)) {
      traceMap.set(span.traceId, []);
    }
    traceMap.get(span.traceId)!.push(span);
  }

  // Create Trace objects
  const traces: Trace[] = [];

  // Sort trace IDs so root trace comes first
  const traceIds = Array.from(traceMap.keys());
  if (rootTraceId && traceIds.includes(rootTraceId)) {
    traceIds.sort((a, b) => {
      if (a === rootTraceId) return -1;
      if (b === rootTraceId) return 1;
      return 0;
    });
  }

  let laneIndex = 0;

  for (const traceId of traceIds) {
    const traceSpans = traceMap.get(traceId)!;
    // Build hierarchy for this trace
    const spanMap = new Map<string, Span>(
      traceSpans.map((s) => [s.id, { ...s, children: [] as Span[] }]),
    );
    const rootSpans: Span[] = [];

    // Build parent-child relationships
    for (const span of spanMap.values()) {
      if (span.parentId && spanMap.has(span.parentId)) {
        const parent = spanMap.get(span.parentId)!;
        if (!parent.children) {
          parent.children = [];
        }
        parent.children.push(span);
      } else {
        rootSpans.push(span);
      }
    }

    // Calculate trace time bounds
    const startTime = Math.min(...traceSpans.map((s) => s.startTime));
    const endTime = Math.max(...traceSpans.map((s) => s.endTime));

    // Collect linked trace IDs
    const linkedTraces = new Set<string>();
    for (const span of traceSpans) {
      if (span.links) {
        for (const link of span.links) {
          if (link.trace_id !== traceId) {
            linkedTraces.add(link.trace_id);
          }
        }
      }
    }

    traces.push({
      id: traceId,
      lane: laneIndex++,
      rootSpans: rootSpans.sort((a, b) => a.startTime - b.startTime),
      allSpans: traceSpans,
      startTime,
      endTime,
      linkedTraces: Array.from(linkedTraces),
    });
  }

  return traces;
}
