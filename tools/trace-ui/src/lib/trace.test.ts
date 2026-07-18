import { describe, expect, it } from "vitest";

import {
  extractSpansFromEntries,
  organizeSpansIntoTraces,
  parseTraceLog,
} from "./trace";

const entry = (
  traceId: string,
  event: Record<string, unknown>,
  extra: Record<string, unknown> = {},
) =>
  JSON.stringify({
    id: "entry",
    ts: 1,
    type: "promise.create",
    metadata: { "trace.id": traceId, "trace.event": event },
    ...extra,
  });

describe("parseTraceLog", () => {
  it("handles empty files and records without trace metadata", () => {
    expect(parseTraceLog("empty.jsonl", "").entries).toEqual([]);
    expect(
      parseTraceLog("plain.jsonl", '{"id":"x","type":"barrier"}').entries,
    ).toEqual([]);
  });

  it("reports malformed JSON with the line number", () => {
    expect(() => parseTraceLog("bad.jsonl", "{}\nnot-json")).toThrow(
      "Invalid JSONL at line 2",
    );
  });

  it("preserves physical line numbers across blank lines", () => {
    expect(() => parseTraceLog("bad.jsonl", "{}\n\nnot-json")).toThrow(
      "Invalid JSONL at line 3",
    );
  });

  it("parses batched and metadata-attached events", () => {
    const batched = JSON.stringify({
      id: "trace-entry",
      ts: 1,
      type: "trace",
      metadata: { "trace.id": "child" },
      events: [
        {
          type: "span.start",
          span_id: "child-span",
          ts: 20,
          name: "child",
        },
      ],
    });
    const attached = entry(
      "root",
      { type: "span.start", span_id: "root-span", ts: 10, name: "root" },
      { result: 42 },
    );
    const file = parseTraceLog("trace.jsonl", `${batched}\n${attached}`);

    expect(file.rootTraceId).toBe("root");
    expect(file.entries).toHaveLength(2);
    expect(file.entries[1]?.attributes).toMatchObject({ result: 42 });
  });

  it("ignores malformed trace events without discarding valid siblings", () => {
    const content = JSON.stringify({
      id: "trace-entry",
      ts: 1,
      type: "trace",
      metadata: { "trace.id": "trace" },
      events: [
        { type: "span.start", span_id: "valid", ts: 10, name: "valid" },
        { type: "span.start", ts: "invalid" },
      ],
    });
    expect(parseTraceLog("trace.jsonl", content).entries).toHaveLength(1);
  });
});

describe("trace transformation", () => {
  it("normalizes timestamps and marks incomplete spans", () => {
    const file = parseTraceLog(
      "trace.jsonl",
      [
        entry("trace", {
          type: "span.start",
          span_id: "complete",
          ts: 1_000_000,
          name: "complete",
        }),
        entry("trace", {
          type: "span.end",
          span_id: "complete",
          ts: 2_000_000,
          status: "OK",
        }),
        entry("trace", {
          type: "span.start",
          span_id: "incomplete",
          ts: 1_500_000,
          name: "incomplete",
        }),
      ].join("\n"),
    );
    const spans = extractSpansFromEntries(file.entries);

    expect(spans.find((span) => span.id === "complete")).toMatchObject({
      startTime: 0,
      endTime: 1,
      status: "OK",
    });
    expect(spans.find((span) => span.id === "incomplete")?.incomplete).toBe(
      true,
    );
  });

  it("merges duplicate span records and preserves cross-trace links", () => {
    const content = [
      entry("root", {
        type: "span.start",
        span_id: "root-span",
        ts: 10,
        name: "root",
      }),
      entry("root", {
        type: "span.start",
        span_id: "root-span",
        ts: 10,
        name: "root",
        links: [{ span_id: "child-span", trace_id: "child" }],
      }),
      entry("root", {
        type: "span.end",
        span_id: "root-span",
        ts: 30,
        status: "OK",
      }),
    ].join("\n");
    const file = parseTraceLog("trace.jsonl", content);
    const spans = extractSpansFromEntries(file.entries);
    const traces = organizeSpansIntoTraces(spans, file.rootTraceId);

    expect(spans).toHaveLength(1);
    expect(traces[0]?.id).toBe("root");
    expect(traces[0]?.linkedTraces).toEqual(["child"]);
  });
});
