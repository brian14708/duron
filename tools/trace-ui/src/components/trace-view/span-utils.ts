import {
  Circle,
  CircleCheck,
  CircleDashed,
  GitMerge,
  type LucideIcon,
  TriangleAlert,
  Waves,
  Workflow,
  Wrench,
} from "lucide-react";

import type { Span } from "@/lib/trace";

export type SpanKind =
  | "workflow"
  | "promise"
  | "completion"
  | "stream"
  | "barrier"
  | "tool"
  | "error"
  | "default";

export interface KindMeta {
  key: SpanKind;
  label: string;
  icon: LucideIcon;
  /** Base Tailwind color name mapped from the `--color-kind-*` theme tokens. */
  color: `kind-${SpanKind}`;
}

const KIND_META: Record<SpanKind, KindMeta> = {
  workflow: {
    key: "workflow",
    label: "Workflow",
    icon: Workflow,
    color: "kind-workflow",
  },
  promise: {
    key: "promise",
    label: "Promise",
    icon: CircleDashed,
    color: "kind-promise",
  },
  completion: {
    key: "completion",
    label: "Await",
    icon: CircleCheck,
    color: "kind-completion",
  },
  stream: { key: "stream", label: "Stream", icon: Waves, color: "kind-stream" },
  barrier: {
    key: "barrier",
    label: "Barrier",
    icon: GitMerge,
    color: "kind-barrier",
  },
  tool: { key: "tool", label: "Tool", icon: Wrench, color: "kind-tool" },
  error: {
    key: "error",
    label: "Error",
    icon: TriangleAlert,
    color: "kind-error",
  },
  default: {
    key: "default",
    label: "Span",
    icon: Circle,
    color: "kind-default",
  },
};

/**
 * Classify a span into a Duron-native kind from its originating event type and
 * name. Real traces only carry `promise.create`, `stream.create`, `barrier`,
 * etc. — so we lean on those plus a few well-known span names the runtime emits.
 */
export const getSpanKind = (span: Span): SpanKind => {
  if (span.status === "ERROR") return "error";

  const kind = span.kind ?? "";
  const name = span.name ?? "";

  if (kind.startsWith("stream")) return "stream";
  if (kind.startsWith("barrier")) return "barrier";

  if (kind.startsWith("promise")) {
    if (name === "Invoke" || name === "prelude") return "workflow";
    if (name === "call_tool" || name.startsWith("call_")) return "tool";
    if (name === "_completion") return "completion";
    return "promise";
  }

  // Fall back to name heuristics when the kind is absent.
  if (name === "Invoke" || name === "prelude") return "workflow";
  if (name === "_completion") return "completion";
  return "default";
};

export const getSpanKindMeta = (span: Span): KindMeta =>
  KIND_META[getSpanKind(span)];

export const KIND_LEGEND: KindMeta[] = [
  KIND_META.workflow,
  KIND_META.promise,
  KIND_META.completion,
  KIND_META.tool,
  KIND_META.stream,
  KIND_META.barrier,
  KIND_META.error,
];

// Format duration for display.
export const formatDuration = (duration: number): string => {
  if (duration < 0.001) return `${(duration * 1_000_000).toFixed(0)}µs`;
  if (duration < 1) return `${(duration * 1000).toFixed(2)}ms`;
  return `${duration.toFixed(2)}s`;
};

// Strip the `stream:` prefix from stream span names for display.
export const formatSpanName = (name: string): string =>
  name.replace(/^stream:/, "");
