import type { Span } from "@/lib/trace";

// Color mapping for different span types
// Soft pastel palette for visual comfort
export const getSpanColor = (span: Span): string => {
  const type = span.attributes?.type as string | undefined;

  let baseColor: string;
  let gradientFrom: string;

  switch (type) {
    case "workflow":
      baseColor = "bg-violet-300 dark:bg-violet-400";
      gradientFrom = "from-violet-300 dark:from-violet-400";
      break;
    case "http":
      baseColor = "bg-blue-300 dark:bg-blue-400";
      gradientFrom = "from-blue-300 dark:from-blue-400";
      break;
    case "database":
      baseColor = "bg-emerald-300 dark:bg-emerald-400";
      gradientFrom = "from-emerald-300 dark:from-emerald-400";
      break;
    case "ml":
      baseColor = "bg-fuchsia-300 dark:bg-fuchsia-400";
      gradientFrom = "from-fuchsia-300 dark:from-fuchsia-400";
      break;
    case "cache":
      baseColor = "bg-sky-300 dark:bg-sky-400";
      gradientFrom = "from-sky-300 dark:from-sky-400";
      break;
    case "notification":
      baseColor = "bg-amber-300 dark:bg-amber-400";
      gradientFrom = "from-amber-300 dark:from-amber-400";
      break;
    case "log":
      baseColor = "bg-gray-300 dark:bg-gray-400";
      gradientFrom = "from-gray-300 dark:from-gray-400";
      break;
    case "metrics":
      baseColor = "bg-teal-300 dark:bg-teal-400";
      gradientFrom = "from-teal-300 dark:from-teal-400";
      break;
    case "analytics":
      baseColor = "bg-indigo-300 dark:bg-indigo-400";
      gradientFrom = "from-indigo-300 dark:from-indigo-400";
      break;
    case "auth":
      baseColor = "bg-rose-300 dark:bg-rose-400";
      gradientFrom = "from-rose-300 dark:from-rose-400";
      break;
    case "validation":
      baseColor = "bg-cyan-300 dark:bg-cyan-400";
      gradientFrom = "from-cyan-300 dark:from-cyan-400";
      break;
    case "computation":
      baseColor = "bg-purple-300 dark:bg-purple-400";
      gradientFrom = "from-purple-300 dark:from-purple-400";
      break;
    case "event":
      baseColor = "bg-lime-300 dark:bg-lime-400";
      gradientFrom = "from-lime-300 dark:from-lime-400";
      break;
    case "wait":
      baseColor = "bg-orange-300 dark:bg-orange-400";
      gradientFrom = "from-orange-300 dark:from-orange-400";
      break;
    default:
      baseColor = "bg-slate-300 dark:bg-slate-400";
      gradientFrom = "from-slate-300 dark:from-slate-400";
  }

  // Add gradient for incomplete spans
  if (span.incomplete) {
    return `bg-gradient-to-r ${gradientFrom} to-transparent`;
  }

  return baseColor;
};

// Icon component for span types
export const getSpanIcon = (span: Span): string => {
  const type = span.attributes?.type as string | undefined;

  switch (type) {
    case "workflow":
      return "⚙️";
    case "http":
      return "🌐";
    case "database":
      return "💾";
    case "ml":
      return "🤖";
    case "cache":
      return "📦";
    case "notification":
      return "📧";
    case "log":
      return "📝";
    case "analytics":
      return "📊";
    case "auth":
      return "🔒";
    case "validation":
      return "✓";
    case "computation":
      return "🔢";
    case "event":
      return "⚡";
    case "wait":
      return "⏳";
    default:
      return "◆";
  }
};

// Format duration for display
export const formatDuration = (duration: number): string => {
  if (duration < 0.001) return `${(duration * 1_000_000).toFixed(0)}µs`;
  if (duration < 1) return `${(duration * 1000).toFixed(2)}ms`;
  return `${duration.toFixed(2)}s`;
};
