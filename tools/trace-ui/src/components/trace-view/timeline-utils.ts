import type { Trace } from "@/lib/trace";

export interface TimelineBounds {
  min: number;
  max: number;
}

export const HEADING_WIDTH = 340; // Width of the heading column in pixels

// Calculate timeline bounds across all traces
export const calculateTimelineBounds = (traces: Trace[]): TimelineBounds => {
  if (traces.length === 0) return { min: 0, max: 10 };

  const min = Math.min(...traces.map((t) => t.startTime));
  const max = Math.max(...traces.map((t) => t.endTime));

  return { min, max };
};

// Create a function to convert time to pixel position
export const createTimeToPixel = (bounds: TimelineBounds, width: number) => {
  return (time: number): number => {
    const { min, max } = bounds;
    const range = max - min;
    if (range === 0) return 0;
    return ((time - min) / range) * width;
  };
};

// Compact time-axis label relative to the timeline origin.
export const formatAxisTime = (seconds: number): string => {
  if (seconds === 0) return "0";
  if (seconds < 1) {
    const ms = seconds * 1000;
    return ms < 10 ? `${ms.toFixed(1)}ms` : `${Math.round(ms)}ms`;
  }
  return `${seconds % 1 === 0 ? seconds : seconds.toFixed(seconds < 10 ? 2 : 1)}s`;
};

export interface AxisTick {
  time: number; // seconds, absolute on the timeline
  x: number; // pixel offset within the timeline track
  label: string;
}

// Round a raw step up to a "nice" 1 / 2 / 2.5 / 5 × 10ⁿ increment.
const niceStep = (raw: number): number => {
  const exp = Math.floor(Math.log10(raw));
  const base = Math.pow(10, exp);
  const frac = raw / base;
  const nice =
    frac <= 1 ? 1 : frac <= 2 ? 2 : frac <= 2.5 ? 2.5 : frac <= 5 ? 5 : 10;
  return nice * base;
};

/**
 * Produce evenly spaced, human-friendly ticks for the time ruler. Aims for one
 * tick roughly every ~110px so labels never crowd.
 */
export const generateTicks = (
  bounds: TimelineBounds,
  width: number,
): AxisTick[] => {
  const range = bounds.max - bounds.min;
  if (range <= 0 || width <= 0) return [];

  const timeToPixel = createTimeToPixel(bounds, width);
  const targetCount = Math.max(2, Math.floor(width / 110));
  const step = niceStep(range / targetCount);

  const ticks: AxisTick[] = [];
  const first = Math.ceil(bounds.min / step) * step;
  for (let t = first; t <= bounds.max + step * 1e-6; t += step) {
    // Guard against floating-point drift landing just below zero.
    const time = Math.abs(t) < step * 1e-6 ? 0 : t;
    ticks.push({
      time,
      x: timeToPixel(time),
      label: formatAxisTime(time - bounds.min),
    });
  }
  return ticks;
};
