import type { Trace } from "@/lib/trace";

export interface TimelineBounds {
  min: number;
  max: number;
}

export const HEADING_WIDTH = 400; // Width of heading column in pixels

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
