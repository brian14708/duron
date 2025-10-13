export function parseTraceLog(content: string): unknown[] {
  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  return lines.map((line, index) => {
    try {
      return JSON.parse(line);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown error parsing line";
      throw new Error(`Invalid JSONL format at line ${index + 1}: ${message}`);
    }
  });
}
