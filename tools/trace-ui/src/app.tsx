import { CircleAlert, FileDown, type LucideIcon, Signal } from "lucide-react";
import { useCallback, useMemo, useState } from "react";

import { FileUpload } from "@/components/file-upload";
import { ModeToggle } from "@/components/mode-toggle";
import { TraceView } from "@/components/trace-view";
import { formatDuration } from "@/components/trace-view/span-utils";
import {
  type TraceFile,
  extractSpansFromEntries,
  parseTraceLog,
} from "@/lib/trace";

function useTraceStats(file: TraceFile | null) {
  return useMemo(() => {
    if (!file) return null;
    const spans = extractSpansFromEntries(file.entries);
    if (spans.length === 0) {
      return { spans: 0, entries: file.entries.length, duration: 0, errors: 0 };
    }
    const min = Math.min(...spans.map((s) => s.startTime));
    const max = Math.max(...spans.map((s) => s.endTime));
    return {
      spans: spans.length,
      entries: file.entries.length,
      duration: max - min,
      errors: spans.filter((s) => s.status === "ERROR").length,
    };
  }, [file]);
}

function Stat({
  label,
  value,
  tone = "default",
  icon: Icon,
}: {
  label: string;
  value: string;
  tone?: "default" | "error";
  icon?: LucideIcon;
}) {
  const active = tone === "error" && value !== "0";
  return (
    <div className="flex flex-col leading-none">
      <span className="text-muted-foreground font-mono text-[9px] tracking-[0.18em] uppercase">
        {label}
      </span>
      <span
        className={`mt-1 flex items-center gap-1.5 font-mono text-sm font-semibold tabular-nums ${
          active ? "text-kind-error" : "text-foreground"
        }`}
      >
        {Icon && (
          <Icon
            className={`h-3.5 w-3.5 ${active ? "text-kind-error" : "text-muted-foreground/50"}`}
            aria-hidden
          />
        )}
        {value}
      </span>
    </div>
  );
}

function App() {
  const [file, setFile] = useState<TraceFile | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const stats = useTraceStats(file);

  const handleFileLoaded = useCallback((loadedFile: TraceFile) => {
    setFile(loadedFile);
    setIsDragging(false);
  }, []);

  const handleClearFile = useCallback(() => setFile(null), []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setIsDragging(false);

      const droppedFile = e.dataTransfer.files[0];
      if (!droppedFile) return;
      if (!droppedFile.name.endsWith(".jsonl")) {
        console.error("Only .jsonl files are supported");
        return;
      }

      const reader = new FileReader();
      reader.onload = (ev) => {
        try {
          const content = ev.target?.result as string;
          handleFileLoaded(parseTraceLog(droppedFile.name, content));
        } catch (err) {
          console.error("Failed to parse trace file:", err);
        }
      };
      reader.onerror = () => console.error("Failed to read file");
      reader.readAsText(droppedFile);
    },
    [handleFileLoaded],
  );

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.currentTarget === e.target) setIsDragging(false);
  }, []);

  return (
    <div
      className="instrument-backdrop relative flex h-screen flex-col overflow-hidden"
      onDrop={handleDrop}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
    >
      {/* Drag overlay */}
      {isDragging && (
        <div className="bg-primary/10 pointer-events-none absolute inset-0 z-50 flex items-center justify-center backdrop-blur-sm">
          <div className="border-primary bg-card/90 rounded-xl border-2 border-dashed px-14 py-10 text-center shadow-2xl">
            <FileDown className="text-primary mx-auto mb-4 h-14 w-14" />
            <p className="text-foreground text-lg font-semibold">
              Drop trace to load
            </p>
            <p className="text-muted-foreground mt-1 font-mono text-xs">
              .jsonl signal logs only
            </p>
          </div>
        </div>
      )}

      {/* Instrument top bar */}
      <header className="border-border bg-surface/60 relative z-10 flex-shrink-0 border-b backdrop-blur">
        <div className="flex min-h-[52px] items-center justify-between gap-4 px-4 py-2">
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={file ? handleClearFile : undefined}
              className={`flex items-center gap-2 ${file ? "cursor-pointer" : "cursor-default"}`}
            >
              <span className="text-lg leading-none" aria-hidden>
                🌀
              </span>
              <span className="text-foreground font-mono text-sm font-bold tracking-[0.16em] uppercase">
                Duron
              </span>
            </button>

            {file && (
              <>
                <div className="bg-border h-7 w-px" />
                <div className="flex items-center gap-2">
                  <Signal className="text-primary h-3.5 w-3.5" aria-hidden />
                  <span className="text-foreground font-mono text-xs">
                    {file.name}
                  </span>
                </div>
              </>
            )}
          </div>

          <div className="flex items-center gap-5">
            {stats && (
              <div className="hidden items-center gap-5 sm:flex">
                <Stat label="Spans" value={String(stats.spans)} />
                <Stat label="Events" value={String(stats.entries)} />
                <Stat label="Elapsed" value={formatDuration(stats.duration)} />
                <Stat
                  label="Errors"
                  value={String(stats.errors)}
                  tone="error"
                  icon={CircleAlert}
                />
              </div>
            )}
            <ModeToggle />
          </div>
        </div>
      </header>

      {/* Body */}
      {!file ? (
        <div className="relative z-10 flex flex-1 items-center justify-center overflow-auto p-4">
          <FileUpload onFileLoaded={handleFileLoaded} />
        </div>
      ) : (
        <div className="relative z-10 flex flex-1 overflow-hidden">
          <TraceView file={file} />
        </div>
      )}
    </div>
  );
}

export default App;
