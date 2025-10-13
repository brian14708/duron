import { FileText } from "lucide-react";
import { useCallback, useState } from "react";

import { FileUpload } from "@/components/file-upload";
import { ModeToggle } from "@/components/mode-toggle";
import { TraceView } from "@/components/trace-view";
import { type TraceFile, parseTraceLog } from "@/lib/trace";

function App() {
  const [file, setFile] = useState<TraceFile | null>(null);
  const [isDragging, setIsDragging] = useState(false);

  const handleFileLoaded = useCallback((loadedFile: TraceFile) => {
    setFile(loadedFile);
    setIsDragging(false);
  }, []);

  const handleClearFile = useCallback(() => {
    setFile(null);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setIsDragging(false);

      const droppedFile = e.dataTransfer.files[0];
      if (!droppedFile) return;

      // Only handle JSONL files
      if (!droppedFile.name.endsWith(".jsonl")) {
        console.error("Only .jsonl files are supported");
        return;
      }

      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const content = e.target?.result as string;
          const traceFile = parseTraceLog(droppedFile.name, content);
          handleFileLoaded(traceFile);
        } catch (err) {
          console.error("Failed to parse trace file:", err);
        }
      };
      reader.onerror = () => {
        console.error("Failed to read file");
      };
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
    // Only set dragging to false if we're leaving the main container
    if (e.currentTarget === e.target) {
      setIsDragging(false);
    }
  }, []);

  return (
    <div
      className="relative flex h-screen flex-col bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-950 dark:to-slate-900"
      onDrop={handleDrop}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
    >
      {/* Drag Overlay */}
      {isDragging && (
        <div className="pointer-events-none absolute inset-0 z-50 flex items-center justify-center bg-blue-500/10 backdrop-blur-sm">
          <div className="rounded-lg border-2 border-dashed border-blue-500 bg-white/90 px-12 py-8 text-center shadow-lg dark:bg-slate-900/90">
            <FileText className="mx-auto mb-4 h-16 w-16 text-blue-500" />
            <p className="text-xl font-semibold text-slate-900 dark:text-slate-50">
              Drop trace file to load
            </p>
            <p className="mt-2 text-sm text-slate-600 dark:text-slate-400">
              .jsonl files only
            </p>
          </div>
        </div>
      )}

      {/* Header */}
      <div className="flex-shrink-0 border-b border-slate-200 dark:border-slate-800">
        <div className="px-4 py-4">
          <div className="flex min-h-[40px] items-center justify-between gap-4">
            <div className="flex items-center gap-4">
              <h1
                className={`text-xl font-semibold text-slate-900 dark:text-slate-50 ${
                  file ? "cursor-pointer" : ""
                }`}
                onClick={file ? handleClearFile : undefined}
              >
                🌀 Duron
              </h1>
              {file && (
                <>
                  <div className="h-6 w-px bg-slate-300 dark:bg-slate-700" />
                  <div className="flex items-center gap-2">
                    <FileText className="h-4 w-4 text-blue-500" />
                    <div className="flex items-center gap-2 text-sm">
                      <span className="font-medium text-slate-900 dark:text-slate-50">
                        {file.name}
                      </span>
                      <span className="text-slate-500 dark:text-slate-400">
                        {file.entries.length} entries
                      </span>
                    </div>
                  </div>
                </>
              )}
            </div>
            <div className="flex items-center gap-2">
              <ModeToggle />
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      {!file ? (
        <div className="flex flex-1 items-center justify-center overflow-auto p-4">
          <div className="w-full max-w-2xl space-y-6">
            {/* File Upload */}
            <FileUpload onFileLoaded={handleFileLoaded} />
          </div>
        </div>
      ) : (
        <TraceView file={file} />
      )}
    </div>
  );
}

export default App;
