import { AlertCircle, FileText, Loader2, Upload, X } from "lucide-react";
import { useCallback, useState } from "react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { parseTraceLog } from "@/lib/trace";

import { ModeToggle } from "./components/mode-toggle";

interface TraceFile {
  name: string;
  size: number;
  content: string;
  entries: unknown[];
}

function App() {
  const [file, setFile] = useState<TraceFile | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [urlInput, setUrlInput] = useState("");
  const [isLoadingUrl, setIsLoadingUrl] = useState(false);

  const handleFile = useCallback((selectedFile: File) => {
    setError(null);

    if (!selectedFile.name.endsWith(".jsonl")) {
      setError("Please upload a .jsonl file");
      return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        const entries = parseTraceLog(content);

        setFile({
          name: selectedFile.name,
          size: selectedFile.size,
          content,
          entries,
        });
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to parse file");
      }
    };
    reader.onerror = () => {
      setError("Failed to read file");
    };
    reader.readAsText(selectedFile);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setIsDragging(false);

      const droppedFile = e.dataTransfer.files[0];
      if (droppedFile) {
        handleFile(droppedFile);
      }
    },
    [handleFile],
  );

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const selectedFile = e.target.files?.[0];
      if (selectedFile) {
        handleFile(selectedFile);
      }
    },
    [handleFile],
  );

  const clearFile = useCallback(() => {
    setFile(null);
    setError(null);
    setUrlInput("");
  }, []);

  const handleLoadFromUrl = useCallback(async () => {
    setError(null);

    const trimmedUrl = urlInput.trim();
    if (!trimmedUrl) {
      setError("Please enter a URL");
      return;
    }

    let parsedUrl: URL;
    try {
      parsedUrl = new URL(trimmedUrl);
    } catch {
      setError("Please enter a valid URL");
      return;
    }

    if (!parsedUrl.pathname.endsWith(".jsonl")) {
      setError("URL must point to a .jsonl file");
      return;
    }

    setIsLoadingUrl(true);
    try {
      const response = await fetch(parsedUrl.toString());

      if (!response.ok) {
        throw new Error(`Failed to fetch file (status ${response.status})`);
      }

      const content = await response.text();
      const entries = parseTraceLog(content);
      const size = new TextEncoder().encode(content).length;
      const fileName =
        parsedUrl.pathname.split("/").filter(Boolean).pop() ?? "trace.jsonl";

      setFile({
        name: fileName,
        size,
        content,
        entries,
      });
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Failed to load trace file from URL",
      );
    } finally {
      setIsLoadingUrl(false);
    }
  }, [urlInput]);

  const formatFileSize = (bytes: number): string => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-950 dark:to-slate-900">
      <div className="container mx-auto px-4 py-8">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-xl font-semibold text-slate-900 dark:text-slate-50">
            🌀 Duron
          </h1>
          <ModeToggle />
        </div>

        {/* Upload Section */}
        {!file ? (
          <Card className="mx-auto max-w-2xl">
            <CardHeader>
              <CardTitle>Upload Trace File</CardTitle>
              <CardDescription>
                Select or drag and drop a .jsonl trace file to visualize
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div
                onDrop={handleDrop}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                className={`cursor-pointer rounded-lg border-2 border-dashed p-12 text-center transition-colors ${
                  isDragging
                    ? "border-blue-500 bg-blue-50 dark:bg-blue-950/20"
                    : "border-slate-300 hover:border-slate-400 dark:border-slate-700 dark:hover:border-slate-600"
                } `}
              >
                <input
                  type="file"
                  accept=".jsonl"
                  onChange={handleFileInput}
                  className="hidden"
                  id="file-input"
                />
                <label htmlFor="file-input" className="cursor-pointer">
                  <Upload className="mx-auto mb-4 h-12 w-12 text-slate-400" />
                  <p className="mb-2 text-lg font-medium text-slate-700 dark:text-slate-300">
                    Drop your trace file here, or click to browse
                  </p>
                  <p className="text-sm text-slate-500 dark:text-slate-400">
                    Supports .jsonl files
                  </p>
                </label>
              </div>

              <div className="mt-6">
                <p className="text-sm font-medium text-slate-700 dark:text-slate-300">
                  Or load from a URL
                </p>
                <div className="mt-2 flex flex-col gap-2 sm:flex-row">
                  <input
                    type="url"
                    value={urlInput}
                    onChange={(e) => setUrlInput(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        if (!isLoadingUrl) {
                          void handleLoadFromUrl();
                        }
                      }
                    }}
                    placeholder="https://example.com/trace.jsonl"
                    className="flex-1 rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-blue-500 focus:ring-2 focus:ring-blue-200 focus:outline-none dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:focus:border-blue-400 dark:focus:ring-blue-800"
                  />
                  <Button
                    onClick={() => void handleLoadFromUrl()}
                    disabled={isLoadingUrl || !urlInput.trim()}
                    className="flex items-center justify-center gap-2"
                  >
                    {isLoadingUrl && (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    )}
                    {isLoadingUrl ? "Loading..." : "Load"}
                  </Button>
                </div>
              </div>

              {error && (
                <Alert variant="destructive" className="mt-4">
                  <AlertCircle className="h-4 w-4" />
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>
        ) : (
          <>
            {/* File Info Bar */}
            <Card className="mb-6">
              <CardContent className="py-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <FileText className="h-5 w-5 text-blue-500" />
                    <div>
                      <p className="font-medium text-slate-900 dark:text-slate-50">
                        {file.name}
                      </p>
                      <p className="text-sm text-slate-500 dark:text-slate-400">
                        {formatFileSize(file.size)} • {file.entries.length}{" "}
                        entries
                      </p>
                    </div>
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={clearFile}
                    className="gap-2"
                  >
                    <X className="h-4 w-4" />
                    Clear
                  </Button>
                </div>
              </CardContent>
            </Card>

            {/* Trace Visualization Area */}
            <Card>
              <CardHeader>
                <CardTitle>Trace Visualization</CardTitle>
                <CardDescription>
                  Execution timeline and operation details
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="py-12 text-center text-slate-500 dark:text-slate-400">
                  <p className="mb-2">Trace visualization coming soon...</p>
                  <p className="text-sm">
                    Loaded {file.entries.length} log entries
                  </p>
                </div>
              </CardContent>
            </Card>
          </>
        )}
      </div>
    </div>
  );
}

export default App;
