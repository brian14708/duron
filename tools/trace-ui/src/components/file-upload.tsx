import { AlertCircle, Loader2, Radio, Upload } from "lucide-react";
import { useCallback, useState } from "react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { type TraceFile, parseTraceLog } from "@/lib/trace";

interface FileUploadProps {
  onFileLoaded: (file: TraceFile) => void;
}

export function FileUpload({ onFileLoaded }: FileUploadProps) {
  const [error, setError] = useState<string | null>(null);
  const [urlInput, setUrlInput] = useState("");
  const [isLoadingUrl, setIsLoadingUrl] = useState(false);

  const handleFile = useCallback(
    (selectedFile: File) => {
      setError(null);
      if (!selectedFile.name.endsWith(".jsonl")) {
        setError("Please upload a .jsonl file");
        return;
      }
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const content = e.target?.result as string;
          onFileLoaded(parseTraceLog(selectedFile.name, content));
        } catch (err) {
          setError(err instanceof Error ? err.message : "Failed to parse file");
        }
      };
      reader.onerror = () => setError("Failed to read file");
      reader.readAsText(selectedFile);
    },
    [onFileLoaded],
  );

  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const selectedFile = e.target.files?.[0];
      if (selectedFile) handleFile(selectedFile);
    },
    [handleFile],
  );

  const loadFromUrl = useCallback(
    async (url: string) => {
      setError(null);
      let parsedUrl: URL;
      try {
        parsedUrl = new URL(url, window.location.href);
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
        const file = parseTraceLog(
          parsedUrl.pathname.split("/").filter(Boolean).pop() ?? "trace.jsonl",
          content,
        );
        onFileLoaded(file);
      } catch (err) {
        setError(
          err instanceof Error
            ? err.message
            : "Failed to load trace file from URL",
        );
      } finally {
        setIsLoadingUrl(false);
      }
    },
    [onFileLoaded],
  );

  const handleLoadFromUrl = useCallback(async () => {
    const trimmedUrl = urlInput.trim();
    if (!trimmedUrl) {
      setError("Please enter a URL");
      return;
    }
    await loadFromUrl(trimmedUrl);
  }, [urlInput, loadFromUrl]);

  const handleLoadSample = useCallback(async () => {
    await loadFromUrl(import.meta.env.BASE_URL + "/sample.jsonl");
  }, [loadFromUrl]);

  return (
    <div className="w-full max-w-xl">
      {/* Wordmark / intro */}
      <div className="mb-8 text-center">
        <div className="mb-3 text-4xl" aria-hidden>
          🌀
        </div>
        <h1 className="text-foreground font-mono text-xl font-bold tracking-[0.24em] uppercase">
          Duron Trace
        </h1>
        <p className="text-muted-foreground mt-2 font-mono text-xs">
          Load a <span className="text-primary">.jsonl</span> signal log to
          visualize workflow execution
        </p>
      </div>

      {/* Dropzone */}
      <div className="group border-border bg-card/60 relative overflow-hidden rounded-xl border shadow-xl backdrop-blur">
        <div className="relative p-6">
          <input
            type="file"
            accept=".jsonl"
            onChange={handleFileInput}
            className="hidden"
            id="file-input"
          />
          <label
            htmlFor="file-input"
            className="border-border bg-background/40 hover:border-primary hover:bg-primary/[0.04] flex cursor-pointer flex-col items-center rounded-lg border-2 border-dashed px-6 py-12 text-center transition-colors"
          >
            <Upload className="text-primary mb-4 h-11 w-11 transition-transform group-hover:-translate-y-0.5" />
            <p className="text-foreground text-base font-medium">
              Drop your trace file, or click to browse
            </p>
            <p className="text-muted-foreground mt-1 font-mono text-xs">
              supports .jsonl
            </p>
          </label>

          <div className="mt-5 flex justify-center">
            <Button
              variant="outline"
              onClick={() => void handleLoadSample()}
              disabled={isLoadingUrl}
              className="gap-2 font-mono text-xs"
            >
              {isLoadingUrl ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Radio className="h-4 w-4" />
              )}
              Try sample signal
            </Button>
          </div>

          <div className="mt-6">
            <p className="text-muted-foreground mb-2 font-mono text-[10px] tracking-[0.18em] uppercase">
              or load from url
            </p>
            <div className="flex flex-col gap-2 sm:flex-row">
              <input
                type="url"
                value={urlInput}
                onChange={(e) => setUrlInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    if (!isLoadingUrl) void handleLoadFromUrl();
                  }
                }}
                placeholder="https://example.com/trace.jsonl"
                className="border-input bg-background/60 text-foreground placeholder:text-muted-foreground focus:border-primary focus:ring-primary/30 flex-1 rounded-md border px-3 py-2 font-mono text-sm focus:ring-2 focus:outline-none"
              />
              <Button
                onClick={() => void handleLoadFromUrl()}
                disabled={isLoadingUrl || !urlInput.trim()}
                className="justify-center gap-2 font-mono text-xs"
              >
                {isLoadingUrl && <Loader2 className="h-4 w-4 animate-spin" />}
                {isLoadingUrl ? "Loading…" : "Load"}
              </Button>
            </div>
          </div>

          {error && (
            <Alert variant="destructive" className="mt-4">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
        </div>
      </div>
    </div>
  );
}
