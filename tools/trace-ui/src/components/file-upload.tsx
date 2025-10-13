import { AlertCircle, Loader2, Upload } from "lucide-react";
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
          const file = parseTraceLog(selectedFile.name, content);

          onFileLoaded(file);
        } catch (err) {
          setError(err instanceof Error ? err.message : "Failed to parse file");
        }
      };
      reader.onerror = () => {
        setError("Failed to read file");
      };
      reader.readAsText(selectedFile);
    },
    [onFileLoaded],
  );

  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const selectedFile = e.target.files?.[0];
      if (selectedFile) {
        handleFile(selectedFile);
      }
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
    <Card className="mx-auto max-w-2xl">
      <CardHeader>
        <CardTitle>Upload Trace File</CardTitle>
        <CardDescription>
          Select or drag and drop a .jsonl trace file to visualize
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="cursor-pointer rounded-lg border-2 border-dashed border-slate-300 p-12 text-center transition-colors hover:border-slate-400 dark:border-slate-700 dark:hover:border-slate-600">
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

        <div className="mt-4 flex justify-center">
          <Button
            variant="outline"
            onClick={() => void handleLoadSample()}
            disabled={isLoadingUrl}
            className="flex items-center gap-2"
          >
            {isLoadingUrl && <Loader2 className="h-4 w-4 animate-spin" />}
            Try with sample data
          </Button>
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
              className="bg-card flex-1 rounded-md border px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-2 focus:ring-blue-200 focus:outline-none dark:focus:border-blue-400 dark:focus:ring-blue-800"
            />
            <Button
              onClick={() => void handleLoadFromUrl()}
              disabled={isLoadingUrl || !urlInput.trim()}
              className="flex items-center justify-center gap-2"
            >
              {isLoadingUrl && <Loader2 className="h-4 w-4 animate-spin" />}
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
  );
}
