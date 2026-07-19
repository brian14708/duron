#!/usr/bin/env bash
set -euo pipefail

uv run pytest -q
uv run ruff check .
uv run mypy
uv run basedpyright
uv run pyrefly check
uv run nox -s docs

if rg -n \
  'CachePolicy|duron\.core|duron\.typing|cache_get|cache_put|cache_delete|duron\.tracing|Tracer|RunContext|RequestContext|\.feed\(|\.serve\(|context_factory|LocalBackend|NoopBackend|SignalPortSender|FailureClassification|executor="thread"' \
  src tests examples docs README.md pyproject.toml noxfile.py mkdocs.yaml; then
  echo "obsolete API references remain" >&2
  exit 1
fi
