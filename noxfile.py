# type: ignore
# pyright: basic, reportMissingImports=false

from __future__ import annotations

import nox

nox.options.default_venv_backend = "uv"

ALL_PYTHON = ["3.14", "3.13", "3.12", "3.11", "3.10"]


def install_deps(s: nox.Session, groups: list[str]):
    s.env["UV_PROJECT_ENVIRONMENT"] = s.virtualenv.location
    cmd = ["uv", "sync", "--frozen"]
    for g in groups:
        cmd.extend(("--group", g))
    _ = s.run_install(*cmd)


@nox.session(python=ALL_PYTHON)
def test(s: nox.Session) -> None:
    install_deps(s, [])
    _ = s.run("pytest")


@nox.session(python=ALL_PYTHON)
def type_check(s: nox.Session) -> None:
    install_deps(s, ["type-checking", "examples"])
    _ = s.run("mypy", ".")
    _ = s.run("basedpyright", "--venvpath", s.virtualenv.location, ".")


@nox.session
def lint(s: nox.Session) -> None:
    install_deps(s, ["lint"])
    _ = s.run("ruff", "check", ".")
    _ = s.run("ruff", "format", "--check", ".")


@nox.session
def docs(s: nox.Session) -> None:
    install_deps(s, ["docs"])
    _ = s.run("mkdocs", "build")
