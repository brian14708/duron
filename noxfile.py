from __future__ import annotations

import nox

nox.options.default_venv_backend = "uv"

ALL_PYTHON = ["3.15", "3.14", "3.13", "3.12", "3.11"]


def install_deps(s: nox.Session, groups: list[str]) -> None:
    s.env["UV_PROJECT_ENVIRONMENT"] = s.virtualenv.location
    cmd = ["uv", "sync", "--locked"]
    cmd.append("--no-dev")
    for g in groups:
        cmd.extend(("--group", g))
    _ = s.run_install(*cmd)


def run_type_checks(s: nox.Session) -> None:
    executable = s.run("python", "-c", "import sys; print(sys.executable)", silent=True)
    assert isinstance(executable, str)
    executable = executable.strip()
    _ = s.run("mypy", "--python-executable", executable)
    _ = s.run("pyright", "--pythonpath", executable, "--warnings")
    _ = s.run(
        "pyright",
        "--pythonpath",
        executable,
        "--verifytypes",
        "duron",
        "--ignoreexternal",
    )
    _ = s.run("pyrefly", "check", "--python-interpreter-path", executable)


@nox.session(reuse_venv=True, default=False)
def check(s: nox.Session) -> None:
    install_deps(s, ["test", "typing", "lint"])
    _ = s.run("ruff", "format")
    _ = s.run("ruff", "check", "--fix")
    _ = s.run("ruff", "format")
    _ = s.run("pytest", "--lf", "-x", "-n", "2")
    run_type_checks(s)


@nox.session(python=ALL_PYTHON)
def test(s: nox.Session) -> None:
    install_deps(s, ["test"])
    _ = s.run("pytest")


@nox.session()
def type_check(s: nox.Session) -> None:
    install_deps(s, ["typing"])
    run_type_checks(s)


@nox.session
def lint(s: nox.Session) -> None:
    install_deps(s, ["lint"])
    _ = s.run("ruff", "check")
    _ = s.run("ruff", "format", "--check")


@nox.session
def docs(s: nox.Session) -> None:
    install_deps(s, ["docs"])
    _ = s.run("zensical", "build")
