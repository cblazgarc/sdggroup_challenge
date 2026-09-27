r"""
Resolves metadata.json's pseudo-absolute paths (e.g. "/data/demo/input/...")
against the project root instead of the OS filesystem root.

`metadata.json`'s `config.path` values (and the engine's own
`--tables-base-path` default) are written as `/data/demo/...`, meaning "the
project's own data/demo folder" — not literally the filesystem root. That
distinction is invisible on Linux/macOS (where it happens to also be a
valid, if unintended, absolute path) but breaks on Windows, where a leading
`/` resolves against the current drive's root (`C:\data\...`), which is
never where the project's data actually lives.

`resolve_project_path` makes this explicit and portable: any path starting
with `/` or `\` is treated as project-relative and rebased under
`PROJECT_ROOT` (the directory containing `main.py`); anything else (a
genuine Windows drive-letter path, or an already-relative path) is left
untouched, so an explicitly absolute `--tables-base-path` still works as
given.
"""
from __future__ import annotations

from pathlib import Path

# engine/paths.py -> engine/ -> project root (the directory containing main.py).
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def resolve_project_path(raw_path: str) -> str:
    r"""
    Rebase a metadata.json-style path under the project root.

    "/data/demo/input/poblacion2024.csv" -> "<PROJECT_ROOT>/data/demo/input/poblacion2024.csv"

    A path that does NOT start with "/" or "\" (a Windows drive-letter path
    like "C:\custom\tables", or an already-relative path) is returned
    unchanged. Any backslash in the project-relative part is normalized to
    a forward slash before rebasing, so segments split correctly regardless
    of the OS this runs on (POSIX `pathlib` does not treat "\" as a
    separator, so an unnormalized "\" would otherwise become part of a
    single path component instead of a directory boundary).
    """
    if raw_path.startswith("/") or raw_path.startswith("\\"):
        relative_part = raw_path.lstrip("/\\").replace("\\", "/")
        return str(PROJECT_ROOT / relative_part)
    return raw_path
