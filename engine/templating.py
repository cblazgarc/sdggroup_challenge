"""
Generic ``{{ variable }}`` templating resolution.

`metadata.json` may contain unresolved placeholders in `config` values (the
motivating case: an input's `config.path` containing `{{ year }}`, since the
year is not known until the CLI is launched — see `engine.cli`). This module
resolves any `{{ name }}` placeholder against an execution context built
from the CLI arguments, generically (not hardcoded to `year` alone).
"""
from __future__ import annotations

import re
from typing import Any

_TEMPLATE_PATTERN = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class TemplatingError(Exception):
    """Raised when a `{{ variable }}` placeholder has no value in the execution context."""


def resolve_template(value: str, context: dict[str, Any]) -> str:
    """Replace every `{{ name }}` placeholder in `value` with `str(context[name])`."""

    def _replace(match: "re.Match[str]") -> str:
        variable_name = match.group(1)
        if variable_name not in context:
            raise TemplatingError(
                f"template variable '{{{{ {variable_name} }}}}' has no value in the "
                f"execution context (available: {sorted(context)})"
            )
        return str(context[variable_name])

    return _TEMPLATE_PATTERN.sub(_replace, value)


def build_execution_context(year: int) -> dict[str, Any]:
    """The namespace of variables available to `{{ ... }}` templating, from the CLI args."""
    return {"year": year}
