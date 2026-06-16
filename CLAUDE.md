# CLAUDE.md

## Python version

This repo targets **Python 3.14** (`requires-python = ">=3.14"`, ruff `target-version = "py314"`).

Unparenthesized multi-exception `except A, B:` is **valid** (PEP 758) and is the **ruff-enforced** style here — `ruff format` strips the parentheses, so `except (A, B):` will fail `ruff format --check` in CI. Do not "fix" or refactor it; it is not a Python-2 syntax error.

## Git

Never push to `main`. Branch + PR.
