# Repository Guidelines

## Project Structure & Module Organization
The library source lives in `src/dora_pydantic_arrow`, where the package exposes the top-level entry point defined in `__init__.py`．Place new modules alongside `__init__.py`, grouping related models or helpers into their own files for clarity．Keep generated or temporary assets out of the repository; persist reusable reference material in `docs/`．Add new tests under `tests/`, mirroring the package layout (e.g., `tests/test_serialization.py` for `src/dora_pydantic_arrow/serialization.py`)．

## Build, Test, and Development Commands
Install dependencies with `uv sync`, which resolves the locked environment for Python 3.12．Add new libraries using `uv add <package>` so the resolved versions are tracked．Run the current CLI entry point locally with `uv run python -m dora_pydantic_arrow`, or call the script wrapper via `uv run dora-pydantic-arrow` to mirror the installed behavior．Execute the full test suite with `uv run pytest`; narrow the scope during development by passing a node selector such as `uv run pytest tests/test_serialization.py -k happy_path`．

## Coding Style & Naming Conventions
Target Python 3.12 syntax and type hints throughout the codebase．Use four-space indentation, descriptive snake_case for functions and modules, and UpperCamelCase for classes．Prefer explicit exports in `__all__` when modules expose a focused API surface．Format and lint code with Ruff (`uv add ruff`), then run `uv run ruff check --fix` followed by `uv run ruff format` before opening a review．Document public functions and Pydantic models with concise docstrings describing inputs, outputs, and validation rules．

## Testing Guidelines
Write tests with `pytest`, organizing files as `tests/test_<subject>.py` and test names as `test_<behavior>`．Aim for meaningful coverage of edge cases, especially around schema validation and Arrow integration points．Use fixtures for expensive resources and mark slow or integration tests with `@pytest.mark.slow` so they can be filtered via `uv run pytest -m "not slow"`．When adding new features, include regression tests that fail prior to the change to demonstrate necessity．

## Commit & Pull Request Guidelines
Keep commits focused; follow the imperative mood used in the history (e.g., `Add conversion pipeline`) and limit subject lines to ~50 characters．Provide context in the body when the change is non-trivial, including references to benchmarking or issue IDs．Pull requests should summarize the intent, outline testing performed (commands and outcomes), and call out follow-up work．If the change affects user-facing behavior, include updated documentation snippets or screenshots in the PR description．
