# Dora Pydantic Arrow

Dora Pydantic Arrow bridges [Pydantic](https://docs.pydantic.dev/) models and [Apache Arrow](https://arrow.apache.org/) tables so structured Python data can be stored, transported, and analyzed efficiently. It provides helpers that convert validated models to Arrow buffers and back again without sacrificing type safety.

## Features

- **Zero-copy conversions** between `BaseModel` instances and Arrow record batches.
- **Schema-aware serialization** that respects field aliases, default values, and optional types.
- **Batch processing utilities** for reading and writing large datasets.
- **Typed dataset API** that keeps metadata and data frames synchronized.

## Installation

Install dependencies with [uv](https://github.com/astral-sh/uv):

```bash
uv sync
```

This will create a virtual environment and install the locked dependency set defined in `pyproject.toml` and `uv.lock`.

## Usage

Run the package's CLI entry point:

```bash
uv run python -m dora_pydantic_arrow
```

or call the installed wrapper directly:

```bash
uv run dora-pydantic-arrow
```

For library usage, import helpers from the package:

```python
from dora_pydantic_arrow.dataset import ArrowDataset
from dora_pydantic_arrow.serialization import dumps, loads
```

Refer to the modules under `src/dora_pydantic_arrow/` for additional utilities.

## Testing

Execute the project test suite with:

```bash
uv run pytest
```

Use selectors (for example, `-k`, `-m`, or a path) to run a subset of tests while iterating.

## Contributing

1. Fork and clone the repository.
2. Run `uv sync` to install dependencies.
3. Make your changes following the coding guidelines in `AGENTS.md`.
4. Format and lint with Ruff (`uv run ruff check --fix` and `uv run ruff format`).
5. Add or update tests as appropriate and run `uv run pytest`.
6. Commit using imperative subject lines and open a pull request summarizing the change and test results.
