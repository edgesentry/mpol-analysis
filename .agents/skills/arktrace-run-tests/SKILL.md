---
name: arktrace-run-tests
description: Run arktrace tests — pipeline unit/integration tests and frontend Vitest tests. Use before committing or when CI fails.
license: Apache-2.0
compatibility: Requires uv, Node.js, npm
metadata:
  repo: arktrace
---

## Pipeline tests (Python)

```bash
uv run pytest tests/
```

## Frontend tests

```bash
# Unit tests (Vitest)
cd app && npm test

# Static analysis
cd app && npx eslint src/
```

## Operations shell smoke test

```bash
bash scripts/run_operations_shell.sh
```

Covers Full Screening, Review-Feedback Evaluation, Historical Backtesting, and Demo/Smoke.
