# Contributing

Thanks for your interest in contributing.

## Development Setup

1. Create and activate Python 3.11 virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Configure `.env` from `.env.example`.
4. Run checks:
   - `pytest -q`
   - `python -m compileall api.py app.py db graph rag retrieval`

## Pull Request Guidelines

1. Keep PRs focused and small.
2. Add/adjust tests for behavior changes.
3. Do not commit secrets or local `.env`.
4. Include a short change summary and verification steps.
