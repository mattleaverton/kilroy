# Browser Smoke Demo

Minimal end-to-end browser verification smoke test using OpenAI CLI + `gpt-5.4-spark`.

## Validate

```bash
./kilroy validate --graph demo/browser-smoke/browser-smoke.dot
```

## Run

```bash
./kilroy run --skip-cli-headless-warning --graph demo/browser-smoke/browser-smoke.dot --config demo/browser-smoke/run.yaml
```

## Expected Signals

- `progress.ndjson` includes `tool_browser_artifacts`
- `verify_browser/browser_artifacts/` contains `frontend/playwright-report/index.html` and `frontend/test-results/result.txt`
- `verify_browser/attempt_1/browser_artifacts/` exists after retry
