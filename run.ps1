$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path

# Prefer a project-local virtualenv; fall back to the active Python.
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $Python = "python"
}

New-Item -ItemType Directory -Force (Join-Path $Root "data"), (Join-Path $Root "logs") | Out-Null

$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"

& $Python -u (Join-Path $Root "main.py") --config (Join-Path $Root "config.yaml") @args
exit $LASTEXITCODE
