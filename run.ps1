$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
New-Item -ItemType Directory -Force (Join-Path $Root "data"), (Join-Path $Root "logs") | Out-Null

$env:PYTHONPATH = (Join-Path $Root "runtime")
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"

python -u (Join-Path $Root "main.py") --config (Join-Path $Root "config.yaml") @args
