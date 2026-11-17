param(
    [string]$Python = 'python',
    [string]$Manifest = '',
    [Parameter(Mandatory=$true)][string]$RunId
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
if (-not $Manifest) { $Manifest = Join-Path $projectRoot 'docs/e2e/viq_e2e_run_manifest.json' }
& $Python (Join-Path $PSScriptRoot 'e2e_control.py') prepare --manifest $Manifest --run-id $RunId
if ($LASTEXITCODE -ne 0) { throw 'Offline E2E preparation failed; nothing was started.' }
