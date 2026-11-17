param(
    [Parameter(Mandatory=$true)][string]$Approval,
    [string]$Python = 'python',
    [string]$Manifest = ''
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
if (-not $Manifest) { $Manifest = Join-Path $projectRoot 'docs/e2e/viq_e2e_run_manifest.json' }
& $Python (Join-Path $PSScriptRoot 'e2e_control.py') stop --manifest $Manifest --approval $Approval
if ($LASTEXITCODE -ne 0) { throw 'Stop refused/failed; inspect exact E2E container identities. No cleanup was attempted.' }
