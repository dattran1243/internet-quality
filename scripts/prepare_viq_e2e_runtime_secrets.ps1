# Windows-only private consumer. Default Inspect never prompts, decrypts or writes secrets.
# No Docker, MinIO Client config, production credentials, account changes or Airflow commands.
param(
    [ValidateSet('Inspect','Prepare')][string]$Action = 'Inspect',
    [Parameter(Mandatory=$true)][ValidatePattern('^[a-f0-9]{64}$')][string]$ExpectedManifestSha256,
    [string]$Python = 'python'
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-ViqPrivateAcl([string]$Path, [string]$OwnerSid) {
    $acl = Get-Acl -LiteralPath $Path
    if ($acl.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin @($OwnerSid,'S-1-5-18')) {
        throw 'PRIVATE_ACL_UNEXPECTED_OWNER'
    }
    if ((Test-Path -LiteralPath $Path -PathType Container) -and -not $acl.AreAccessRulesProtected) {
        throw 'PRIVATE_DIRECTORY_INHERITANCE_NOT_PROTECTED'
    }
    foreach ($ace in $acl.Access) {
        if ($ace.AccessControlType -eq 'Allow') {
            $sid = $ace.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
            if ($sid -notin @($OwnerSid,'S-1-5-18')) { throw 'PRIVATE_ACL_EXTRA_IDENTITY' }
        }
    }
    foreach ($requiredSid in @($OwnerSid,'S-1-5-18')) {
        if (-not @($acl.Access | Where-Object {
            $_.AccessControlType -eq 'Allow' -and
            $_.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -eq $requiredSid -and
            ($_.FileSystemRights -band [Security.AccessControl.FileSystemRights]::FullControl) -eq [Security.AccessControl.FileSystemRights]::FullControl
        }).Count) { throw 'PRIVATE_ACL_REQUIRED_IDENTITY_MISSING' }
    }
}

function Convert-ViqSecureToPlain([Security.SecureString]$Secure) {
    if ($Secure.Length -eq 0) { throw 'EMPTY_PRIVATE_INPUT' }
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }
}

function New-ViqRandomText([int]$ByteCount) {
    $bytes = New-Object byte[] $ByteCount
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $rng.GetBytes($bytes)
        return [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+','-').Replace('/','_')
    } finally {
        $rng.Dispose()
        [Array]::Clear($bytes,0,$bytes.Length)
    }
}

function Write-ViqPrivateJson([string]$Path, [object]$Payload) {
    # CreateNew prevents overwrite or following an existing file/symlink.
    $json = $Payload | ConvertTo-Json -Depth 6 -Compress
    $bytes = ([Text.UTF8Encoding]::new($false)).GetBytes($json)
    $stream = [IO.File]::Open($Path,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
    try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush() }
    finally { $stream.Dispose(); [Array]::Clear($bytes,0,$bytes.Length); $json = $null }
}

try {
    if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) { throw 'WINDOWS_DPAPI_REQUIRED' }
    $viqRoot = Split-Path $PSScriptRoot -Parent
    $viqManifestPath = Join-Path $viqRoot 'docs\e2e\viq_e2e_run_manifest.json'
    if ((Get-FileHash -LiteralPath $viqManifestPath -Algorithm SHA256).Hash.ToLowerInvariant() -ne $ExpectedManifestSha256) {
        throw 'MANIFEST_HASH_MISMATCH'
    }
    $viqManifest = Get-Content -LiteralPath $viqManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $viqControl = Join-Path $viqRoot $viqManifest.runtime_layout.host_control
    $viqPrivate = Join-Path $viqRoot $viqManifest.runtime_layout.host_private
    $viqApprovalPath = Join-Path $viqControl 'approval.json'
    # This pure Python preflight reads public controls, hashes and file metadata only.
    $viqPreflight = @'
import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from processing.e2e.guards import load_manifest,file_hash,gate_c,verify_sources,require
from processing.e2e.layout import host_paths,reject_links
from processing.e2e.attestation import verify_attestation
root=Path(sys.argv[1]);mp=Path(sys.argv[2]);ap=Path(sys.argv[3])
m=load_manifest(mp);require(file_hash(mp)==sys.argv[4],'PRIVATE_CONSUMER_MANIFEST')
for action in ('provision','start','stop'):gate_c(m,mp,ap,action)
a=json.loads(ap.read_bytes());require(set(a['actions'])=={'provision','start','stop'},'PRIVATE_CONSUMER_ACTIONS')
verify_sources(m,root)
for name,digest in m['deployment_sha256'].items():require(file_hash(root/name)==digest,'PRIVATE_CONSUMER_DEPLOYMENT_PIN')
paths=host_paths(m,root);reject_links(ap)
require(ap.resolve()==(paths['control']/'approval.json').resolve(),'PRIVATE_CONSUMER_APPROVAL_PATH')
require(m['provision_attestation']['mode']=='security_rebind_v2','PRIVATE_CONSUMER_ORIGINAL_BINDING')
verify_attestation(m,mp,paths['control'])
reject_links(root/m['host_evidence']/'secrets/ca_credentials.dpapi.json')
print('PRIVATE_CONSUMER_PUBLIC_PREFLIGHT_PASS')
'@
    try {
        $viqPreflightOutput = & $Python -B -c $viqPreflight $viqRoot $viqManifestPath $viqApprovalPath $ExpectedManifestSha256 2>$null
        if ($LASTEXITCODE -ne 0 -or $viqPreflightOutput -ne 'PRIVATE_CONSUMER_PUBLIC_PREFLIGHT_PASS') { throw 'PUBLIC_PREFLIGHT_FAILED' }
    } catch { throw 'PUBLIC_PREFLIGHT_FAILED_NO_PRIVATE_INPUT_ACCESSED' }
    $viqSid = [Security.Principal.WindowsIdentity]::GetCurrent().User
    $viqInputPath = Join-Path $viqRoot ($viqManifest.host_evidence + '/secrets/ca_credentials.dpapi.json')
    if (-not (Test-Path -LiteralPath $viqInputPath -PathType Leaf)) { throw 'CA_PRIVATE_INPUT_MISSING' }
    Assert-ViqPrivateAcl $viqInputPath $viqSid.Value
    $viqFiles = @('worker.json','control.json','api.json','airflow_passwords.json')
    foreach ($viqFile in $viqFiles) {
        if (Test-Path -LiteralPath (Join-Path $viqPrivate $viqFile)) { throw 'RUNTIME_FILE_EXISTS_NO_OVERWRITE' }
    }
    if (Test-Path -LiteralPath $viqPrivate) { Assert-ViqPrivateAcl $viqPrivate $viqSid.Value }
    if ($Action -eq 'Inspect') {
        Write-Output 'RUNTIME_SECRET_INPUT_METADATA_CHECK=PASS; PREPARED=False; DECRYPTED=False'
        return
    }

    if (-not (Test-Path -LiteralPath $viqPrivate)) {
        [void][IO.Directory]::CreateDirectory($viqPrivate)
        $viqAcl = [Security.AccessControl.DirectorySecurity]::new()
        $viqAcl.SetAccessRuleProtection($true,$false)
        foreach ($viqAllowedSid in @($viqSid,[Security.Principal.SecurityIdentifier]::new('S-1-5-18'))) {
            $viqAcl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new(
                $viqAllowedSid,[Security.AccessControl.FileSystemRights]::FullControl,
                ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit),
                [Security.AccessControl.PropagationFlags]::None,[Security.AccessControl.AccessControlType]::Allow))
        }
        # Persist only the DACL; do not request SACL/audit privileges.
        [IO.Directory]::SetAccessControl($viqPrivate,$viqAcl)
    }
    Assert-ViqPrivateAcl $viqPrivate $viqSid.Value
    # Recheck canonical no-link roots after directory creation and before private input.
    try {
        $viqPreflightOutput = & $Python -B -c $viqPreflight $viqRoot $viqManifestPath $viqApprovalPath $ExpectedManifestSha256 2>$null
        if ($LASTEXITCODE -ne 0 -or $viqPreflightOutput -ne 'PRIVATE_CONSUMER_PUBLIC_PREFLIGHT_PASS') { throw 'PUBLIC_PREFLIGHT_CHANGED' }
    } catch { throw 'PUBLIC_PREFLIGHT_CHANGED_NO_PRIVATE_INPUT_ACCESSED' }
    # All authority/path/ACL/no-overwrite checks above precede decryption and secure prompt.
    $viqInput = Get-Content -LiteralPath $viqInputPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $viqKeys = @('POSTGRES_E2E_PASSWORD','CLICKHOUSE_LOADER_PASSWORD','CLICKHOUSE_API_PASSWORD')
    if ($viqInput.schema_version -ne 1 -or $viqInput.protection -ne 'Windows DPAPI CurrentUser' -or
        $viqInput.manifest_sha256 -ne $viqManifest.provision_attestation.original_manifest_sha256 -or
        $viqInput.run_id -ne $viqManifest.run_id -or $viqInput.protected_for_windows_sid -ne $viqSid.Value -or
        @($viqInput.protected_values.PSObject.Properties.Name).Count -ne 3 -or
        @(Compare-Object @($viqInput.protected_values.PSObject.Properties.Name | Sort-Object) @($viqKeys | Sort-Object)).Count -ne 0) {
        throw 'CA_DPAPI_BINDING_MISMATCH'
    }
    $viqValues = @{}
    foreach ($viqKey in $viqKeys) {
        $viqSecure = ConvertTo-SecureString $viqInput.protected_values.$viqKey
        try { $viqValues[$viqKey] = Convert-ViqSecureToPlain $viqSecure }
        finally { $viqSecure.Dispose() }
    }
    $viqMinioSecure = Read-Host 'Existing E2E MinIO worker secret (never production/root; input hidden)' -AsSecureString
    try { $viqMinioSecret = Convert-ViqSecureToPlain $viqMinioSecure }
    finally { $viqMinioSecure.Dispose() }
    $viqJwt = New-ViqRandomText 48
    $viqAuthPassword = New-ViqRandomText 32
    $viqResources = $viqManifest.resources
    $viqDsn = 'postgresql+psycopg2://' + $viqResources.metadata_user + ':' +
        [Uri]::EscapeDataString($viqValues.POSTGRES_E2E_PASSWORD) + '@' +
        $viqManifest.endpoints.postgres_host + ':' + $viqManifest.endpoints.postgres_port + '/' + $viqResources.metadata_database
    $viqControlPayload = [ordered]@{
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = $viqDsn
        AIRFLOW__API_AUTH__JWT_SECRET = $viqJwt
        VIQ_AIRFLOW_ADMIN_PASSWORD = $viqAuthPassword
    }
    $viqWorkerPayload = [ordered]@{}
    foreach ($viqKey in $viqControlPayload.Keys) { $viqWorkerPayload[$viqKey] = $viqControlPayload[$viqKey] }
    $viqWorkerPayload['MINIO_ACCESS_KEY'] = 'viq_e2e_worker_' + $viqResources.rid
    $viqWorkerPayload['MINIO_SECRET_KEY'] = $viqMinioSecret
    $viqWorkerPayload['VIQ_CH_LOADER_PASSWORD'] = $viqValues.CLICKHOUSE_LOADER_PASSWORD
    $viqPayloads = [ordered]@{
        'worker.json' = $viqWorkerPayload
        'control.json' = $viqControlPayload
        'api.json' = [ordered]@{ CLICKHOUSE_PASSWORD = $viqValues.CLICKHOUSE_API_PASSWORD }
        'airflow_passwords.json' = [ordered]@{ e2eadmin = $viqAuthPassword }
    }
    foreach ($viqFile in $viqFiles) {
        $viqPath = Join-Path $viqPrivate $viqFile
        Write-ViqPrivateJson $viqPath $viqPayloads[$viqFile]
        Assert-ViqPrivateAcl $viqPath $viqSid.Value
    }
    $viqValues.Clear(); $viqPayloads.Clear(); $viqWorkerPayload.Clear(); $viqControlPayload.Clear()
    $viqDsn = $viqJwt = $viqAuthPassword = $viqMinioSecret = $null
    Write-Output 'RUNTIME_SECRET_FILES_PREPARED=True; FILES=4; CREDENTIAL_VALUES_OUTPUT=False; APPLICATION_IO=False'
} catch {
    # Never echo third-party exception messages or private variables.
    Write-Error 'Runtime secret preparation refused/failed. Output details withheld. Preserve any private files; no overwrite or automatic retry.'
    exit 1
}
