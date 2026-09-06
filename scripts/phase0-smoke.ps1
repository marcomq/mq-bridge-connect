$ErrorActionPreference = "Stop"

$RepoDir = Split-Path -Parent $PSScriptRoot
$TargetDir = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $RepoDir "target" }
$Profile = if ($env:PROFILE) { $env:PROFILE } else { "debug" }
$OutputDir = Join-Path $TargetDir $Profile
$GoLibrary = Join-Path $OutputDir "mq_bridge_redpanda_go.dll"
$RustLibrary = Join-Path $OutputDir "mq_bridge_redpanda.dll"
$Smoke = Join-Path $OutputDir "phase0_smoke.exe"

if (-not $env:GOCACHE) { $env:GOCACHE = Join-Path $TargetDir "go-build-cache" }
if (-not $env:GOMODCACHE) { $env:GOMODCACHE = Join-Path $TargetDir "go-mod-cache" }

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
Push-Location (Join-Path $RepoDir "go-bridge")
try {
    go build -buildmode=c-shared -o $GoLibrary .
} finally {
    Pop-Location
}

Push-Location $RepoDir
try {
    cargo build --locked --lib --bin phase0_smoke
} finally {
    Pop-Location
}

1..3 | ForEach-Object {
    & $Smoke $RustLibrary $GoLibrary
    if ($LASTEXITCODE -ne 0) { throw "phase0_smoke failed" }
}
