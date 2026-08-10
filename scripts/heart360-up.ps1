<#
.SYNOPSIS
    HEARTS360 Toolkit - port-aware launcher (Windows).

.DESCRIPTION
    Picks a free host port for every published service, records the choice in
    .env, then starts the stack. Solves the most common first-run failure:

      Error response from daemon: Ports are not available: exposing port
      TCP 0.0.0.0:3000 -> ... bind: An attempt was made to access a socket
      in a way forbidden by its access permissions.

    Note that on Windows the above can also mean the port sits inside a
    Hyper-V/WSL reserved range rather than being used by a real process; this
    script detects that case too, because it tests an actual bind.

.EXAMPLE
    .\scripts\heart360-up.ps1
.EXAMPLE
    .\scripts\heart360-up.ps1 -Check
.EXAMPLE
    .\scripts\heart360-up.ps1 -NoStart
#>
[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$NoStart,
    [switch]$Build
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $RepoRoot '.env'

$Services = @(
    [pscustomobject]@{ Key = 'grafana';     Var = 'HEART360_GRAFANA_PORT';     Default = 3000; Description = 'Grafana dashboards' }
    [pscustomobject]@{ Key = 'postgres';    Var = 'HEART360_POSTGRES_PORT';    Default = 5432; Description = 'PostgreSQL' }
    [pscustomobject]@{ Key = 'filebrowser'; Var = 'HEART360_FILEBROWSER_PORT'; Default = 8080; Description = 'FileBrowser upload UI' }
    [pscustomobject]@{ Key = 'pgadmin';     Var = 'HEART360_PGADMIN_PORT';     Default = 5050; Description = 'pgAdmin' }
)

$ContainerVars = @(
    'HEART360_GRAFANA_CONTAINER', 'HEART360_POSTGRES_CONTAINER',
    'HEART360_FILEBROWSER_CONTAINER', 'HEART360_PGADMIN_CONTAINER',
    'HEART360_FILEPROC_CONTAINER'
)
$DefaultContainers = @('grafana', 'postgres', 'filebrowser-quantum', 'pgadmin', 'file-upload-trigger')

function Write-Ok   { param($Message) Write-Host "  [ ok ] $Message" }
function Write-Warn { param($Message) Write-Host "  [warn] $Message" -ForegroundColor Yellow }
function Write-Info { param($Message) Write-Host "  $Message" -ForegroundColor DarkGray }

function Get-EnvValue {
    param([string]$Key)
    if (-not (Test-Path $EnvFile)) { return $null }
    $match = Select-String -Path $EnvFile -Pattern "^\s*$([regex]::Escape($Key))=(.*)$" |
             Select-Object -Last 1
    if ($null -eq $match) { return $null }
    return $match.Matches[0].Groups[1].Value.Trim()
}

function Set-EnvValue {
    param([string]$Key, [string]$Value)
    if (-not (Test-Path $EnvFile)) { New-Item -ItemType File -Path $EnvFile -Force | Out-Null }
    $lines = @(Get-Content -LiteralPath $EnvFile)
    $pattern = "^\s*$([regex]::Escape($Key))="
    if ($lines -match $pattern) {
        $lines = $lines | ForEach-Object { if ($_ -match $pattern) { "$Key=$Value" } else { $_ } }
    }
    else {
        $lines += "$Key=$Value"
    }
    Set-Content -LiteralPath $EnvFile -Value $lines -Encoding UTF8
}

function Get-OwnPorts {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { return @() }

    $names = @($DefaultContainers)
    foreach ($var in $ContainerVars) {
        $value = Get-EnvValue $var
        if ($value) { $names += $value }
    }

    $filters = foreach ($n in ($names | Select-Object -Unique)) { '--filter'; "name=^/$n$" }
    $output = & docker ps @filters --format '{{.Ports}}' 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $output) { return @() }

    $ports = foreach ($line in $output) {
        foreach ($m in [regex]::Matches($line, ':(\d+)->')) { [int]$m.Groups[1].Value }
    }
    return @($ports | Select-Object -Unique)
}

function Test-PortInUse {
    param([int]$Port)

    # 1. Any listener, in any address family. Docker Desktop publishes on the
    #    IPv6 wildcard (::), which an IPv4-only bind test silently misses.
    if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue) {
        $listeners = @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)
        if ($listeners.Count -gt 0) { return $true }
    }

    # 2. A real bind attempt. This additionally catches Hyper-V/WSL excluded
    #    port ranges, which have no listener at all yet still refuse binds and
    #    produce the confusing "access ... forbidden by its access permissions"
    #    error from Docker.
    $addresses = @([System.Net.IPAddress]::Any)
    if ([System.Net.Sockets.Socket]::OSSupportsIPv6) {
        $addresses += [System.Net.IPAddress]::IPv6Any
    }

    foreach ($address in $addresses) {
        $listener = $null
        try {
            $listener = [System.Net.Sockets.TcpListener]::new($address, $Port)
            $listener.Start()
        }
        catch {
            return $true
        }
        finally {
            if ($listener) { try { $listener.Stop() } catch { } }
        }
    }

    return $false
}

function Get-NextFreePort {
    param([int]$StartPort, [int[]]$Reserved)
    for ($candidate = $StartPort; $candidate -le 65535; $candidate++) {
        if ($Reserved -contains $candidate) { continue }
        if (-not (Test-PortInUse $candidate)) { return $candidate }
    }
    throw "No free port found at or above $StartPort."
}

# ------------------------------------------------------------------- main ----
Write-Host ''
Write-Host 'HEARTS360 Toolkit - port preflight'
Write-Host ''

if (-not (Test-Path $EnvFile) -and -not $Check) {
    $example = Join-Path $RepoRoot '.env.example'
    if (Test-Path $example) {
        Copy-Item $example $EnvFile
        Write-Ok 'created .env from .env.example'
    }
}

$ownPorts = Get-OwnPorts
if ($ownPorts.Count -gt 0) {
    Write-Info "ports already held by this stack: $($ownPorts -join ', ')"
}

$reserved = New-Object System.Collections.Generic.List[int]
$resolved = @{}
$changed = $false

foreach ($svc in $Services) {
    $wantedRaw = Get-EnvValue $svc.Var
    $wanted = $svc.Default
    if ($wantedRaw -and ($wantedRaw -match '^\d+$')) { $wanted = [int]$wantedRaw }

    $label = $svc.Description.PadRight(24)

    if ($ownPorts -contains $wanted) {
        Write-Ok "$label $wanted (already served by this stack)"
        $reserved.Add($wanted); $resolved[$svc.Key] = $wanted
        continue
    }

    if (Test-PortInUse $wanted) {
        $chosen = Get-NextFreePort -StartPort ($wanted + 1) -Reserved $reserved.ToArray()
        Write-Warn "$label $wanted is busy -> using $chosen"
        $changed = $true
    }
    else {
        $chosen = $wanted
        Write-Ok "$label $chosen"
    }

    if (-not $Check) { Set-EnvValue $svc.Var $chosen }
    $reserved.Add($chosen); $resolved[$svc.Key] = $chosen
}

if (-not $Check -and $changed) {
    # Keep Grafana's generated links pointing at the port people actually use.
    Set-EnvValue 'HEART360_GRAFANA_ROOT_URL' "http://localhost:$($resolved['grafana'])/"
}

if ($Check) {
    Write-Host ''
    Write-Host '  (-Check: .env was not modified)'
    Write-Host ''
    return
}

if (-not $NoStart) {
    Write-Host ''
    Write-Host '  Starting stack...'
    Write-Host ''
    Push-Location $RepoRoot
    try {
        $composeArgs = @('compose', 'up', '-d')
        if ($Build) { $composeArgs += '--build' }
        & docker @composeArgs
        if ($LASTEXITCODE -ne 0) { throw "docker compose exited with code $LASTEXITCODE" }
    }
    finally { Pop-Location }
}

@"

  HEARTS360 Toolkit is configured on:

    Dashboards   http://localhost:$($resolved['grafana'])
    Upload files http://localhost:$($resolved['filebrowser'])
    pgAdmin      http://localhost:$($resolved['pgadmin'])
    PostgreSQL   localhost:$($resolved['postgres'])

  Ports are recorded in .env and will be reused on the next start.
  Trouble connecting or building? See docs/ports-and-proxy.md

"@ | Write-Host
