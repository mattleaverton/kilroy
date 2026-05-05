# Install kilroy binary and built-in workflows from this repo.
# Re-run after git pull to update both.
#
# Usage: powershell -ExecutionPolicy Bypass -File scripts\install.ps1
$ErrorActionPreference = "Stop"

$Repo = Split-Path -Parent $PSScriptRoot
$BinDir = "$env:LOCALAPPDATA\kilroy\bin"
$DataDir = "$env:LOCALAPPDATA\kilroy"

Write-Host "building kilroy..."
Push-Location $Repo
go build -o "$Repo\kilroy.exe" .\cmd\kilroy
Pop-Location

New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
New-Item -ItemType Directory -Force -Path "$DataDir\workflows" | Out-Null

Copy-Item "$Repo\kilroy.exe" "$BinDir\kilroy.exe" -Force
Copy-Item "$Repo\workflows\*" "$DataDir\workflows\" -Recurse -Force

Write-Host ""
Write-Host "installed:"
Write-Host "  binary:    $BinDir\kilroy.exe"
Write-Host "  workflows: $DataDir\workflows\"
Write-Host ""

$currentPath = [Environment]::GetEnvironmentVariable("PATH", "User")
if ($currentPath -notlike "*$BinDir*") {
    Write-Host "note: adding $BinDir to your user PATH..."
    [Environment]::SetEnvironmentVariable("PATH", "$currentPath;$BinDir", "User")
    Write-Host "restart your terminal for PATH changes to take effect"
}
