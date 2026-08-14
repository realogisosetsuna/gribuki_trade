<#
.SYNOPSIS
Run pytest with the repository's centralized scratch-root policy.

.EXAMPLE
.\scripts\run_tests.ps1 -TempDir D:\scratch\gribuki -PytestArgs @(
    'tests/unit/test_temp_root.py', '-q'
)
#>

#Requires -Version 5.1

[CmdletBinding()]
param(
    [string]$TempDir = '',

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$venvPython = Join-Path $repositoryRoot '.venv\Scripts\python.exe'
$python = if (Test-Path -LiteralPath $venvPython -PathType Leaf) {
    $venvPython
}
else {
    'python'
}

$arguments = @('-m', 'pytest')
if (-not [string]::IsNullOrWhiteSpace($TempDir)) {
    $arguments += @('--temp-dir', $TempDir)
}
$arguments += $PytestArgs

$exitCode = 1
Push-Location -LiteralPath $repositoryRoot
try {
    & $python @arguments
    $exitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
exit $exitCode
