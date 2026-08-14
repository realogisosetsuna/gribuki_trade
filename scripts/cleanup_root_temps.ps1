<#
.SYNOPSIS
Delete only the workspace-root .tmp directories frozen in an archive manifest.

.DESCRIPTION
The evidence archive must already exist. The command refuses set drift,
non-root paths, reparse points, and an incomplete evidence copy before it calls
Remove-Item. Deleted reproducible scratch data does not enter the Recycle Bin.
#>

#Requires -Version 5.1

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Manifest,

    [Parameter(Mandatory = $true)]
    [ValidateSet('DELETE_ARCHIVED_ROOT_TEMPS')]
    [string]$Confirm,

    [string]$Workspace = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-TreeInventory {
    param([Parameter(Mandatory = $true)][string]$Root)

    $rootPath = [IO.Path]::GetFullPath($Root).TrimEnd(
        [IO.Path]::DirectorySeparatorChar
    )
    $records = @(
        Get-ChildItem -LiteralPath $rootPath -Recurse -File -Force |
            ForEach-Object {
                $relative = $_.FullName.Substring($rootPath.Length + 1).Replace(
                    [IO.Path]::DirectorySeparatorChar,
                    [IO.Path]::AltDirectorySeparatorChar
                )
                [pscustomobject]@{
                    Relative = $relative
                    Size = [long]$_.Length
                    Sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
                }
            } |
            Sort-Object -Property Relative -CaseSensitive
    )
    $digest = [Security.Cryptography.IncrementalHash]::CreateHash(
        [Security.Cryptography.HashAlgorithmName]::SHA256
    )
    $utf8 = [Text.UTF8Encoding]::new($false)
    foreach ($record in $records) {
        $line = [string]$record.Relative + [char]0 +
            [string]$record.Size + [char]0 +
            [string]$record.Sha256 + "`n"
        $digest.AppendData($utf8.GetBytes($line))
    }
    $treeSha256 = [BitConverter]::ToString($digest.GetHashAndReset()).Replace(
        '-',
        ''
    ).ToLowerInvariant()
    $digest.Dispose()
    return [pscustomobject]@{
        FileCount = $records.Count
        SizeBytes = [long](($records | Measure-Object -Property Size -Sum).Sum)
        TreeSha256 = $treeSha256
    }
}

$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$workspaceRoot = if ([string]::IsNullOrWhiteSpace($Workspace)) {
    $repositoryRoot
}
else {
    [IO.Path]::GetFullPath($Workspace)
}
$workspaceRoot = $workspaceRoot.TrimEnd([IO.Path]::DirectorySeparatorChar)
$manifestPath = [IO.Path]::GetFullPath($Manifest)
$manifestChecksumPath = $manifestPath + '.sha256'
if (-not (Test-Path -LiteralPath $manifestChecksumPath -PathType Leaf)) {
    throw 'manifest SHA-256 sidecar is missing'
}
$expectedManifestSha256 = [IO.File]::ReadAllText($manifestChecksumPath).Trim()
$actualManifestSha256 = (Get-FileHash `
    -LiteralPath $manifestPath `
    -Algorithm SHA256
).Hash.ToLowerInvariant()
if ($expectedManifestSha256 -cne $actualManifestSha256) {
    throw 'manifest SHA-256 verification failed'
}
$document = [IO.File]::ReadAllText($manifestPath) | ConvertFrom-Json

$documentWorkspace = [IO.Path]::GetFullPath([string]$document.workspace).TrimEnd(
    [IO.Path]::DirectorySeparatorChar
)
if (-not $documentWorkspace.Equals(
    $workspaceRoot,
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw 'manifest workspace mismatch'
}
if ($document.authority_touched -ne $false) {
    throw 'manifest does not assert protected authority isolation'
}
if ($document.cleanup_applied -ne $false) {
    throw 'manifest cleanup is not pending'
}

$expectedNames = @(
    $document.entries |
        ForEach-Object { [string]$_.name } |
        Sort-Object
)
$currentDirectories = @(
    Get-ChildItem -LiteralPath $workspaceRoot -Directory -Force |
        Where-Object { $_.Name -like '.tmp*' } |
        Sort-Object Name
)
$currentNames = @($currentDirectories | ForEach-Object { $_.Name })
$setDifference = @(
    Compare-Object -ReferenceObject $expectedNames -DifferenceObject $currentNames
)
if (
    $expectedNames.Count -ne $currentNames.Count -or
    $setDifference.Count -ne 0
) {
    throw 'root temp set changed after archival; refusing cleanup'
}

foreach ($entry in $document.entries) {
    $target = [IO.Path]::GetFullPath(
        (Join-Path $workspaceRoot ([string]$entry.name))
    ).TrimEnd([IO.Path]::DirectorySeparatorChar)
    $parent = [IO.Path]::GetDirectoryName($target)
    if (-not $parent.Equals(
        $workspaceRoot,
        [StringComparison]::OrdinalIgnoreCase
    )) {
        throw "target escaped workspace root: $target"
    }
    if (-not [IO.Path]::GetFileName($target).StartsWith(
        '.tmp',
        [StringComparison]::Ordinal
    )) {
        throw "target is not an explicit .tmp directory: $target"
    }
    $rootItem = Get-Item -LiteralPath $target -Force
    if (-not $rootItem.PSIsContainer) {
        throw "target is not a directory: $target"
    }
    if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "target is a reparse point: $target"
    }
    $nestedReparse = @(
        Get-ChildItem -LiteralPath $target -Recurse -Force -ErrorAction Stop |
            Where-Object {
                ($_.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0
            }
    )
    if ($nestedReparse.Count -ne 0) {
        throw "target contains reparse points: $target"
    }
    $sourceInventory = Get-TreeInventory -Root $target
    if (
        $sourceInventory.FileCount -ne [int]$entry.file_count -or
        $sourceInventory.SizeBytes -ne [long]$entry.size_bytes -or
        $sourceInventory.TreeSha256 -cne [string]$entry.tree_sha256
    ) {
        throw "source temp tree changed after archival: $target"
    }
}

$unique = @(
    $document.entries |
        Where-Object { $_.classification -eq 'ARCHIVED_UNIQUE_EVIDENCE' }
)
if ($unique.Count -ne 1) {
    throw 'expected exactly one archived unique-evidence tree'
}
$archiveEvidence = Join-Path `
    ([string]$document.archive_path) `
    ([string]$unique[0].archive_relative_path)
if (-not (Test-Path -LiteralPath $archiveEvidence -PathType Container)) {
    throw 'archived evidence copy is missing'
}
$archiveInventory = Get-TreeInventory -Root $archiveEvidence
if (
    $archiveInventory.FileCount -ne [int]$unique[0].file_count -or
    $archiveInventory.SizeBytes -ne [long]$unique[0].size_bytes -or
    $archiveInventory.TreeSha256 -cne [string]$unique[0].tree_sha256
) {
    throw 'archived evidence tree does not match the manifest'
}

$deletedBytes = [long]$document.total_size_bytes
foreach ($directory in $currentDirectories) {
    Remove-Item -LiteralPath $directory.FullName -Recurse -Force -ErrorAction Stop
}
$remaining = @(
    Get-ChildItem -LiteralPath $workspaceRoot -Directory -Force |
        Where-Object { $_.Name -like '.tmp*' }
)
if ($remaining.Count -ne 0) {
    throw 'root temp cleanup is incomplete'
}

[pscustomobject]@{
    deleted_bytes = $deletedBytes
    deleted_directories = $currentDirectories.Count
    remaining_root_temp_directories = $remaining.Count
} | ConvertTo-Json -Compress
