[CmdletBinding()]
param(
    [ValidatePattern('^\d{5,12}$')]
    [string]$QQAccount = ''
)

$ErrorActionPreference = 'Stop'

# NapCat emits UTF-8 text (including the terminal QR code). Windows PowerShell
# commonly starts with code page 936 even when Console.OutputEncoding reports
# UTF-8, so native output is otherwise decoded as GBK and becomes mojibake.
$codePageTool = Join-Path $env:SystemRoot 'System32\chcp.com'
& $codePageTool 65001 | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Unable to switch the console to UTF-8 (chcp exit code $LASTEXITCODE)"
}

$utf8NoBom = [Text.UTF8Encoding]::new($false)
[Console]::InputEncoding = $utf8NoBom
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

# NapCat's logger always emits ANSI colors. Enable virtual-terminal processing
# for legacy Console Host; Windows Terminal already has the flag enabled. A
# redirected handle is not a console handle, so GetConsoleMode safely skips it.
if (-not ('GribukiTrade.ConsoleNativeMethods' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

namespace GribukiTrade
{
    public static class ConsoleNativeMethods
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr GetStdHandle(int standardHandle);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetConsoleMode(
            IntPtr consoleHandle,
            out uint consoleMode
        );

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool SetConsoleMode(
            IntPtr consoleHandle,
            uint consoleMode
        );
    }
}
'@
}

$enableVirtualTerminalProcessing = [uint32]0x0004
foreach ($standardHandle in @(-11, -12)) {
    $handle = [GribukiTrade.ConsoleNativeMethods]::GetStdHandle($standardHandle)
    $consoleMode = [uint32]0
    if ([GribukiTrade.ConsoleNativeMethods]::GetConsoleMode(
        $handle,
        [ref]$consoleMode
    )) {
        [void][GribukiTrade.ConsoleNativeMethods]::SetConsoleMode(
            $handle,
            ($consoleMode -bor $enableVirtualTerminalProcessing)
        )
    }
}

$repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$shellRoot = (Resolve-Path -LiteralPath (
    Join-Path $repositoryRoot 'vendor\NapCatQQ-shell-v4.18.18'
)).Path
$qqRoot = (Resolve-Path -LiteralPath (
    Join-Path $repositoryRoot 'vendor\qq-extracted-9.9.32.50969\Files'
)).Path

$launcher = Join-Path $shellRoot 'NapCatWinBootMain.exe'
$hook = Join-Path $shellRoot 'NapCatWinBootHook.dll'
$main = Join-Path $shellRoot 'napcat.mjs'
$loader = Join-Path $shellRoot 'loadNapCat.js'
$patchPackage = Join-Path $shellRoot 'qqnt.json'
$qq = Join-Path $qqRoot 'QQ.exe'

foreach ($required in @($launcher, $hook, $main, $patchPackage, $qq)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "NapCat portable dependency is missing: $required"
    }
}

$env:NAPCAT_PATCH_PACKAGE = $patchPackage
$env:NAPCAT_LOAD_PATH = $loader
$env:NAPCAT_INJECT_PATH = $hook
$env:NAPCAT_LAUNCHER_PATH = $launcher
$env:NAPCAT_MAIN_PATH = $main

$mainUri = [Uri]::new($main).AbsoluteUri
$loaderSource = "(async () => { await import('$mainUri') })()`n"
[IO.File]::WriteAllText(
    $loader,
    $loaderSource,
    [Text.UTF8Encoding]::new($false)
)

$arguments = @($qq, $hook)
if ($QQAccount) {
    $arguments += @('-q', $QQAccount)
}

Push-Location $shellRoot
try {
    & $launcher @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "NapCat launcher exited with code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
