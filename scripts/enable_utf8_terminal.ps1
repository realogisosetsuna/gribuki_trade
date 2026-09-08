# 为 Windows PowerShell 5.1 设置 UTF-8 输出，避免中文源文件和 CLI 输出乱码。
$utf8 = New-Object System.Text.UTF8Encoding($false)
chcp 65001 > $null
[Console]::InputEncoding = $utf8
[Console]::OutputEncoding = $utf8
$OutputEncoding = $utf8
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
Write-Host "UTF-8 terminal encoding enabled for this PowerShell session."
