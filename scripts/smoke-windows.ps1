param([Parameter(Mandatory=$true)][string]$Package)
$ErrorActionPreference = 'Stop'
$Package = (Resolve-Path -LiteralPath $Package).Path
$dataPath = Join-Path $Package 'data'
if (Test-Path -LiteralPath $dataPath) { throw '启动检查只允许无个人资料的新包' }
$stdout = Join-Path $Package 'smoke-stdout.txt'
$stderr = Join-Path $Package 'smoke-stderr.txt'
$baseUrl = $null
$state = $null
$proc = Start-Process -FilePath "$Package\cloudprobe.exe" -ArgumentList @('gui','--no-open') -WorkingDirectory (Split-Path $Package -Parent) -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
try {
    for ($i=0; $i -lt 120; $i++) {
        Start-Sleep -Milliseconds 250
        $line = Get-Content -LiteralPath $stdout | Where-Object { $_ -like 'GUI_URL=*' } | Select-Object -First 1
        if (!$line) { if ($proc.HasExited) { throw '程序提前退出' }; continue }
        $uri = [Uri]$line.Substring(8)
        $baseUrl = $uri.GetLeftPart([UriPartial]::Authority)
        $headers = @{'X-CloudProbe-UI'=$uri.Fragment.TrimStart('#')}
        $state = Invoke-RestMethod "$baseUrl/api/status" -Headers $headers -TimeoutSec 5
        if ($state.engine_ready -or $state.engine_error) { break }
    }
    if (!$state.engine_ready) { throw "引擎启动失败：$($state.engine_error)" }
    if (!(Test-Path -LiteralPath "$dataPath\original-sdk")) { throw '未使用软件目录内 data' }
    $page = Invoke-WebRequest "$baseUrl/" -TimeoutSec 5
    if ($page.StatusCode -ne 200) { throw '管理页面不可访问' }
    Write-Output "启动检查通过：CloudProbe $($state.version)，引擎已就绪，默认 data 位于软件目录内。"
} finally {
    if ($baseUrl) { try { Invoke-RestMethod "$baseUrl/api/quit" -Method Post -Headers $headers -ContentType 'application/json' -Body '{}' -TimeoutSec 5 | Out-Null } catch {} }
    if (!$proc.WaitForExit(10000)) { Stop-Process -Id $proc.Id -Force; $proc.WaitForExit() }
    # This directory was absent before the check and is strictly inside Package.
    $resolved = [IO.Path]::GetFullPath($dataPath)
    if ($resolved -ne (Join-Path $Package 'data')) { throw '测试资料清理范围不符' }
    if (Test-Path -LiteralPath $resolved) { Remove-Item -LiteralPath $resolved -Recurse -Force }
    Remove-Item -LiteralPath $stdout,$stderr -Force -ErrorAction SilentlyContinue
}
