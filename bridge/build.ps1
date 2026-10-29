param([string]$SdkPath = '')
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
if (-not $SdkPath) {
    $SdkPath = Join-Path $taskRoot 'sdk'
    if (-not (Test-Path -LiteralPath $SdkPath)) { $SdkPath = Join-Path $taskRoot 'baomihua\unpacked' }
}
$buildPath = Join-Path $PSScriptRoot 'build'
New-Item -ItemType Directory -Path $buildPath -Force | Out-Null
[System.Reflection.Assembly]::LoadFrom((Join-Path $PSHOME 'Microsoft.CodeAnalysis.dll')) | Out-Null
[System.Reflection.Assembly]::LoadFrom((Join-Path $PSHOME 'Microsoft.CodeAnalysis.CSharp.dll')) | Out-Null
$source = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'Program.cs') -Raw
$syntax = [Microsoft.CodeAnalysis.CSharp.CSharpSyntaxTree]::ParseText($source)
$frameworkPath = $SdkPath
if (-not (Test-Path -LiteralPath (Join-Path $frameworkPath 'System.Private.CoreLib.dll'))) {
    $runtime = dotnet --list-runtimes | Where-Object {$_ -match '^Microsoft.NETCore.App 8\.'} | Select-Object -Last 1
    if (-not $runtime -or $runtime -notmatch '^Microsoft.NETCore.App (\S+) \[(.+)\]') {throw '编译桥接组件需要 .NET 8 x64 运行时'}
    $frameworkPath = Join-Path $Matches[2] $Matches[1]
}
$referencePaths = @(Get-ChildItem -LiteralPath $frameworkPath -File | Where-Object {
    ($_.Name -like 'System.*.dll' -and $_.Name -notlike '*.Native.dll') -or $_.Name -in @('netstandard.dll','mscorlib.dll')
}) + @(Get-Item -LiteralPath (Join-Path $SdkPath 'ntkernel.dll'),(Join-Path $SdkPath 'QRCoder.dll'))
$refs = [System.Collections.Generic.List[Microsoft.CodeAnalysis.MetadataReference]]::new()
foreach ($file in $referencePaths) {
    try {
        $reference = [Microsoft.CodeAnalysis.MetadataReference]::CreateFromFile($file.FullName)
        $refs.Add($reference)
    } catch { }
}
$options = [Microsoft.CodeAnalysis.CSharp.CSharpCompilationOptions]::new([Microsoft.CodeAnalysis.OutputKind]::ConsoleApplication)
$options = $options.WithPlatform([Microsoft.CodeAnalysis.Platform]::X64).WithOptimizationLevel([Microsoft.CodeAnalysis.OptimizationLevel]::Release)
$compilation = [Microsoft.CodeAnalysis.CSharp.CSharpCompilation]::Create('CloudProbe.Bridge', [Microsoft.CodeAnalysis.SyntaxTree[]]@($syntax), $refs, $options)
$target = Join-Path $buildPath 'CloudProbe.Bridge.dll'
$stream = [System.IO.File]::Create($target)
try { $result = $compilation.Emit($stream) } finally { $stream.Dispose() }
if (-not $result.Success) {
    $result.Diagnostics | Where-Object {$_.Severity -eq 'Error'} | ForEach-Object {$_.ToString()}
    throw '桥接组件编译失败'
}
@'
{"runtimeOptions":{"tfm":"net8.0","framework":{"name":"Microsoft.NETCore.App","version":"8.0.0"},"rollForward":"LatestPatch","configProperties":{"Sentry.Native.IsEnabled":false}}}
'@ | Set-Content -LiteralPath (Join-Path $buildPath 'CloudProbe.Bridge.runtimeconfig.json') -Encoding utf8
Get-Item -LiteralPath $target | Select-Object Name,Length
