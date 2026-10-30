param(
    [Parameter(Mandatory=$true)][string]$SdkPath,
    [Parameter(Mandatory=$true)][string]$OutputRoot,
    [string]$ExpectedTag = ''
)
$ErrorActionPreference = 'Stop'
$repo = Split-Path $PSScriptRoot -Parent
$SdkPath = (Resolve-Path -LiteralPath $SdkPath).Path
$OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
$source = Get-Content -LiteralPath (Join-Path $repo 'main.go') -Raw
if ($source -notmatch 'const version = "([0-9]+\.[0-9]+\.[0-9]+)"') { throw '找不到版本号' }
$version = $Matches[1]
if ($ExpectedTag -and $ExpectedTag -ne "v$version") { throw '标签与 main.go 版本号不一致' }
$files = @('ntkernel.dll','BaoMiHua.dll','QRCoder.dll','msvcp140.dll','msvcp140_1.dll','msvcp140_2.dll','msvcp140_atomic_wait.dll','msvcp140_codecvt_ids.dll','vcruntime140.dll','vcruntime140_1.dll','concrt140.dll')
foreach ($name in $files) { if (!(Test-Path -LiteralPath (Join-Path $SdkPath $name) -PathType Leaf)) { throw "SDK 缺少 $name" } }
$package = Join-Path $OutputRoot "CloudProbeWebDAV-v$version-Windows-x64"
if (Test-Path -LiteralPath $package) { throw '目标包已存在，拒绝覆盖' }
New-Item -ItemType Directory -Path "$package\sdk","$package\bridge" | Out-Null
foreach ($name in $files) { Copy-Item -LiteralPath (Join-Path $SdkPath $name) -Destination "$package\sdk" }
foreach ($name in @('QRCoder LICENSE.txt','THIRD-PARTY-NOTICES.txt')) {
    if (Test-Path -LiteralPath (Join-Path $SdkPath $name)) { Copy-Item -LiteralPath (Join-Path $SdkPath $name) -Destination "$package\sdk" }
}
& (Join-Path $repo 'bridge\build.ps1') -SdkPath $SdkPath
Copy-Item -LiteralPath "$repo\bridge\build\CloudProbe.Bridge.dll","$repo\bridge\build\CloudProbe.Bridge.runtimeconfig.json" -Destination "$package\bridge"
Push-Location $repo
try {
    & go build -mod=vendor -trimpath -ldflags '-s -w' -o "$package\cloudprobe.exe" .
    if ($LASTEXITCODE -ne 0) { throw 'Go 编译失败' }
} finally { Pop-Location }
Copy-Item -LiteralPath "$repo\third_party" -Destination "$package\third_party" -Recurse
@"
CloudProbe $version — Windows x64 完整运行包

完整解压后双击 cloudprobe.exe，自动打开管理界面。保留 sdk、bridge 两个目录。
需要 .NET 8 x64 运行时，本包未包含 .NET 运行时。
配置、账号、日志、浏览器登录资料和上传缓存默认全部放在 EXE 旁的 data 目录。
--data 只接受软件目录内路径，相对路径也以 EXE 所在目录为基准。
软件目录不可写时会报错，不会回退到 AppData 或系统临时目录。
退出程序后删除整个软件目录即可移除本程序管理的数据。
升级时将新程序与组件覆盖到原软件目录，保留 data；不要覆盖自己的 data。
SDK 为原版组件，保留原版权。发布者应确保 SDK 来源及再分发权限。
本分发 ZIP 不包含个人资料。
"@ | Set-Content -LiteralPath "$package\启动说明.txt" -Encoding utf8
& (Join-Path $PSScriptRoot 'smoke-windows.ps1') -Package $package
Compress-Archive -LiteralPath $package -DestinationPath "$package.zip" -CompressionLevel Optimal
Get-Item -LiteralPath "$package.zip" | Select-Object FullName,Length
