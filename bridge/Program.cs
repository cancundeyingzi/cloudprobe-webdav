using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using clr.wrapper;
using QRCoder;

internal static class Program
{
    [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool SetDllDirectory(string path);
    internal static StreamWriter Protocol;
    internal static string Sdk;

    [STAThread]
    public static int Main(string[] args)
    {
        Protocol = new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(false)) { AutoFlush = true };
        // 原版引擎的控制台输出不混入 IPC，也不导出可能包含会话的日志。
        Console.SetOut(TextWriter.Null);
        Console.SetError(TextWriter.Null);
        try
        {
            if (args.Length != 2) throw new ArgumentException("需要 SDK 目录与独立测试数据目录");
            Sdk = Path.GetFullPath(args[0]);
            SetDllDirectory(Sdk);
            Environment.SetEnvironmentVariable("PATH", Sdk + Path.PathSeparator + Environment.GetEnvironmentVariable("PATH"));
            AssemblyLoadContext.Default.Resolving += (context, name) =>
            {
                string file = Path.Combine(Sdk, name.Name + ".dll");
                return File.Exists(file) ? context.LoadFromAssemblyPath(file) : null;
            };
            return SdkHost.Run(Path.GetFullPath(args[1])).GetAwaiter().GetResult();
        }
        catch (Exception e)
        {
            Reply(new { id = 0, ok = false, error = e.GetType().Name + ": " + e.Message });
            return 1;
        }
    }

    internal static void Reply(object value) => Protocol.WriteLine(JsonSerializer.Serialize(value));
}

internal sealed class Adapter : IGlobalAdapter
{
    public override void OnConsoleOutput(string log) { }
}

internal sealed class Callback : IRequestCallback
{
    private readonly TaskCompletionSource<(int Code, string Message, string Body)> source =
        new TaskCompletionSource<(int, string, string)>(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task<(int Code, string Message, string Body)> Task => source.Task;
    public override void OnResult(int errCode, string errMsg, string traceId, string response) =>
        source.TrySetResult((errCode, errMsg ?? "", response ?? ""));
}

internal static class SdkHost
{
    private static IEngine engine;
    private static IRequestService requests;
    private static readonly Adapter adapter = new Adapter();

    internal static async Task<int> Run(string data)
    {
        Directory.CreateDirectory(data);
        string Make(string name) { string p = Path.Combine(data, name); Directory.CreateDirectory(p); return p; }
        engine = IEngine.Get();
        var config = new InitConfig(null, "1.2.1", 1210u, ServerType.KLiveServer,
            Make("log"), Make("log-cache"), Make("data"), Make("other"));
        if (!engine.Init(config, adapter))
            throw new InvalidOperationException("原版引擎初始化失败；若网易爆米花正在运行，请先退出后重试");
        requests = engine.GetRequestService();
        Program.Reply(new { id = 0, ok = true, ready = true, engine = "NetEase original ntkernel 1.2.1", inner_version = 1210 });
        try
        {
            string line;
            while ((line = await Console.In.ReadLineAsync()) != null)
            {
                long id = -1;
                bool dispatched = false;
                try
                {
                    using var doc = JsonDocument.Parse(line);
                    var root = doc.RootElement;
                    id = root.GetProperty("id").GetInt64();
                    if (!root.TryGetProperty("deadline_unix_ms", out var deadlineField) ||
                        !deadlineField.TryGetInt64(out long deadlineMs))
                        throw new ArgumentException("桥接请求缺少截止时间，请更新主程序");
                    long remainingMs = deadlineMs - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if (remainingMs <= 0) throw new TimeoutException("排队请求已过期，未调用 SDK");
                    string kind = root.GetProperty("kind").GetString();
                    if (kind == "stop") { Program.Reply(new { id, ok = true }); break; }
                    if (kind == "identity") { Program.Reply(ReadMobileIdentity(id)); continue; }
                    if (kind == "qr_png")
                    {
                        string text = root.GetProperty("text").GetString();
                        using var generator = new QRCodeGenerator();
                        using var qrData = generator.CreateQrCode(text, QRCodeGenerator.ECCLevel.M);
                        using var png = new PngByteQRCode(qrData);
                        Program.Reply(new { id, ok = true, png = Convert.ToBase64String(png.GetGraphic(6)) });
                        continue;
                    }
                    if (kind != "request") throw new ArgumentException("未知 IPC 操作");
                    string method = root.GetProperty("method").GetString();
                    string path = root.GetProperty("path").GetString();
                    if ((method != "GET" && method != "POST") || !Allowed(path))
                        throw new ArgumentException("此测试桥接不支持该请求");
                    string parameters = root.TryGetProperty("parameters", out var p) ? p.GetString() : "{}";
                    var headers = new Dictionary<string, string>();
                    if (root.TryGetProperty("headers", out var h) && h.ValueKind == JsonValueKind.Object)
                        foreach (var v in h.EnumerateObject()) headers[v.Name] = v.Value.GetString();
                    var callback = new Callback();
                    remainingMs = deadlineMs - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if (remainingMs <= 0) throw new TimeoutException("请求已过期，未调用 SDK");
                    dispatched = true;
                    requests.Request(new RequestInfo(method, path, headers, parameters ?? "{}"), callback);
                    var response = await callback.Task.WaitAsync(TimeSpan.FromMilliseconds(Math.Min(35000, remainingMs)));
                    Program.Reply(new { id, ok = response.Code == 0, code = response.Code,
                        error = response.Code == 0 ? "" : "原版引擎请求失败，错误码 " + response.Code,
                        body = response.Body });
                }
                catch (Exception e)
                {
                    Program.Reply(new { id, ok = false, not_executed = !dispatched, result_unknown = dispatched,
                        error = e.GetType().Name + ": " + e.Message });
                }
            }
        }
        finally
        {
            // 回调先唤醒托管等待者，原版原生回调栈随后才返回。
            // 留出退出窗口，避免在其回调栈仍活动时销毁全局引擎。
            await Task.Delay(300);
            engine.Destroy();
            GC.KeepAlive(adapter);
        }
        return 0;
    }

    private static bool Allowed(string path) => path == "/v1/account/tv/qrcode" ||
        path == "/v1/account/tv/qrcode/state" || path == "/v1/account/info" ||
        path == "/v2/account/logout" || path == "/v1/drive/alltokens" ||
        path == "/v1/api/wopan/query_files" || path == "/v1/api/wopan/query_file_info" ||
        path == "/v1/api/wopan/get_download_url" || path == "/v1/api/wopan/refresh_token" || path == "/v1/api/m139";

    private static object ReadMobileIdentity(long id)
    {
        using var stream = File.OpenRead(Path.Combine(Program.Sdk, "BaoMiHua.dll"));
        using var pe = new PEReader(stream);
        var reader = pe.GetMetadataReader();
        string appId = null, appSecret = null;
        foreach (var handle in reader.TypeDefinitions)
        {
            var type = reader.GetTypeDefinition(handle);
            if (reader.GetString(type.Name) != "NetDiskHelper" || reader.GetString(type.Namespace) != "Filmly.Helpers") continue;
            foreach (var f in type.GetFields())
            {
                var field = reader.GetFieldDefinition(f);
                string name = reader.GetString(field.Name);
                if (name != "Xd" && name != "XE") continue;
                var ch = field.GetDefaultValue();
                if (ch.IsNil) continue;
                var constant = reader.GetConstant(ch);
                if (constant.TypeCode != ConstantTypeCode.String) continue;
                string value = Encoding.Unicode.GetString(reader.GetBlobBytes(constant.Value));
                if (name == "Xd") appId = value; else appSecret = value;
            }
        }
        if (String.IsNullOrEmpty(appId) || String.IsNullOrEmpty(appSecret))
            throw new InvalidDataException("无法从该版本主程序集读取移动应用身份");
        return new { id, ok = true, app_id = appId, app_secret = appSecret };
    }
}
