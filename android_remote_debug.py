#!/usr/bin/env python3
"""Android read-only web diagnostics via exported Shizuku rish and a protected Quick Tunnel.

Run in Termux: python android_remote_debug.py
Requires Python, cloudflared, curl, and Shizuku's exported rish + rish_shizuku.dex.
No third-party Python packages. No remote arbitrary-command or settings-write endpoint.
"""
import concurrent.futures
import html
import os
from pathlib import Path
import queue
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

VERSION = "1.0"
RISH = None
GATE = threading.Lock()
LIMIT = 260_000


def run(argv, timeout=22):
    env = os.environ.copy()
    env.update(RISH_APPLICATION_ID="com.termux", RISH_PRESERVE_ENV="0")
    p = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, env=env, start_new_session=True)
    try:
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(p.pid, signal.SIGKILL)
        p.communicate()
        return b"", f"Timed out after {timeout}s".encode(), 124
    return out, err, p.returncode


def shell(command, timeout=22):
    if RISH is None:
        raise RuntimeError("Shizuku backend is not initialized")
    return run(["/system/bin/sh", str(RISH), "-c", command], timeout)


def redact(text):
    text = re.sub(r"(?i)(authorization\s*[:=]\s*(?:bearer|basic)\s+)\S+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)\b(password|passwd|token|secret|api[_-]?key|uuid)(\s*[:=]\s*)[^\s&;,]+", r"\1\2[REDACTED]", text)
    text = re.sub(r"(?i)\b(?:vmess|vless|trojan|ss|ssr)://\S+", "[PROXY URI REDACTED]", text)
    return text


def decoded(result):
    out, err, code = result
    raw = (out + b"\n" + err)
    text = raw[:LIMIT].decode("utf-8", "replace")
    if len(raw) > LIMIT:
        text += "\n[Output clipped]"
    return redact(text) + f"\n[exit={code}]"


# These strings are fixed in the program. HTTP input never becomes shell input.
COMMANDS = {
    "/overview": """id
date
getprop ro.product.model
getprop ro.build.version.release
pm list packages -U | grep -Ei 'clash|mihomo|v2ray|v2box|sing|neko|husi|hiddify|karing|surfboard|sager|shadow|anxray|vpn'
""",
    "/network": """echo '[Interfaces]'
ip addr show
echo '[IPv4 rules and all route tables]'
ip rule show
ip route show table all
echo '[IPv6 rules and all route tables]'
ip -6 rule show
ip -6 route show table all
""",
    "/vpn": """for k in private_dns_mode private_dns_specifier http_proxy; do
  echo "$k=$(settings get global "$k")"
done
for k in always_on_vpn_app always_on_vpn_lockdown; do
  echo "$k=$(settings get secure "$k")"
done
echo '[VPN service]'
dumpsys -t 6 vpn_management
echo '[Connectivity service]'
dumpsys -t 6 connectivity
""",
}
LINKS = [("设备与客户端", "/overview"), ("路由 / IPv6", "/network"),
         ("VPN / DNS", "/vpn"), ("连通性测试", "/probe"),
         ("相关系统日志", "/logs"), ("当前屏幕", "/screen.png")]


def probe():
    targets = [("Google", "https://www.google.com/generate_204"),
               ("Cloudflare", "https://www.cloudflare.com/cdn-cgi/trace")]
    tests = [(name, family, url) for name, url in targets for family in ("-4", "-6")]
    def one(test):
        name, family, url = test
        result = run(["curl", "--noproxy", "*", "-sS", family,
                      "--connect-timeout", "4", "--max-time", "8", "-o", "/dev/null",
                      "-w", "http=%{http_code} ip=%{remote_ip} dns=%{time_namelookup} tcp=%{time_connect} tls=%{time_appconnect} total=%{time_total}\n", url], 10)
        return f"{name} {family}\n{decoded(result)}"
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        parts = list(pool.map(one, tests))
    return ("测试由 Termux 发起；按应用分流可能使它与 Chrome 的路径不同。\n"
            "--noproxy 只忽略 HTTP/SOCKS 环境变量，不绕过 Android VPN。\n\n" + "\n\n".join(parts))


def logs():
    raw = decoded(shell("logcat -d -t 1500 -v time", 12))
    pattern = re.compile(r"ConnectivityService|NetworkMonitor|DnsResolver|netd|Vpn|tun2socks|sing.box|mihomo|clash|v2ray|TLS|handshake|QUIC|denied|exit=", re.I)
    return "\n".join(line for line in raw.splitlines() if pattern.search(line))[-60_000:] or "No matching system logs. Some clients keep their logs inside the app."


def page(title, body):
    nav = " ".join(f'<a href="{path}">{label}</a>' for label, path in LINKS)
    return ("<!doctype html><html lang=zh><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>"
            f"<title>{html.escape(title)}</title><style>body{{max-width:1000px;margin:24px auto;padding:0 16px;font:16px/1.6 sans-serif}}"
            "nav{display:flex;gap:12px;flex-wrap:wrap}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f3f5f7;padding:16px}"
            f"a{{color:#135dbc}}</style><h1>{html.escape(title)}</h1><nav>{nav}</nav>{body}</html>")


class Handler(BaseHTTPRequestHandler):
    def send(self, code, data, mime="text/html; charset=utf-8"):
        if isinstance(data, str):
            data = data.encode("utf-8")
        self.send_response(code)
        for k, v in {"Content-Type": mime, "Content-Length": str(len(data)), "Cache-Control": "no-store",
                     "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY",
                     "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"}.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/":
            self.send(200, page("Android 远程诊断", "<p>Shizuku 调试桥已启动。点击上方项目读取当前状态；每次点击都会重新采集。</p>"
                                "<p>仅有固定的只读检查入口，没有任意命令或修改系统设置的接口。</p>"
                                "<p>页面必须通过已启用邮箱验证的 Cloudflare 隧道访问。手机端 Ctrl+C 可结束本次会话。</p>"))
            return
        if path not in COMMANDS and path not in ("/probe", "/logs", "/screen.png"):
            self.send(404, "Not found")
            return
        if not GATE.acquire(blocking=False):
            self.send(429, "另一个检查正在进行，请稍后刷新。")
            return
        try:
            if path == "/screen.png":
                out, err, code = shell("screencap -p", 12)
                if code or not out.startswith(b"\x89PNG\r\n\x1a\n"):
                    self.send(503, html.escape(decoded((out[:200], err, code))))
                else:
                    self.send(200, out, "image/png")
                return
            result = probe() if path == "/probe" else logs() if path == "/logs" else decoded(shell(COMMANDS[path]))
            title = dict((p, label) for label, p in LINKS)[path]
            self.send(200, page(title, f"<p>采集时间：{time.strftime('%Y-%m-%d %H:%M:%S %z')}</p><pre>{html.escape(result)}</pre>"))
        except Exception as exc:
            self.send(503, html.escape(str(exc)))
        finally:
            GATE.release()

    def log_message(self, fmt, *args):
        # Do not log request headers, authentication codes, or query strings.
        pass


def import_rish(destination):
    roots = [Path.home() / "storage/shared/Documents", Path("/sdcard/Documents"),
             Path.home() / "storage/downloads", Path("/sdcard/Download")]
    for root in roots:
        if not root.is_dir():
            continue
        candidates = [root / "rish"] + list(root.glob("*/rish"))
        for source in candidates:
            dex = source.with_name("rish_shizuku.dex")
            if source.is_file() and dex.is_file():
                target = destination / "rish"
                shutil.copyfile(source, target)
                shutil.copyfile(dex, destination / dex.name)
                target.chmod(0o600)
                (destination / dex.name).chmod(0o400)
                return target
    raise RuntimeError("未找到 Shizuku 导出的两个文件。请在 Shizuku → Use Shizuku in terminal apps → 导出文件，选择 Documents 文件夹，并允许 Termux 访问文件。")


def main():
    global RISH
    if not Path("/system/bin/app_process").exists():
        raise RuntimeError("此启动程序需要在安卓 Termux 中运行。")
    for program in ("cloudflared", "curl"):
        if not shutil.which(program):
            raise RuntimeError(f"缺少依赖 {program}；请使用配套的整段启动命令。")
    help_out, help_err, help_code = run(["cloudflared", "tunnel", "--help"], 15)
    if help_code or b"--allowed-mail" not in help_out + help_err:
        raise RuntimeError("当前 cloudflared 不支持所需的邮箱访问保护，已停止；不会降级为公开隧道。")
    with tempfile.TemporaryDirectory(prefix="android-debug-", dir=str(Path.home())) as directory:
        private = Path(directory)
        RISH = import_rish(private)
        print("正在请求 Shizuku 权限；如果弹窗询问是否允许 Termux，请点允许。", flush=True)
        result = shell("id -u", 90)
        if result[2] or result[0].strip() not in (b"2000", b"0"):
            raise RuntimeError("Shizuku 权限验证失败：\n" + decoded(result))
        print("Shizuku 已连接，不需要 ADB 重新配对。", flush=True)
        mail = input("输入你能接收验证码的邮箱（仅允许这个邮箱访问调试网页）：").strip()
        if not re.fullmatch(r"[^\s,@*<>]+@[^\s,@*<>]+\.[^\s,@*<>]+", mail):
            raise RuntimeError("邮箱格式不正确；未启动隧道。")
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        config = private / "cloudflared.yml"
        config.write_text("{}\n")
        local_url = f"http://127.0.0.1:{server.server_port}"
        tunnel = None
        locked = False
        try:
            if shutil.which("termux-wake-lock"):
                locked = run(["termux-wake-lock"], 5)[2] == 0
            tunnel = subprocess.Popen(["cloudflared", "--no-autoupdate", "tunnel", "--config", str(config),
                                       "--protocol", "http2", "--url", local_url, "--allowed-mail", mail],
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                      text=True, start_new_session=True)
            events = queue.Queue()
            def read_output():
                for line in tunnel.stdout:
                    events.put(line)
            threading.Thread(target=read_output, daemon=True).start()
            print("正在建立受邮箱验证保护的 HTTPS 隧道……", flush=True)
            url, connected, recent = None, False, []
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if tunnel.poll() is not None:
                    raise RuntimeError("隧道进程已退出：\n" + "".join(recent[-15:]))
                try:
                    line = events.get(timeout=1)
                except queue.Empty:
                    continue
                recent.append(line)
                recent = recent[-30:]
                match = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com\b", line)
                if match:
                    url = match.group()
                if "Registered tunnel connection" in line:
                    connected = True
                if url and connected:
                    break
            if not url or not connected:
                raise RuntimeError("60 秒内未建立隧道；可能受到当前网络或代理影响。\n" + "".join(recent[-15:]))
            print("\n调试地址：" + url, flush=True)
            print("把上面这一行地址发给 ChatGPT。首次访问需要邮箱验证。", flush=True)
            print("保持 Termux 运行。Ctrl+C 随时停止；本次会话最长 2 小时。", flush=True)
            deadline = time.monotonic() + 7200
            while time.monotonic() < deadline and tunnel.poll() is None:
                try:
                    events.get(timeout=1)
                except queue.Empty:
                    pass
            if tunnel.poll() is not None:
                print("隧道已停止。如正在改代理设置，调试连接也可能受影响。", flush=True)
        finally:
            if tunnel is not None and tunnel.poll() is None:
                os.killpg(tunnel.pid, signal.SIGTERM)
                try:
                    tunnel.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(tunnel.pid, signal.SIGKILL)
                    tunnel.wait()
            server.shutdown()
            server.server_close()
            if locked:
                run(["termux-wake-unlock"], 5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n调试会话已停止。")
    except Exception as exc:
        print("\n启动未完成：" + str(exc), flush=True)
        raise SystemExit(1)
