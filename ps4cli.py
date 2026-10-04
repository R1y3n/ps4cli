#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ps4cli - pure-CLI PS4 toolbox: direct fPKG install + browsable file push/pull.

Two engines, one tool (see LEARNINGS.md for why they are built this way):

  1. PULL-INSTALL engine (install / serve / task / exists / uninstall)
     The PC runs a tiny zero-copy HTTP server with full Range support and tells
     the PS4's Remote Package Installer (RPI, port 12800) "install this URL".
     The PS4 itself pulls the package over HTTP and installs it in place -
     the .pkg is never copied to /data/pkg by us.  Same mechanism as
     ps4-remote-pkg-sender (the Electron app this tool was derived from).

  2. FTP engine (ls / cd / tree / get / put / rm / mv / cat ...)
     A parallel, resumable FTP client for browsing the PS4 filesystem and
     pushing / pulling files (GoldHEN / ps4-ftp FTP server, default port 2121).

Stdlib only.  Python >= 3.8.  Run `ps4cli.py` with no command for an
interactive shell, or `ps4cli.py <command> ...` for one-shot use.
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures as cf
import fnmatch
import ftplib
import glob
import json
import os
import posixpath
import re
import shlex
import shutil
import socket
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

__version__ = "1.0.0"

MiB = 1 << 20
BLOCK = 1 * MiB                  # FTP block size (big blocks = fewer syscalls)
SOCK_BUF = 4 * MiB               # socket buffers for bulk data
PART_SUFFIX = ".ps4cli-part"     # temp name used while a transfer is in flight
SEGMENT_MIN = 32 * MiB           # files smaller than this are never segmented

Entry = collections.namedtuple("Entry", "name kind size mtime perms")  # kind: d|f|l


class CLIError(Exception):
    """User-facing error (printed without traceback)."""


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
def human(n, per_sec=False):
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            s = f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
            return s + ("/s" if per_sec else "")
        n /= 1024


def fmt_dur(sec):
    if sec is None or sec < 0 or sec != sec:
        return "--:--"
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def toint(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def term_width():
    return shutil.get_terminal_size((100, 20)).columns


def eprint(*a):
    print(*a, file=sys.stderr)


def confirm(msg, assume_yes=False):
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        raise CLIError(f"{msg} -- refusing without a terminal; pass -y to confirm")
    return input(f"{msg} [y/N] ").strip().lower() in ("y", "yes")


# --------------------------------------------------------------------------- #
# configuration  (CLI flag > environment > ~/.config/ps4cli.json > default)
# --------------------------------------------------------------------------- #
DEFAULTS = {
    "ip": None,
    "rpi_port": 12800,
    "ftp_port": 2121,
    "user": "anonymous",
    "password": "anonymous@",
    "jobs": 4,
    "http_port": 8080,
    "host": None,           # PC address the PS4 should use (auto-detected)
    "timeout": 30,
}
ENV_MAP = {"ip": "PS4_IP", "rpi_port": "PS4_RPI_PORT", "ftp_port": "PS4_FTP_PORT",
           "user": "PS4_FTP_USER", "password": "PS4_FTP_PASS", "host": "PS4CLI_HOST"}
CONFIG_PATH = os.path.expanduser(os.environ.get("PS4CLI_CONFIG", "~/.config/ps4cli.json"))


def load_file_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def build_config(args):
    cfg = dict(DEFAULTS)
    cfg.update({k: v for k, v in load_file_config().items() if k in DEFAULTS})
    for key, env in ENV_MAP.items():
        if os.environ.get(env):
            cfg[key] = os.environ[env]
    for key in DEFAULTS:
        v = getattr(args, key, None)
        if v is not None:
            cfg[key] = v
    for k in ("rpi_port", "ftp_port", "jobs", "http_port", "timeout"):
        cfg[k] = toint(cfg[k], DEFAULTS[k])
    return cfg


# --------------------------------------------------------------------------- #
# progress meter (thread-safe, ~4 Hz on a TTY)
# --------------------------------------------------------------------------- #
class Meter:
    def __init__(self, total_bytes, total_files, label=""):
        self.total = total_bytes
        self.files = total_files
        self.label = label
        self.done = 0
        self.files_done = 0
        self.skipped = 0
        self.resumed = 0
        self.lock = threading.Lock()
        self.t0 = time.monotonic()
        self.samples = collections.deque([(self.t0, 0)], maxlen=40)
        self._stop = threading.Event()
        self._thread = None
        self._tty = sys.stderr.isatty()
        self._last_len = 0

    def add(self, n):
        with self.lock:
            self.done += n

    def file_done(self):
        with self.lock:
            self.files_done += 1

    def rate(self):
        now = time.monotonic()
        with self.lock:
            self.samples.append((now, self.done))
            t_old, d_old = self.samples[0]
            for t, d in self.samples:      # ~3 s window
                if now - t <= 3:
                    t_old, d_old = t, d
                    break
            done = self.done
        dt = now - t_old
        return (done - d_old) / dt if dt > 0 else 0.0

    def line(self):
        r = self.rate()
        with self.lock:
            done, fd = self.done, self.files_done
        pct = (done / self.total * 100) if self.total else 100.0
        eta = (self.total - done) / r if r > 1 else None
        width = 20
        fill = int(width * min(pct, 100) / 100)
        bar = "#" * fill + "-" * (width - fill)
        return (f"{self.label}[{bar}] {pct:5.1f}%  {human(done)}/{human(self.total)}  "
                f"{human(r, True)}  ETA {fmt_dur(eta)}  files {fd}/{self.files}")

    def _loop(self):
        interval = 0.25 if self._tty else 5.0
        while not self._stop.wait(interval):
            self._print(self.line())

    def _print(self, s, final=False):
        if self._tty:
            s = s[:term_width() - 1]
            pad = " " * max(0, self._last_len - len(s))
            sys.stderr.write("\r" + s + pad + ("\n" if final else ""))
            self._last_len = len(s)
        else:
            sys.stderr.write(s + "\n")
        sys.stderr.flush()

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)
        dt = max(time.monotonic() - self.t0, 1e-6)
        self._print(f"{self.label}done: {self.files_done}/{self.files} files, {human(self.done)} "
                    f"in {fmt_dur(dt)} (avg {human(self.done / dt, True)})"
                    + (f", {self.skipped} skipped (already complete)" if self.skipped else "")
                    + (f", {self.resumed} resumed" if self.resumed else ""), final=True)


# --------------------------------------------------------------------------- #
# ENGINE 1a - zero-copy HTTP server with full Range support
# --------------------------------------------------------------------------- #
def parse_ranges(header, size):
    """Parse an HTTP Range header.
    Returns: None  -> ignore header, serve 200 full body
             'unsat' -> 416
             [(start, end_inclusive), ...] -> 206 (single or multipart)"""
    m = re.match(r"^\s*bytes\s*=\s*(.+)$", header or "", re.I)
    if not m:
        return None
    out = []
    for part in m.group(1).split(","):
        part = part.strip()
        if not part:
            continue
        a, sep, b = part.partition("-")
        a, b = a.strip(), b.strip()
        if not sep:
            return None
        if a == "":                              # suffix range: last N bytes
            if not b.isdigit():
                return None
            n = int(b)
            if n == 0:
                continue
            out.append((max(size - n, 0), size - 1))
        else:
            if not a.isdigit() or (b and not b.isdigit()):
                return None
            s = int(a)
            if s >= size:                        # start beyond EOF -> unsatisfiable (check first!)
                continue
            e = int(b) if b else size - 1
            if e < s:
                return None
            out.append((s, min(e, size - 1)))
    return out if out else "unsat"


class PkgHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"          # keep-alive: the PS4 opens many ranged requests
    server_version = "ps4cli-pkgserver/1.0"

    def setup(self):
        super().setup()
        try:
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCK_BUF)
        except OSError:
            pass

    def log_message(self, fmt, *args):
        if self.server.verbose:
            eprint(f"[http {self.client_address[0]}] {fmt % args}  range={self.headers.get('Range')}")

    def do_HEAD(self):
        self._handle(head=True)

    def do_GET(self):
        self._handle(head=False)

    def _simple(self, code, body, ctype="text/plain; charset=utf-8", head=False, extra=None):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if not head:
            self.wfile.write(data)

    def _handle(self, head):
        try:
            self._serve(head)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError):
            self.close_connection = True
        except OSError as e:
            if self.server.verbose:
                eprint(f"[http] I/O error: {e}")
            self.close_connection = True

    def _serve(self, head):
        path = unquote(urlparse(self.path).path)
        srv = self.server
        if path == "/hb":
            body = json.dumps({"message": "ps4cli heartbeat ok",
                               "remoteAddress": self.client_address[0],
                               "files": len(srv.files)})
            return self._simple(200, body, "application/json", head)
        if path in ("", "/"):
            lines = [f"/{k}\t{human(os.path.getsize(v))}" for k, v in sorted(srv.files.items())]
            return self._simple(200, "\n".join(lines) + "\n", head=head)

        fpath = srv.files.get(path.lstrip("/"))
        if not fpath:
            return self._simple(404, "not found\n", head=head)
        try:
            st = os.stat(fpath)
        except OSError:
            return self._simple(404, "file vanished\n", head=head)
        size = st.st_size
        base = {"Accept-Ranges": "bytes",
                "Last-Modified": formatdate(st.st_mtime, usegmt=True),
                "ETag": f'"{size:x}-{int(st.st_mtime):x}"'}

        ranges = parse_ranges(self.headers.get("Range"), size) if self.headers.get("Range") else None
        if ranges == "unsat":
            return self._simple(416, "range not satisfiable\n", head=head,
                                extra={**base, "Content-Range": f"bytes */{size}"})

        ctype = "application/octet-stream"
        if ranges is None:                                           # 200 full body
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(size))
            for k, v in base.items():
                self.send_header(k, v)
            self.end_headers()
            if not head and size:
                with open(fpath, "rb") as f:
                    self._sendfile(f, 0, size)
            return

        if len(ranges) == 1:                                         # 206 single range
            s, e = ranges[0]
            self.send_response(206)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(e - s + 1))
            self.send_header("Content-Range", f"bytes {s}-{e}/{size}")
            for k, v in base.items():
                self.send_header(k, v)
            self.end_headers()
            if not head:
                with open(fpath, "rb") as f:
                    self._sendfile(f, s, e - s + 1)
            return

        # 206 multipart/byteranges (RFC 7233).  Some PS4 requests need this.
        boundary = "ps4cli" + os.urandom(8).hex()
        parts, total = [], 0
        for s, e in ranges:
            hdr = (f"--{boundary}\r\nContent-Type: {ctype}\r\n"
                   f"Content-Range: bytes {s}-{e}/{size}\r\n\r\n").encode()
            parts.append((hdr, s, e))
            total += len(hdr) + (e - s + 1) + 2
        closing = f"--{boundary}--\r\n".encode()
        total += len(closing)
        self.send_response(206)
        self.send_header("Content-Type", f"multipart/byteranges; boundary={boundary}")
        self.send_header("Content-Length", str(total))
        for k, v in base.items():
            self.send_header(k, v)
        self.end_headers()
        if head:
            return
        with open(fpath, "rb") as f:
            for hdr, s, e in parts:
                self.wfile.write(hdr)
                self._sendfile(f, s, e - s + 1)
                self.wfile.write(b"\r\n")
            self.wfile.write(closing)

    def _sendfile(self, f, start, length):
        """Kernel-side file->socket copy (sendfile(2)); no user-space buffer copy."""
        sock, off, rem = self.connection, start, length
        stats = self.server.stats
        while rem > 0:
            sent = sock.sendfile(f, off, min(rem, 16 * MiB))
            if not sent:
                raise ConnectionError("short sendfile")
            off += sent
            rem -= sent
            with stats["lock"]:
                stats["bytes"] += sent


class _HTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128


class PkgServer:
    """Registers local files under sanitised URL paths and serves them."""

    def __init__(self, bind="0.0.0.0", port=8080, verbose=False):
        self.bind, self.port, self.verbose = bind, port, verbose
        self.files = {}                       # url path (no leading /) -> local path
        self.stats = {"lock": threading.Lock(), "bytes": 0}
        self.httpd = None

    def add(self, path):
        name = re.sub(r"[^a-zA-Z0-9_.-]", "", os.path.basename(path)) or "file.pkg"
        stem, ext = os.path.splitext(name)
        key, i = name, 1
        while key in self.files and self.files[key] != path:
            i += 1
            key = f"{stem}_{i}{ext}"
        self.files[key] = os.path.abspath(path)
        return key

    def start(self):
        last = None
        for p in range(self.port, self.port + 20):
            try:
                self.httpd = _HTTPServer((self.bind, p), PkgHandler)
                self.port = p
                break
            except OSError as e:
                last = e
        else:
            raise CLIError(f"cannot bind an HTTP port starting at {self.port}: {last}")
        self.httpd.files = self.files
        self.httpd.stats = self.stats
        self.httpd.verbose = self.verbose
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return self

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()

    def served(self):
        with self.stats["lock"]:
            return self.stats["bytes"]


def local_ip_towards(target_ip):
    """The PC address the PS4 will see (UDP connect sends no packets)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((target_ip, 9))
        return s.getsockname()[0]
    except OSError:
        return None
    finally:
        s.close()


# --------------------------------------------------------------------------- #
# ENGINE 1b - Remote Package Installer (RPI) client + PKG header reader
# --------------------------------------------------------------------------- #
RPI_ERRORS = {   # taken from ps4-remote-pkg-sender (ps4.js + Troubleshoot.md)
    2157510681: "task doesn't exist",
    2157510663: "already installed (delete the copy on the PS4 first)",
    2157510677: "already installed / duplicate (delete app + chunks first)",
    2157510789: "not enough storage",
    2157510920: "unknown (undocumented)",
    2143223530: "Ethernet/WiFi connection issue",
    -2135809020: "PKG URL returned 404 - the PS4 cannot reach/see the file on this server",
}


class RPIError(Exception):
    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


class RPI:
    """Client for flatZ's Remote Package Installer HTTP API (default port 12800).

    Note: RPI expects the JSON in the request body, and the original sender posts
    it with Content-Type application/x-www-form-urlencoded (what `curl --data`
    does).  We do exactly the same."""

    def __init__(self, ip, port=12800, timeout=10):
        self.base = f"http://{ip}:{port}"
        self.timeout = timeout

    def call(self, endpoint, payload, timeout=None):
        body = json.dumps(payload).encode()
        req = urllib.request.Request(f"{self.base}/api/{endpoint}", data=body, method="POST",
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
            raise RPIError(f"cannot reach RPI at {self.base} ({e}). Is Remote Package Installer "
                           f"running on the PS4 (HEN/GoldHEN loaded)?")
        try:
            data = json.loads(raw.decode("utf-8", "replace"))
        except ValueError:
            raise RPIError(f"non-JSON reply from RPI: {raw[:200]!r}")
        if isinstance(data, dict) and data.get("status") == "fail":
            code = data.get("error_code")
            code_i = toint(code, None) if code is not None else None
            raise RPIError(f"RPI error {code}: {RPI_ERRORS.get(code_i, 'see Troubleshoot.md')}", code_i)
        return data

    # -- API (all endpoints documented in the header comment of ps4.js) --
    def check(self):
        return self.call("is_exists", {"title_id": "CUSA00000"})

    def is_exists(self, title_id):
        d = self.call("is_exists", {"title_id": title_id})
        return str(d.get("exists")).lower() == "true", d.get("size")

    def install(self, urls, timeout=120):
        # RPI fetches headers from our server *before* answering -> long timeout.
        return self.call("install", {"type": "direct", "packages": list(urls)}, timeout=timeout)

    def task(self, action, task_id):
        ep = {"info": "get_task_progress", "pause": "pause_task", "resume": "resume_task",
              "stop": "stop_task", "remove": "unregister_task", "start": "start_task"}[action]
        return self.call(ep, {"task_id": int(task_id)})

    def find_task(self, content_id, sub_type=6):
        return self.call("find_task", {"content_id": content_id, "sub_type": sub_type})

    def uninstall(self, kind, ident):
        ep, key = {"game": ("uninstall_game", "title_id"), "patch": ("uninstall_patch", "title_id"),
                   "ac": ("uninstall_ac", "content_id"), "theme": ("uninstall_theme", "content_id")}[kind]
        return self.call(ep, {key: ident})


def read_pkg_info(path):
    """Read the PS4 PKG header + PARAM.SFO.  Raises ValueError for non-PKG files.
    Offsets follow the public PS4 PKG documentation (see LEARNINGS.md)."""
    with open(path, "rb") as f:
        h = f.read(0x80)
        if len(h) < 0x80 or h[:4] != b"\x7fCNT":
            raise ValueError("not a PS4 PKG (bad magic)")
        be32 = lambda b, o: struct.unpack(">I", b[o:o + 4])[0]
        info = {"content_id": h[0x40:0x64].split(b"\0")[0].decode("ascii", "replace"),
                "drm_type": be32(h, 0x70), "content_type": be32(h, 0x74),
                "content_flags": be32(h, 0x78), "sfo": {}}
        entry_count, table_off = be32(h, 0x10), be32(h, 0x18)
        try:
            f.seek(table_off)
            table = f.read(entry_count * 32)
            for i in range(min(entry_count, 4096)):
                eid, _fn, _f1, _f2, off, size = struct.unpack(">6I", table[i * 32:i * 32 + 24])
                if eid == 0x1000 and 0 < size < 1 << 20:      # PARAM_SFO
                    f.seek(off)
                    info["sfo"] = parse_sfo(f.read(size))
                    break
        except (struct.error, OSError):
            pass
    m = re.search(r"(CUSA\d{5})", info["sfo"].get("TITLE_ID", "") or info["content_id"] or os.path.basename(path), re.I)
    info["title_id"] = (info["sfo"].get("TITLE_ID") or (m.group(1).upper() if m else ""))
    return info


def parse_sfo(b):
    if b[:4] != b"\x00PSF":
        return {}
    _ver, key_off, data_off, count = struct.unpack("<4xIIII", b[:20])
    out = {}
    for i in range(min(count, 512)):
        ko, fmt, ln, _mx, do = struct.unpack("<HHIII", b[20 + i * 16:36 + i * 16])
        key = b[key_off + ko:b.index(b"\0", key_off + ko)].decode("ascii", "replace")
        raw = b[data_off + do:data_off + do + ln]
        out[key] = (struct.unpack("<I", raw[:4])[0] if fmt == 0x0404
                    else raw.split(b"\0")[0].decode("utf-8", "replace"))
    return out


CATEGORY = {"gd": "game", "gp": "patch", "ac": "additional content", "gdc": "game", "gde": "game",
            "gda": "game", "ap": "app"}


# --------------------------------------------------------------------------- #
# ENGINE 2 - FTP client: browsing + parallel / resumable transfers
# --------------------------------------------------------------------------- #
class PS4FTP(ftplib.FTP):
    """ftplib.FTP tuned for bulk LAN transfers (big socket buffers, utf-8, binary)."""

    def __init__(self, host, port, user, password, timeout):
        super().__init__(encoding="utf-8")
        self.connect(host, port, timeout)
        self.login(user, password)
        self.voidcmd("TYPE I")
        try:
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

    def ntransfercmd(self, cmd, rest=None):
        conn, size = super().ntransfercmd(cmd, rest)
        for opt in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                conn.setsockopt(socket.SOL_SOCKET, opt, SOCK_BUF)
            except OSError:
                pass
        return conn, size

    def safe_size(self, path):
        try:
            return self.size(path)
        except ftplib.error_perm:
            return None


class Remote:
    """Remote filesystem view over FTP.  The *current directory* is tracked
    client-side so that every command uses absolute paths and every pooled
    connection stays independent."""

    CACHE_TTL = 4.0

    def __init__(self, cfg):
        self.cfg = cfg
        self.cwd = "/"
        self._conn = None
        self._cache = {}
        self._mlsd_ok = None
        self._dirs_ok = set()      # dirs we already created/verified this session

    # connection management
    def new_conn(self):
        c = self.cfg
        try:
            return PS4FTP(c["ip"], c["ftp_port"], c["user"], c["password"], c["timeout"])
        except (OSError, EOFError) as e:
            raise CLIError(f"FTP connect to {c['ip']}:{c['ftp_port']} failed: {e}\n"
                           f"  Is an FTP server running on the PS4 (GoldHEN FTP, default port 2121)?")
        except ftplib.error_perm as e:
            raise CLIError(f"FTP login refused: {e}")

    @property
    def conn(self):
        if self._conn is None:
            self._conn = self.new_conn()
        return self._conn

    def close(self):
        if self._conn:
            try:
                self._conn.quit()
            except Exception:
                try:
                    self._conn.close()
                except Exception:
                    pass
            self._conn = None

    def _retry(self, fn):
        try:
            return fn(self.conn)
        except (EOFError, ConnectionError, socket.timeout, ftplib.error_temp):
            self.close()
            return fn(self.conn)

    # paths
    def abspath(self, p):
        if not p:
            return self.cwd
        return posixpath.normpath(posixpath.join(self.cwd, p))

    # listing
    def listdir(self, path, fresh=False):
        path = self.abspath(path)
        hit = self._cache.get(path)
        if hit and not fresh and time.time() - hit[0] < self.CACHE_TTL:
            return hit[1]

        def run(c):
            c.cwd(path)
            ents = None
            if self._mlsd_ok is not False:
                try:
                    ents = self._mlsd(c)
                    self._mlsd_ok = True
                except ftplib.error_perm as e:
                    if str(e)[:3] in ("500", "501", "502", "504"):
                        self._mlsd_ok = False
                    else:
                        raise
            if ents is None:
                ents = self._list(c)
            return ents

        ents = self._retry(run)
        ents.sort(key=lambda e: (e.kind != "d", e.name.lower()))
        self._cache[path] = (time.time(), ents)
        return ents

    @staticmethod
    def _mlsd(c):
        out = []
        for name, f in c.mlsd(facts=["type", "size", "modify", "perm"]):
            t = f.get("type", "").lower()
            if t in ("cdir", "pdir") or name in (".", ".."):
                continue
            kind = "d" if t == "dir" else "l" if "symlink" in t else "f"
            m = f.get("modify", "")
            mt = f"{m[0:4]}-{m[4:6]}-{m[6:8]} {m[8:10]}:{m[10:12]}" if len(m) >= 12 else ""
            out.append(Entry(name, kind, toint(f.get("size")), mt, f.get("perm", "")))
        return out

    @staticmethod
    def _list(c):
        lines = []
        c.retrlines("LIST", lines.append)
        out = []
        for ln in lines:
            parts = ln.split(None, 8)
            if len(parts) < 9 or not re.match(r"^[-dlbcps][-rwxsStTl]{8,9}", parts[0]):
                continue
            name = parts[8]
            kind = "d" if parts[0][0] == "d" else "l" if parts[0][0] == "l" else "f"
            if kind == "l" and " -> " in name:
                name = name.split(" -> ")[0]
            if name in (".", ".."):
                continue
            out.append(Entry(name, kind, toint(parts[4]), " ".join(parts[5:8]), parts[0]))
        return out

    def stat(self, path):
        path = self.abspath(path)
        if path == "/":
            return Entry("/", "d", 0, "", "")
        parent, name = posixpath.split(path)
        try:
            for e in self.listdir(parent):
                if e.name == name:
                    return e
        except ftplib.error_perm:
            pass
        return None

    def glob(self, pattern):
        """Expand * ? [] in the last path component."""
        pattern = self.abspath(pattern)
        parent, base = posixpath.split(pattern)
        if not re.search(r"[*?\[]", base):
            return [pattern]
        try:
            names = [e.name for e in self.listdir(parent)]
        except ftplib.error_perm:
            return []
        return [posixpath.join(parent, n) for n in names if fnmatch.fnmatchcase(n, base)]

    def walk(self, root):
        """Yield (path, Entry) for everything below root (symlinks are skipped)."""
        stack = [root]
        while stack:
            d = stack.pop()
            for e in self.listdir(d, fresh=True):
                p = posixpath.join(d, e.name)
                if e.kind == "l":
                    eprint(f"warning: skipping symlink {p}")
                    continue
                yield p, e
                if e.kind == "d":
                    stack.append(p)

    # mutations
    def invalidate(self):
        self._cache.clear()

    def mkdir_p(self, path):
        path = self.abspath(path)
        cur = ""
        for part in [p for p in path.split("/") if p]:
            cur += "/" + part
            if cur in self._dirs_ok:
                continue
            try:
                self.conn.mkd(cur)
            except ftplib.error_perm:
                pass              # most likely "already exists"; real failures surface on use
            self._dirs_ok.add(cur)
        self.invalidate()

    def remove(self, path, recursive=False):
        path = self.abspath(path)
        e = self.stat(path)
        if e is None:
            raise CLIError(f"no such file or directory: {path}")
        if e.kind == "d":
            if not recursive:
                raise CLIError(f"{path} is a directory (use -r)")
            n = 0
            for ent in self.listdir(path, fresh=True):
                n += self.remove(posixpath.join(path, ent.name), True)
            self.conn.rmd(path)
            self._dirs_ok.discard(path)
            self.invalidate()
            return n + 1
        self.conn.delete(path)
        self.invalidate()
        return 1

    def rename(self, a, b):
        self.conn.rename(self.abspath(a), self.abspath(b))
        self.invalidate()


# ---- transfer tasks ------------------------------------------------------- #
class _Stop(Exception):
    """Raised from a data callback to end a segmented download early."""


class Ctx:
    """Per-thread connection holder for the transfer pool."""

    def __init__(self, remote):
        self.remote = remote
        self.local = threading.local()
        self.all = []
        self.lock = threading.Lock()

    def conn(self):
        c = getattr(self.local, "c", None)
        if c is None:
            c = self.remote.new_conn()
            self.local.c = c
            with self.lock:
                self.all.append(c)
        return c

    def discard(self):
        c = getattr(self.local, "c", None)
        self.local.c = None
        if c:
            try:
                c.close()
            except Exception:
                pass

    def close_all(self):
        for c in self.all:
            try:
                c.close()
            except Exception:
                pass


class GetTask:
    def __init__(self, remote, local, size, force):
        self.remote, self.local, self.size, self.force = remote, local, size, force
        self.name = remote

    def run(self, ctx, meter):
        part = self.local + PART_SUFFIX
        if not self.force and os.path.isfile(self.local) and os.path.getsize(self.local) == self.size:
            meter.add(self.size)
            meter.file_done()
            return "skipped"
        rest = 0
        if not self.force and os.path.isfile(part):
            ps = os.path.getsize(part)
            if 0 < ps <= self.size:
                rest = ps
        c = ctx.conn()
        meter.add(rest)
        try:
            self._fetch(c, part, rest, meter)
        except (ftplib.error_reply, ftplib.error_perm):
            if not rest:
                raise
            meter.add(-rest)                           # server refused REST -> restart from 0
            self._fetch(c, part, 0, meter)
        got = os.path.getsize(part)
        if got != self.size:
            raise IOError(f"size mismatch for {self.remote}: got {got}, expected {self.size}")
        os.replace(part, self.local)
        meter.file_done()
        return "resumed" if rest else "ok"

    def _fetch(self, c, part, rest, meter):
        if rest and rest == self.size:
            return
        with open(part, "ab" if rest else "wb") as f:
            def cb(b):
                f.write(b)
                meter.add(len(b))
            c.retrbinary("RETR " + self.remote, cb, BLOCK, rest or None)


class SegState:
    def __init__(self, nseg):
        self.left = nseg
        self.lock = threading.Lock()


class SegTask:
    """One byte-range of a big file, downloaded on its own connection (REST)."""

    def __init__(self, remote, part, final, start, end, total, state):
        self.remote, self.part, self.final = remote, part, final
        self.start, self.end, self.total, self.state = start, end, total, state
        self.size = end - start
        self.name = f"{remote} [{start}-{end})"

    def run(self, ctx, meter):
        c = ctx.conn()
        remaining = self.end - self.start
        to_eof = self.end >= self.total
        with open(self.part, "r+b") as f:
            f.seek(self.start)

            def cb(b):
                nonlocal remaining
                if not to_eof and len(b) >= remaining:
                    f.write(b[:remaining])
                    meter.add(remaining)
                    remaining = 0
                    raise _Stop()
                f.write(b)
                meter.add(len(b))
                remaining -= len(b)
            try:
                c.retrbinary("RETR " + self.remote, cb, BLOCK, self.start or None)
            except _Stop:
                ctx.discard()          # control channel still owes a 226/426 -> drop connection
        if remaining > 0 and not to_eof:
            raise IOError(f"short segment for {self.remote}")
        with self.state.lock:
            self.state.left -= 1
            last = self.state.left == 0
        if last:
            if os.path.getsize(self.part) != self.total:
                raise IOError(f"size mismatch for {self.remote}")
            os.replace(self.part, self.final)
            meter.file_done()
        return "ok"


class PutTask:
    def __init__(self, local, remote, size, force):
        self.local, self.remote, self.size, self.force = local, remote, size, force
        self.name = local

    def run(self, ctx, meter):
        c = ctx.conn()
        rsize = c.safe_size(self.remote)
        if not self.force and rsize == self.size:
            meter.add(self.size)
            meter.file_done()
            return "skipped"
        part = self.remote + PART_SUFFIX
        psz = c.safe_size(part)
        rest = psz if (not self.force and psz and 0 < psz < self.size) else 0
        try:
            self._send(c, part, rest, meter)
        except (ftplib.error_reply, ftplib.error_perm):
            if not rest:
                raise
            meter.add(-rest)
            self._send(c, part, 0, meter)
        if rsize is not None:
            try:
                c.delete(self.remote)
            except ftplib.error_perm:
                pass
        c.rename(part, self.remote)
        final = c.safe_size(self.remote)
        if final is not None and final != self.size:
            raise IOError(f"size mismatch after upload of {self.remote}: {final} != {self.size}")
        meter.file_done()
        return "resumed" if rest else "ok"

    def _send(self, c, part, rest, meter):
        with open(self.local, "rb") as f:
            if rest:
                f.seek(rest)
                meter.add(rest)
            c.storbinary("STOR " + part, f, BLOCK, lambda b: meter.add(len(b)), rest or None)


def run_transfers(remote, tasks, jobs, label):
    """Run tasks on a pool of independent FTP connections with one shared meter."""
    if not tasks:
        print("nothing to transfer")
        return 0
    total = sum(t.size for t in tasks)
    nfiles = sum(1 for t in tasks if not isinstance(t, SegTask)) + \
        len({t.final for t in tasks if isinstance(t, SegTask)})
    tasks = sorted(tasks, key=lambda t: -t.size)       # big first -> no long tail
    meter = Meter(total, nfiles, label).start()
    ctx = Ctx(remote)
    failures = []

    def work(t):
        for attempt in (1, 2):
            try:
                res = t.run(ctx, meter)
                if res in ("skipped", "resumed"):
                    with meter.lock:
                        setattr(meter, res, getattr(meter, res) + 1)
                return res
            except (EOFError, ConnectionError, socket.timeout, ftplib.error_temp) as e:
                ctx.discard()
                if attempt == 2:
                    raise
            except _Stop:
                ctx.discard()

    try:
        with cf.ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
            futs = {ex.submit(work, t): t for t in tasks}
            for fut in cf.as_completed(futs):
                try:
                    fut.result()
                except Exception as e:                  # keep going; report at the end
                    failures.append((futs[fut].name, e))
    except KeyboardInterrupt:
        meter.stop()
        ctx.close_all()
        raise CLIError("interrupted (partial files are kept as *%s and will resume)" % PART_SUFFIX)
    meter.stop()
    ctx.close_all()
    for name, e in failures:
        eprint(f"FAILED {name}: {e}")
    return len(failures)


# --------------------------------------------------------------------------- #
# argparse that raises instead of exiting (so the shell survives typos)
# --------------------------------------------------------------------------- #
class ParserExit(Exception):
    def __init__(self, status=0):
        self.status = status


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise CLIError(f"{self.prog}: {message}  (try `{self.prog} -h`)")

    def exit(self, status=0, message=None):
        if message:
            eprint(message)
        raise ParserExit(status)


# --------------------------------------------------------------------------- #
# the application: one method per command
# --------------------------------------------------------------------------- #
class App:
    def __init__(self, cfg):
        self.cfg = cfg
        self._remote = None
        self._rpi = None
        self.interactive = False
        self.parsers = {}
        self._build_parsers()

    # ---- lazily-created clients
    def need_ip(self):
        if not self.cfg["ip"]:
            raise CLIError("PS4 IP not set. Use --ip 192.168.x.y, env PS4_IP, or "
                           "`ps4cli.py config set ip 192.168.x.y`")
        return self.cfg["ip"]

    @property
    def remote(self):
        if self._remote is None:
            self.need_ip()
            self._remote = Remote(self.cfg)
        return self._remote

    @property
    def rpi(self):
        if self._rpi is None:
            self._rpi = RPI(self.need_ip(), self.cfg["rpi_port"], timeout=10)
        return self._rpi

    # ---- parser registry
    def _sub(self, name, help_, fn, aliases=()):
        p = Parser(prog=name, description=help_, formatter_class=argparse.RawDescriptionHelpFormatter)
        p.set_defaults(_fn=fn)
        p.help_text = help_.splitlines()[0]
        self.parsers[name] = p
        for a in aliases:
            self.parsers[a] = p
        return p

    def _build_parsers(self):
        s = self._sub
        p = s("ls", "List a remote directory.", self.cmd_ls, ["dir"])
        p.add_argument("path", nargs="*", default=["."])
        p.add_argument("-l", "--long", action="store_true", help="long format (type, size, date)")
        p.add_argument("--bytes", action="store_true", help="exact byte sizes in long format")

        p = s("cd", "Change remote directory (client-side; no state on the PS4).", self.cmd_cd)
        p.add_argument("path", nargs="?", default="/")

        s("pwd", "Print remote directory.", self.cmd_pwd)

        p = s("tree", "Recursive tree view of a remote directory.", self.cmd_tree)
        p.add_argument("path", nargs="?", default=".")
        p.add_argument("-L", "--depth", type=int, default=3, help="max depth (default 3)")
        p.add_argument("-d", "--dirs-only", action="store_true")
        p.add_argument("-s", "--sizes", action="store_true", help="show sizes")

        p = s("du", "Disk usage of remote files/dirs (recursive).", self.cmd_du)
        p.add_argument("path", nargs="*", default=["."])
        p.add_argument("-s", "--summary", action="store_true", help="only totals")

        p = s("find", "Find remote entries by name pattern (recursive).", self.cmd_find)
        p.add_argument("path", nargs="?", default=".")
        p.add_argument("-name", "--name", default="*", help="glob, e.g. '*.pkg'")
        p.add_argument("-type", "--type", choices=["f", "d"], help="restrict to files/dirs")

        p = s("stat", "Show info about a remote path.", self.cmd_stat)
        p.add_argument("path")

        p = s("cat", "Print (or hex-dump) a remote file.", self.cmd_cat, ["view"])
        p.add_argument("path")
        p.add_argument("-n", "--bytes", type=int, default=0, help="only the first N bytes")
        p.add_argument("-x", "--hex", action="store_true", help="hex dump (default limit 4096 bytes)")

        p = s("get", "Download remote files/dirs (parallel, resumable).\n\n"
                     "  get REMOTE... [LOCAL_DEST]\n"
                     "Globs allowed (e.g. '/data/pkg/*.pkg'). Directories are fetched recursively.\n"
                     "Interrupted transfers leave *.ps4cli-part files and resume next time.",
              self.cmd_get, ["pull", "download"])
        p.add_argument("paths", nargs="+")
        p.add_argument("-j", "--jobs", type=int, help="parallel connections (default from config, 4)")
        p.add_argument("--segments", type=int, default=1,
                       help="split each big file (>=32 MiB) into N parallel ranges (no resume)")
        p.add_argument("-f", "--force", action="store_true", help="overwrite / ignore partials")

        p = s("put", "Upload local files/dirs (parallel, resumable).\n\n"
                     "  put LOCAL... [REMOTE_DEST]\n"
                     "Globs allowed. Directories are uploaded recursively.\n"
                     "Uploads go to <name>.ps4cli-part and are renamed when complete.",
              self.cmd_put, ["push", "upload"])
        p.add_argument("paths", nargs="+")
        p.add_argument("-j", "--jobs", type=int)
        p.add_argument("-f", "--force", action="store_true")

        p = s("rm", "Delete remote files/dirs.", self.cmd_rm, ["del"])
        p.add_argument("paths", nargs="+")
        p.add_argument("-r", "--recursive", action="store_true")
        p.add_argument("-y", "--yes", action="store_true", help="do not ask")

        p = s("mkdir", "Create remote directories (parents created as needed).", self.cmd_mkdir)
        p.add_argument("paths", nargs="+")

        p = s("mv", "Rename/move a remote path.", self.cmd_mv, ["rename"])
        p.add_argument("src")
        p.add_argument("dst")

        # local helpers (shell)
        s("lpwd", "Print local directory.", self.cmd_lpwd)
        p = s("lcd", "Change local directory.", self.cmd_lcd)
        p.add_argument("path", nargs="?", default=os.path.expanduser("~"))
        p = s("lls", "List a local directory.", self.cmd_lls)
        p.add_argument("path", nargs="?", default=".")

        # PKG install engine
        p = s("install", "Install fPKGs directly on the PS4 (PS4 pulls them over HTTP).\n\n"
                         "  install FILE_OR_DIR_OR_URL...\n"
                         "Starts a local Range-capable HTTP server, asks RPI (port 12800) to install each\n"
                         "package as a 'direct' task, and shows live progress. The server MUST keep running\n"
                         "until the PS4 has finished downloading (this command waits for you).",
              self.cmd_install)
        p.add_argument("paths", nargs="+", help=".pkg files, directories (scanned recursively), or http:// URLs")
        p.add_argument("--multi", action="store_true",
                       help="treat all given packages as ONE split package (_0,_1,... in the given order)")
        p.add_argument("--parallel", type=int, default=1,
                       help="tasks installed at the same time (default 1; RPI can crash with many)")
        p.add_argument("--interval", type=float, default=3.0,
                       help="progress poll seconds (default 3; keep >=2 or RPI may crash)")
        p.add_argument("--force", action="store_true", help="install even if the title already exists")
        p.add_argument("--host", help="PC address the PS4 should use (default: auto-detect)")
        p.add_argument("--http-port", type=int, help="HTTP server port (default 8080)")
        p.add_argument("--bind", default="0.0.0.0", help="address to bind the HTTP server to")
        p.add_argument("-v", "--verbose", action="store_true", help="log every HTTP request from the PS4")

        p = s("serve", "Only run the HTTP server (for use with other installers) and print URLs.", self.cmd_serve)
        p.add_argument("paths", nargs="+")
        p.add_argument("--host")
        p.add_argument("--http-port", type=int)
        p.add_argument("--bind", default="0.0.0.0")
        p.add_argument("-v", "--verbose", action="store_true")

        p = s("pkginfo", "Show title/content id, type and version of local .pkg files.", self.cmd_pkginfo)
        p.add_argument("paths", nargs="+")

        p = s("exists", "Ask RPI whether a title is installed.", self.cmd_exists)
        p.add_argument("title_id", help="e.g. CUSA03041")

        p = s("task", "Control an RPI task: info|pause|resume|stop|remove|start|find.\n\n"
                      "  task info 268435806\n  task find UP1004-CUSA03041_00-REDEMPTION000002 [--type game|ac|patch|license]",
              self.cmd_task)
        p.add_argument("action", choices=["info", "pause", "resume", "stop", "remove", "start", "find"])
        p.add_argument("ident", help="task id (or content id for 'find')")
        p.add_argument("--type", choices=["game", "ac", "patch", "license"], default="game")

        p = s("uninstall", "Uninstall via RPI.  kinds: game|patch (title id), ac|theme (content id).", self.cmd_uninstall)
        p.add_argument("kind", choices=["game", "patch", "ac", "theme"])
        p.add_argument("ident")
        p.add_argument("-y", "--yes", action="store_true")

        s("probe", "Check connectivity to the PS4's FTP and RPI services.", self.cmd_probe)

        p = s("config", "show | path | set KEY VALUE | unset KEY  (persistent defaults).", self.cmd_config)
        p.add_argument("action", choices=["show", "path", "set", "unset"])
        p.add_argument("key", nargs="?")
        p.add_argument("value", nargs="?")

        s("shell", "Interactive shell (default when no command is given).", self.cmd_shell)

    # ---- dispatch
    def run(self, argv):
        if not argv:
            raise CLIError("empty command")
        name, rest = argv[0], argv[1:]
        if name in ("help", "?"):
            return self.show_help(rest[0] if rest else None)
        parser = self.parsers.get(name)
        if parser is None:
            raise CLIError(f"unknown command '{name}'. Type `help` for the list.")
        ns = parser.parse_intermixed_args(rest)
        return ns._fn(ns) or 0

    def show_help(self, name=None):
        if name and name in self.parsers:
            self.parsers[name].print_help()
            return 0
        seen = []
        print("Commands (use `<command> -h` for details):")
        for n, p in self.parsers.items():
            if p not in seen:
                seen.append(p)
                al = [k for k, v in self.parsers.items() if v is p and k != n]
                print(f"  {n:<10} {p.help_text}" + (f"   (aliases: {', '.join(al)})" if al else ""))
        print("  exit       leave the shell")
        return 0

    # ======================= remote browsing =============================== #
    def cmd_ls(self, a):
        multi = len(a.path) > 1
        for raw in a.path:
            for p in self.remote.glob(raw):
                e = self.remote.stat(p)
                if e is None:
                    eprint(f"ls: cannot access '{p}': no such file or directory")
                    continue
                if e.kind != "d":
                    self._print_entries([e._replace(name=p)], a)
                    continue
                if multi:
                    print(f"{p}:")
                try:
                    self._print_entries(self.remote.listdir(p, fresh=True), a)
                except ftplib.error_perm as ex:
                    eprint(f"ls: {p}: {ex}")
        return 0

    def _print_entries(self, ents, a):
        use_color = sys.stdout.isatty()
        if a.long:
            for e in ents:
                size = str(e.size) if a.bytes else human(e.size)
                name = e.name + ("/" if e.kind == "d" else "")
                if use_color and e.kind == "d":
                    name = f"\033[1;34m{name}\033[0m"
                print(f"{e.kind}{e.perms[1:] if len(e.perms) == 10 else '':<9} {size:>10}  {e.mtime:<17} {name}")
        else:
            names = [e.name + ("/" if e.kind == "d" else "") for e in ents]
            if not names:
                return
            w = max(len(n) for n in names) + 2
            cols = max(1, term_width() // w) if sys.stdout.isatty() else 1
            for i in range(0, len(names), cols):
                print("".join(n.ljust(w) for n in names[i:i + cols]).rstrip())

    def cmd_cd(self, a):
        target = self.remote.abspath(a.path)
        e = self.remote.stat(target)
        if e is None or e.kind != "d":
            raise CLIError(f"cd: {target}: not a directory")
        self.remote.cwd = target
        return 0

    def cmd_pwd(self, a):
        print(self.remote.cwd)
        return 0

    def cmd_tree(self, a):
        root = self.remote.abspath(a.path)
        e = self.remote.stat(root)
        if e is None or e.kind != "d":
            raise CLIError(f"tree: {root}: not a directory")
        print(root)
        counts = [0, 0]

        def rec(path, prefix, depth):
            ents = [x for x in self.remote.listdir(path, fresh=True) if not (a.dirs_only and x.kind != "d")]
            for i, x in enumerate(ents):
                last = i == len(ents) - 1
                label = x.name + ("/" if x.kind == "d" else "")
                if a.sizes and x.kind != "d":
                    label += f"  [{human(x.size)}]"
                print(prefix + ("└── " if last else "├── ") + label)
                counts[0 if x.kind == "d" else 1] += 1
                if x.kind == "d" and depth < a.depth:
                    try:
                        rec(posixpath.join(path, x.name), prefix + ("    " if last else "│   "), depth + 1)
                    except ftplib.error_perm:
                        print(prefix + ("    " if last else "│   ") + "└── [permission denied]")
        rec(root, "", 1)
        print(f"\n{counts[0]} directories, {counts[1]} files")
        return 0

    def _du(self, path):
        total = 0
        for _p, e in self.remote.walk(path):
            if e.kind == "f":
                total += e.size
        return total

    def cmd_du(self, a):
        for raw in a.path:
            for p in self.remote.glob(raw):
                e = self.remote.stat(p)
                if e is None:
                    eprint(f"du: {p}: no such file or directory")
                elif e.kind != "d":
                    print(f"{human(e.size):>10}  {p}")
                else:
                    if not a.summary:
                        rows = []
                        for c in self.remote.listdir(p, fresh=True):
                            cp = posixpath.join(p, c.name)
                            rows.append((c.size if c.kind != "d" else self._du(cp), cp))
                        for sz, cp in sorted(rows, reverse=True):
                            print(f"{human(sz):>10}  {cp}")
                    print(f"{human(self._du(p)):>10}  {p}  (total)")
        return 0

    def cmd_find(self, a):
        root = self.remote.abspath(a.path)
        for p, e in self.remote.walk(root):
            if a.type and e.kind != a.type:
                continue
            if fnmatch.fnmatchcase(e.name, a.name):
                print(p + ("/" if e.kind == "d" else ""))
        return 0

    def cmd_stat(self, a):
        p = self.remote.abspath(a.path)
        e = self.remote.stat(p)
        if e is None:
            raise CLIError(f"stat: {p}: no such file or directory")
        kind = {"d": "directory", "f": "file", "l": "symlink"}[e.kind]
        print(f"path: {p}\ntype: {kind}\nsize: {e.size} ({human(e.size)})\nmtime: {e.mtime}\nperms: {e.perms}")
        return 0

    def cmd_cat(self, a):
        p = self.remote.abspath(a.path)
        limit = a.bytes or (4096 if a.hex else 0)
        c = self.remote.new_conn()
        out = sys.stdout.buffer
        buf = bytearray()
        got = [0]

        def cb(b):
            if limit and got[0] + len(b) >= limit:
                b = b[:limit - got[0]]
                (buf.extend(b) if a.hex else out.write(b))
                got[0] += len(b)
                raise _Stop()
            (buf.extend(b) if a.hex else out.write(b))
            got[0] += len(b)
        try:
            c.retrbinary("RETR " + p, cb, 64 * 1024)
        except _Stop:
            pass
        except ftplib.error_perm as e:
            raise CLIError(f"cat: {p}: {e}")
        finally:
            c.close()
        if a.hex:
            for off in range(0, len(buf), 16):
                chunk = buf[off:off + 16]
                hx = " ".join(f"{x:02x}" for x in chunk)
                asc = "".join(chr(x) if 32 <= x < 127 else "." for x in chunk)
                print(f"{off:08x}  {hx:<47}  |{asc}|")
        else:
            out.flush()
        return 0

    # ======================= transfers ===================================== #
    def _jobs(self, a):
        return a.jobs or self.cfg["jobs"]

    def cmd_get(self, a):
        srcs_raw, dest = (a.paths[:-1], a.paths[-1]) if len(a.paths) > 1 else (a.paths, ".")
        srcs = []
        for s in srcs_raw:
            found = self.remote.glob(s)
            if not found:
                raise CLIError(f"get: {s}: no match")
            srcs += found
        dest_is_dir = os.path.isdir(dest) or len(srcs) > 1 or dest.endswith(("/", os.sep))
        tasks, dirs = [], set()
        eprint("scanning remote...")
        for s in srcs:
            e = self.remote.stat(s)
            if e is None:
                raise CLIError(f"get: {s}: no such file or directory")
            base = posixpath.basename(s.rstrip("/")) or "root"
            if e.kind != "d":
                target = os.path.join(dest, base) if dest_is_dir else dest
                dirs.add(os.path.dirname(os.path.abspath(target)))
                tasks += self._get_tasks(s, target, e.size, a)
            else:
                root_t = os.path.join(dest, base) if dest_is_dir else dest
                dirs.add(os.path.abspath(root_t))
                for p, ent in self.remote.walk(s):
                    t = os.path.join(root_t, *p[len(s):].strip("/").split("/"))
                    if ent.kind == "d":
                        dirs.add(os.path.abspath(t))
                    else:
                        dirs.add(os.path.dirname(os.path.abspath(t)))
                        tasks += self._get_tasks(p, t, ent.size, a)
        for d in dirs:
            os.makedirs(d, exist_ok=True)
        return 1 if run_transfers(self.remote, tasks, self._jobs(a), "get ") else 0

    def _get_tasks(self, remote_path, local_path, size, a):
        nseg = max(1, a.segments)
        if nseg > 1 and size >= SEGMENT_MIN:
            part = local_path + PART_SUFFIX
            if os.path.isfile(local_path) and os.path.getsize(local_path) == size and not a.force:
                return [GetTask(remote_path, local_path, size, False)]      # -> skipped
            with open(part, "wb") as f:
                f.truncate(size)
            state = SegState(nseg)
            step = -(-size // nseg)
            segs = []
            for i in range(nseg):
                s, e = i * step, min(size, (i + 1) * step)
                if s < e:
                    segs.append(SegTask(remote_path, part, local_path, s, e, size, state))
            state.left = len(segs)
            return segs
        return [GetTask(remote_path, local_path, size, a.force)]

    def cmd_put(self, a):
        srcs_raw, dest = (a.paths[:-1], a.paths[-1]) if len(a.paths) > 1 else (a.paths, self.remote.cwd)
        srcs = []
        for s in srcs_raw:
            g = sorted(glob.glob(os.path.expanduser(s)))
            if not g:
                raise CLIError(f"put: {s}: no such local file")
            srcs += g
        dest_abs = self.remote.abspath(dest)
        de = self.remote.stat(dest_abs)
        dest_is_dir = (de is not None and de.kind == "d") or len(srcs) > 1 or dest.endswith("/")
        tasks, mkdirs = [], set()
        for s in srcs:
            base = os.path.basename(os.path.abspath(s))
            if os.path.isdir(s):
                root_r = posixpath.join(dest_abs, base) if dest_is_dir else dest_abs
                mkdirs.add(root_r)
                for dp, dns, fns in os.walk(s):
                    rel = os.path.relpath(dp, s)
                    rdir = root_r if rel == "." else posixpath.join(root_r, *rel.split(os.sep))
                    mkdirs.add(rdir)
                    for fn in fns:
                        lp = os.path.join(dp, fn)
                        if os.path.isfile(lp):
                            tasks.append(PutTask(lp, posixpath.join(rdir, fn), os.path.getsize(lp), a.force))
            else:
                rp = posixpath.join(dest_abs, base) if dest_is_dir else dest_abs
                mkdirs.add(posixpath.dirname(rp))
                tasks.append(PutTask(s, rp, os.path.getsize(s), a.force))
        for d in sorted(mkdirs, key=len):
            self.remote.mkdir_p(d)
        rc = 1 if run_transfers(self.remote, tasks, self._jobs(a), "put ") else 0
        self.remote.invalidate()
        return rc

    def cmd_rm(self, a):
        targets = []
        for raw in a.paths:
            found = self.remote.glob(raw)
            if not found:
                raise CLIError(f"rm: {raw}: no match")
            targets += found
        for t in targets:
            if t == "/":
                raise CLIError("refusing to delete /")
        if not confirm(f"delete {len(targets)} item(s): {', '.join(targets[:5])}"
                       f"{' ...' if len(targets) > 5 else ''}"
                       f"{' (recursive!)' if a.recursive else ''}?", a.yes):
            print("aborted")
            return 1
        n = 0
        for t in targets:
            n += self.remote.remove(t, a.recursive)
        print(f"removed {n} item(s)")
        return 0

    def cmd_mkdir(self, a):
        for p in a.paths:
            self.remote.mkdir_p(p)
        return 0

    def cmd_mv(self, a):
        self.remote.rename(a.src, a.dst)
        return 0

    # local helpers
    def cmd_lpwd(self, a):
        print(os.getcwd())
        return 0

    def cmd_lcd(self, a):
        try:
            os.chdir(os.path.expanduser(a.path))
        except OSError as e:
            raise CLIError(f"lcd: {e}")
        return 0

    def cmd_lls(self, a):
        try:
            for n in sorted(os.listdir(os.path.expanduser(a.path))):
                p = os.path.join(os.path.expanduser(a.path), n)
                print(f"{'d' if os.path.isdir(p) else 'f'} {human(os.path.getsize(p)) if os.path.isfile(p) else '':>10}  {n}")
        except OSError as e:
            raise CLIError(f"lls: {e}")
        return 0

    # ======================= PKG engine ==================================== #
    def _collect_pkgs(self, paths):
        out = []
        for p in paths:
            if re.match(r"^https?://", p, re.I):
                out.append(p)
            elif os.path.isdir(p):
                for dp, _dn, fns in os.walk(p):
                    out += [os.path.join(dp, f) for f in sorted(fns) if f.lower().endswith(".pkg")]
            elif os.path.isfile(p):
                out.append(p)
            else:
                raise CLIError(f"no such file or directory: {p}")
        if not out:
            raise CLIError("no .pkg files found")
        return out

    def _host_for_ps4(self, override=None):
        host = override or self.cfg["host"] or local_ip_towards(self.need_ip())
        if not host:
            raise CLIError("cannot determine this PC's LAN address; pass --host <ip>")
        return host

    def cmd_pkginfo(self, a):
        for p in self._collect_pkgs(a.paths):
            try:
                i = read_pkg_info(p)
            except (ValueError, OSError) as e:
                eprint(f"{p}: {e}")
                continue
            sfo = i["sfo"]
            print(f"{p}\n  size       : {human(os.path.getsize(p))}\n  content id : {i['content_id']}\n"
                  f"  title id   : {i['title_id']}\n  title      : {sfo.get('TITLE', '?')}\n"
                  f"  category   : {sfo.get('CATEGORY', '?')} ({CATEGORY.get(sfo.get('CATEGORY', ''), 'unknown')})\n"
                  f"  app ver    : {sfo.get('APP_VER', '?')}   version: {sfo.get('VERSION', '?')}\n"
                  f"  drm/ctype  : {i['drm_type']:#x} / {i['content_type']:#x}")
        return 0

    def _start_server(self, a, files):
        port = a.http_port or self.cfg["http_port"]
        srv = PkgServer(a.bind, port, a.verbose)
        keys = {f: srv.add(f) for f in files}
        srv.start()
        return srv, keys

    def cmd_serve(self, a):
        files = [f for f in self._collect_pkgs(a.paths) if os.path.isfile(f)]
        host = self._host_for_ps4(a.host) if (self.cfg["ip"] or a.host) else (local_ip_towards("8.8.8.8") or "127.0.0.1")
        srv, keys = self._start_server(a, files)
        for f, k in keys.items():
            print(f"http://{host}:{srv.port}/{k}   ({human(os.path.getsize(f))})")
        print("serving - Ctrl-C to stop")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            srv.stop()
        return 0

    def cmd_install(self, a):
        items = self._collect_pkgs(a.paths)
        local = [i for i in items if not re.match(r"^https?://", i, re.I)]
        rpi = self.rpi
        try:
            rpi.check()
        except RPIError as e:
            raise CLIError(str(e))
        host = self._host_for_ps4(a.host)
        srv = keys = None
        if local:
            srv, keys = self._start_server(a, local)
            print(f"serving {len(local)} file(s) at http://{host}:{srv.port}/  (PS4 will pull from here)")

        def url_of(i):
            return i if i not in (keys or {}) else f"http://{host}:{srv.port}/{keys[i]}"

        groups = [items] if a.multi else [[i] for i in items]
        jobs = []                                   # (label, urls, title_id, total_bytes)
        for g in groups:
            first = g[0]
            tid, label = "", os.path.basename(first)
            size = sum(os.path.getsize(i) for i in g if i in (keys or {}))
            if first in (keys or {}):
                try:
                    info = read_pkg_info(first)
                    tid = info["title_id"]
                    t = info["sfo"].get("TITLE")
                    cat = CATEGORY.get(info["sfo"].get("CATEGORY", ""), "")
                    label = f"{t or label} [{info['content_id'] or tid}]" + (f" ({cat})" if cat else "")
                except (ValueError, OSError) as e:
                    eprint(f"warning: {first}: cannot read PKG header ({e}); sending anyway")
            jobs.append((label, [url_of(i) for i in g], tid, size))

        try:
            return self._run_install_jobs(a, rpi, srv, jobs)
        except KeyboardInterrupt:
            eprint("\ninterrupted. PS4 tasks keep their state but need this server to continue.\n"
                   "Resume later: run `install` again (or `task pause|resume|remove <id>`).")
            return 130
        finally:
            if srv:
                srv.stop()

    def _run_install_jobs(self, a, rpi, srv, jobs):
        pending = list(jobs)
        active = {}                                  # task_id -> state dict
        failed = 0
        interval = max(1.0, a.interval)
        t_start = time.monotonic()
        last_served, last_t = (srv.served() if srv else 0), time.monotonic()
        while pending or active:
            while pending and len(active) < max(1, a.parallel):
                label, urls, tid, size = pending.pop(0)
                if tid and not a.force:
                    try:
                        exists, sz = rpi.is_exists(tid)
                    except RPIError as e:
                        raise CLIError(str(e))
                    if exists:
                        print(f"SKIP  {label}: {tid} already installed (use --force to try anyway)")
                        continue
                print(f"INSTALL {label}")
                try:
                    r = rpi.install(urls)
                except RPIError as e:
                    eprint(f"FAIL  {label}: {e}")
                    failed += 1
                    continue
                tid_task = r.get("task_id")
                print(f"  task {tid_task} registered ({r.get('title', '')}); the PS4 is now pulling the data")
                active[tid_task] = {"label": label, "last": None, "t": time.monotonic(), "size": size}
            if not active:
                break
            time.sleep(interval)
            parts = []
            now = time.monotonic()
            served = srv.served() if srv else 0
            net = (served - last_served) / max(now - last_t, 1e-6)
            last_served, last_t = served, now
            for tk in list(active):
                st = active[tk]
                try:
                    d = rpi.task("info", tk)
                except RPIError as e:
                    eprint(f"\nFAIL  {st['label']}: {e}")
                    failed += 1
                    del active[tk]
                    continue
                state, pct, done, total, rest = self._task_state(d)
                if state == "error":
                    eprint(f"\nFAIL  {st['label']}: PS4 reports error {d.get('error')}: "
                           f"{RPI_ERRORS.get(toint(d.get('error')), 'see Troubleshoot.md')}")
                    failed += 1
                    del active[tk]
                elif state == "finished":
                    print(f"\nDONE  {st['label']} (task {tk}) in {fmt_dur(now - st['t'])}")
                    del active[tk]
                elif state in ("preparing", "starting"):
                    parts.append(f"[{tk}] {state} ({d.get('preparing_percent', '?')}%)")
                else:
                    prev = st["last"]
                    rate = (done - prev[1]) / max(now - prev[0], 1e-6) if prev else 0
                    st["last"] = (now, done)
                    parts.append(f"[{tk}] {pct:5.1f}% {human(done)}/{human(total)} {human(rate, True)} "
                                 f"ETA(ps4) {fmt_dur(rest)}")
            if parts:
                line = " | ".join(parts) + (f"   served {human(net, True)}" if srv else "")
                if sys.stderr.isatty():
                    sys.stderr.write("\r" + line[:term_width() - 1].ljust(term_width() - 1))
                    sys.stderr.flush()
                else:
                    eprint(line)
        if sys.stderr.isatty():
            sys.stderr.write("\n")
        print(f"finished in {fmt_dur(time.monotonic() - t_start)}"
              + (f"; served {human(srv.served())} from this PC" if srv else "")
              + (f"; {failed} failed" if failed else ""))
        return 1 if failed else 0

    @staticmethod
    def _task_state(d):
        """Interpret get_task_progress.  Mirrors the sender's Index.vue logic, except we
        never call a task 'finished' while transferred < length (rest_sec may be 0 early)."""
        if toint(d.get("error")):
            return "error", 0, 0, 0, None
        length, tr = toint(d.get("length")), toint(d.get("transferred"))
        ltot, ttot = toint(d.get("length_total")), toint(d.get("transferred_total"))
        if ltot > 0:
            length, tr = ltot, ttot
        if d.get("preparing_percent") != 100 and d.get("local_copy_percent") != 100 and tr == 0 and length == 0:
            return "preparing", 0, 0, 0, None
        if length == 0 and tr == 0:
            return "starting", 0, 0, 0, None
        if tr >= length:
            return "finished", 100.0, tr, length, 0
        return "running", tr / length * 100, tr, length, toint(d.get("rest_sec_total")) or None

    def cmd_exists(self, a):
        try:
            ex, size = self.rpi.is_exists(a.title_id.upper())
        except RPIError as e:
            raise CLIError(str(e))
        print(f"{a.title_id.upper()}: " + (f"installed ({human(toint(size))})" if ex else "not installed"))
        return 0

    def cmd_task(self, a):
        try:
            if a.action == "find":
                d = self.rpi.find_task(a.ident, {"game": 6, "ac": 7, "patch": 8, "license": 9}[a.type])
            else:
                d = self.rpi.task(a.action, a.ident)
        except RPIError as e:
            raise CLIError(str(e))
        print(json.dumps(d, indent=2))
        return 0

    def cmd_uninstall(self, a):
        if not confirm(f"uninstall {a.kind} {a.ident} from the PS4?", a.yes):
            print("aborted")
            return 1
        try:
            print(json.dumps(self.rpi.uninstall(a.kind, a.ident), indent=2))
        except RPIError as e:
            raise CLIError(str(e))
        return 0

    # ======================= misc ========================================== #
    def cmd_probe(self, a):
        ip = self.need_ip()
        rc = 0
        for label, port in (("FTP", self.cfg["ftp_port"]), ("RPI", self.cfg["rpi_port"])):
            t0 = time.monotonic()
            try:
                socket.create_connection((ip, port), timeout=3).close()
                print(f"{label:<4} {ip}:{port}  open ({(time.monotonic() - t0) * 1000:.0f} ms)")
            except OSError as e:
                print(f"{label:<4} {ip}:{port}  CLOSED ({e})")
                rc = 1
        try:
            c = self.remote.new_conn()
            print(f"FTP  banner: {c.getwelcome()!r}; cwd {c.pwd()!r}")
            c.close()
        except CLIError as e:
            print(str(e))
            rc = 1
        try:
            self.rpi.check()
            print("RPI  API answers (is_exists ok)")
        except RPIError as e:
            print(f"RPI  {e}")
            rc = 1
        print(f"this PC as seen by the PS4: {local_ip_towards(ip)}")
        return rc

    def cmd_config(self, a):
        cur = load_file_config()
        if a.action == "path":
            print(CONFIG_PATH)
        elif a.action == "show":
            eff = self.cfg
            for k in DEFAULTS:
                src = "file" if k in cur else "default"
                print(f"{k:<10} = {eff[k]!s:<18} ({src})")
        else:
            if not a.key or a.key not in DEFAULTS:
                raise CLIError(f"key must be one of: {', '.join(DEFAULTS)}")
            if a.action == "set":
                if a.value is None:
                    raise CLIError("config set KEY VALUE")
                cur[a.key] = a.value
            else:
                cur.pop(a.key, None)
            os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(cur, f, indent=2)
            print(f"saved to {CONFIG_PATH}")
        return 0

    # ======================= interactive shell ============================= #
    def cmd_shell(self, a=None):
        self.interactive = True
        try:
            import readline
        except ImportError:
            readline = None
        if readline:
            readline.set_completer_delims(" \t\n")
            readline.set_completer(self._complete)
            readline.parse_and_bind("tab: complete")
            if "libedit" in (readline.__doc__ or ""):
                readline.parse_and_bind("bind ^I rl_complete")
        print(f"ps4cli {__version__} - PS4 {self.cfg['ip'] or '(no --ip set)'}  | `help` for commands, Ctrl-D to exit")
        while True:
            try:
                where = self.remote.cwd if self._remote else "/"
                line = input(f"ps4 {self.cfg['ip'] or '?'}:{where}> ").strip()
            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print()
                continue
            if not line:
                continue
            if line in ("exit", "quit", "bye"):
                break
            try:
                argv = shlex.split(line)
            except ValueError as e:
                eprint(f"parse error: {e}")
                continue
            execute(self, argv)
        if self._remote:
            self._remote.close()
        return 0

    REMOTE_PATH_CMDS = {"ls", "dir", "cd", "tree", "du", "find", "stat", "cat", "view", "get", "pull",
                        "download", "rm", "del", "mkdir", "mv", "rename"}
    LOCAL_PATH_CMDS = {"put", "push", "upload", "lcd", "lls", "install", "serve", "pkginfo"}

    def _complete(self, text, state):
        try:
            import readline
            buf = readline.get_line_buffer()
            try:
                toks = shlex.split(buf[:readline.get_begidx()] + "x")
            except ValueError:
                return None
            if len(toks) <= 1:
                opts = sorted(set(self.parsers) | {"exit", "help"})
                opts = [o + " " for o in opts if o.startswith(text)]
            else:
                cmd = toks[0]
                if text.startswith("-"):
                    p = self.parsers.get(cmd)
                    opts = [o for act in (p._actions if p else []) for o in act.option_strings if o.startswith(text)]
                elif cmd in self.REMOTE_PATH_CMDS:
                    opts = self._complete_remote(text)
                elif cmd in self.LOCAL_PATH_CMDS:
                    opts = self._complete_local(text)
                    if cmd in ("put", "push", "upload") and len(toks) > 2:
                        opts += self._complete_remote(text)
                else:
                    opts = []
            return opts[state] if state < len(opts) else None
        except Exception:
            return None

    def _complete_remote(self, text):
        if self._remote is None:
            return []
        d, _, prefix = text.rpartition("/")
        base = (d + "/") if d or text.startswith("/") else ""
        try:
            ents = self.remote.listdir(d or ("/" if text.startswith("/") else "."))
        except Exception:
            return []
        return [base + e.name + ("/" if e.kind == "d" else " ") for e in ents if e.name.startswith(prefix)]

    @staticmethod
    def _complete_local(text):
        g = glob.glob(os.path.expanduser(text) + "*")
        return [x + ("/" if os.path.isdir(x) else " ") for x in sorted(g)]


def execute(app, argv):
    """Run one command line, translating errors into friendly messages.  Returns exit code."""
    try:
        return app.run(argv) or 0
    except ParserExit as e:
        return e.status
    except CLIError as e:
        eprint(f"error: {e}")
    except ftplib.error_perm as e:
        eprint(f"error: server refused: {e}")
    except ftplib.all_errors as e:
        eprint(f"error: FTP: {e}")
        if app._remote:
            app._remote.close()
    except KeyboardInterrupt:
        eprint("\ninterrupted")
        return 130
    except OSError as e:
        eprint(f"error: {e}")
    return 1


# --------------------------------------------------------------------------- #
def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    gp = Parser(prog="ps4cli", add_help=False)
    gp.add_argument("--ip")
    gp.add_argument("--ftp-port", dest="ftp_port", type=int)
    gp.add_argument("--rpi-port", dest="rpi_port", type=int)
    gp.add_argument("--user")
    gp.add_argument("--password")
    gp.add_argument("-V", "--version", action="store_true")
    gp.add_argument("-h", "--help", action="store_true")
    try:
        ns, rest = gp.parse_known_args(argv)
    except (CLIError, ParserExit) as e:
        eprint(e)
        return 2
    if ns.version:
        print(f"ps4cli {__version__}")
        return 0
    app = App(build_config(ns))
    if ns.help or (rest and rest[0] in ("-h", "--help")):
        print(__doc__)
        print("Global options (before the command): --ip IP  --ftp-port N  --rpi-port N  --user U  --password P\n")
        app.show_help()
        return 0
    if not rest or rest[0] == "shell":
        return app.cmd_shell()
    return execute(app, rest)


if __name__ == "__main__":
    sys.exit(main())
