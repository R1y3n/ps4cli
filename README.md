# ps4cli

Pure-CLI PS4 toolbox. **One Python file, no dependencies** (Python ≥ 3.8, Linux/macOS/Windows).

* `install` - direct fPKG install: your PC serves the file, the **PS4 pulls and installs it itself** (no `/data/pkg`).
* `ls cd tree du find stat cat get put rm mv mkdir` - browse the PS4 filesystem and push/pull files over FTP, in parallel, resumable.
* An interactive shell with tab-completion (remote and local paths) when you run it with no command.

Read `LEARNINGS.md` for how the original repo does it and why it is fast.

## PS4 prerequisites
1. Console on HEN / GoldHEN. 2. **Remote Package Installer** running (port 12800) for `install`.
3. An **FTP server** on the console (GoldHEN's, port 2121) for the browser/transfer commands.
4. PC and PS4 on the same LAN, wired if possible. Allow Python through the PC firewall (inbound TCP 8080).

## Quick start
```bash
python3 ps4cli.py config set ip 192.168.1.50      # once (or use --ip / PS4_IP)
python3 ps4cli.py probe                            # FTP + RPI reachable? PC address as seen by PS4?
python3 ps4cli.py install /games/CUSA03041.pkg     # direct install with live progress
python3 ps4cli.py                                  # interactive shell
```

## Global options (before the command)
`--ip IP  --ftp-port N (2121)  --rpi-port N (12800)  --user U (anonymous)  --password P  -V`
Priority: flag > env (`PS4_IP`, `PS4_FTP_PORT`, `PS4_RPI_PORT`, `PS4_FTP_USER`, `PS4_FTP_PASS`, `PS4CLI_HOST`) > `~/.config/ps4cli.json` (`config set`) > default.
Persisted keys: `ip rpi_port ftp_port user password jobs http_port host timeout`.

## Install engine
```
install PATH|DIR|URL ...  [--multi] [--parallel N] [--interval S] [--force]
                          [--host PC_IP] [--http-port 8080] [--bind 0.0.0.0] [-v]
serve   PATH|DIR ...      [--host] [--http-port] [-v]       # only the HTTP server, prints URLs
pkginfo FILE|DIR ...                                         # title/content id/category from the PKG
exists  CUSA12345                                            # is it installed? (size)
task    info|pause|resume|stop|remove|start ID               # control a PS4 task
task    find CONTENT_ID [--type game|ac|patch|license]
uninstall game|patch CUSA12345 | ac|theme CONTENT_ID  [-y]
probe
```
* Directories are scanned recursively for `*.pkg`; `http://` URLs are passed to RPI as is (e.g. CDN or another server).
* Already-installed titles are skipped unless `--force`.
* `--multi`: the given files are **one** split package in this order (`..._0.pkg ..._1.pkg`).
* Keep `install` running until it prints `DONE` - the PS4 is reading from this server. Ctrl-C leaves the task on the PS4 (resume with `task resume ID`, or run install again).
* If auto-detect picks the wrong NIC (VPN, multiple adapters): `--host 192.168.1.20`.
* Error `-2135809020 … 404`: the PS4 couldn't fetch the URL (wrong `--host`, firewall, other subnet).

Example session line: `[268435801]  41.2% 8.2 GiB/19.9 GiB 108.3 MiB/s ETA(ps4) 01:48 | served 109.0 MiB/s`

## Browse / transfer
```
ls [-l] [--bytes] [PATH...]    cd PATH    pwd    tree [-L N] [-d] [-s] [PATH]    du [-s] [PATH]
find [PATH] -name '*.pkg' [-type f|d]    stat PATH    cat [-n BYTES] [-x] PATH    (-x = hex dump)
get  REMOTE... [LOCAL]   [-j N] [--segments N] [-f]       aliases: pull, download
put  LOCAL...  [REMOTE]  [-j N] [-f]                      aliases: push, upload
rm [-r] [-y] PATH...     mv SRC DST     mkdir PATH...
lcd lpwd lls                         (local counterparts, mostly for the shell)
```
* Globs work on the last path component: `get '/data/pkg/*.pkg' ./pkgs/`, `put ./saves/*.dat /user/home`.
* Directories are transferred recursively, tree structure preserved. Spaces are fine (quote them).
* **Speed:** `-j` parallel connections (default 4; try 6-8 for many small files); `--segments 4` splits big files (≥32 MiB) into parallel byte ranges.
* **Safe & resumable:** data goes to `name.ps4cli-part`, renamed only when the size matches. Re-running the same command skips finished files and resumes partial ones (not with `--segments`). `-f` forces overwrite.
* `rm` asks for confirmation (needs `-y` when not interactive) and refuses `/`.
* Current directory is client-side state, so every command uses absolute paths.

### Shell
```
$ python3 ps4cli.py --ip 192.168.1.50
ps4 192.168.1.50:/> cd /user/app
ps4 192.168.1.50:/user/app> ls -l
ps4 192.168.1.50:/user/app> tree -L 2 CUSA03041
ps4 192.168.1.50:/user/app> lcd ~/dumps
ps4 192.168.1.50:/user/app> get CUSA03041/sce_sys ./
ps4 192.168.1.50:/user/app> put ~/mods/patch.bin /data/
ps4 192.168.1.50:/user/app> install ~/games/        # same install command in the shell
```
Tab completes commands, options, remote paths (for remote commands) and local paths (for `put/lcd/install`).

## Tests (no console needed)
```bash
pip install pyftpdlib
python3 tests/run_tests.py     # 42 end-to-end checks vs a mock PS4 (FTP + fake RPI that pulls with Range)
python3 tests/test_units.py    # 20 checks: Range parser, HTTP server, LIST fallback, throughput
python3 tests/mock_ps4.py      # run the mock yourself; then: ps4cli --ip 127.0.0.1 probe
```
