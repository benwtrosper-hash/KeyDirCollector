import os
import re
import fnmatch
import shutil
import threading
import queue
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import tkinter as tk
from tkinter import filedialog, messagebox


# ----------------------------
# Fast filesystem walkers
# ----------------------------
def iter_dirs(root: str, recursive: bool):
    """Yield directory paths under root (including nested) using os.scandir (fast)."""
    if not recursive:
        try:
            with os.scandir(root) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        yield e.path
        except (PermissionError, FileNotFoundError):
            return
        return

    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        yield e.path
                        stack.append(e.path)
        except (PermissionError, FileNotFoundError):
            continue


def iter_files(root: str, recursive: bool):
    """Yield file paths under root using os.scandir (fast)."""
    if not recursive:
        try:
            with os.scandir(root) as it:
                for e in it:
                    if e.is_file(follow_symlinks=False):
                        yield e.path
        except (PermissionError, FileNotFoundError):
            return
        return

    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        stack.append(e.path)
                    elif e.is_file(follow_symlinks=False):
                        yield e.path
        except (PermissionError, FileNotFoundError):
            continue


# ----------------------------
# Matching + output naming
# ----------------------------
def parse_ext_list(ext_text: str):
    t = (ext_text or "").strip().lower()
    if not t or t == "*":
        return {"*"}
    parts = [p.strip().lstrip(".") for p in t.split(",") if p.strip()]
    return set(parts) or {"*"}


def unique_path_if_allowed(path: Path, allow_duplicates: bool):
    """
    If allow_duplicates:
      file.ext -> file (1).ext -> file (2).ext ...
    Else:
      return None if exists
    """
    if not path.exists():
        return path
    if not allow_duplicates:
        return None
    stem = path.stem
    suffix = path.suffix
    i = 1
    while True:
        candidate = path.with_name(f"{stem} ({i}){suffix}")
        if not candidate.exists():
            return candidate
        i += 1


# ----------------------------
# Batch shortcut creation (single powershell per chunk)
# ----------------------------
def _ps_escape_single(s: str) -> str:
    return s.replace("'", "''")


def create_shortcuts_batch_powershell(pairs, chunk_size=200):
    """
    pairs: list[(target_path:str, link_path:str)]
    Creates shortcuts in batches to avoid command-length limits.
    Returns: (ok_count:int, err_lines:list[str]) where err_lines are "ERR|link|message".
    """
    ok_total = 0
    err_lines = []

    for i in range(0, len(pairs), chunk_size):
        chunk = pairs[i:i + chunk_size]

        items = []
        for t, l in chunk:
            t_esc = _ps_escape_single(t)
            l_esc = _ps_escape_single(l)
            items.append(f"@{{t='{t_esc}'; l='{l_esc}'}}")

        ps = (
            "$WshShell = New-Object -ComObject WScript.Shell; "
            f"$items = @({','.join(items)}); "
            "foreach($i in $items){ "
            "  try { "
            "    $s = $WshShell.CreateShortcut($i.l); "
            "    $s.TargetPath = $i.t; "
            "    $s.WorkingDirectory = (Split-Path -Path $s.TargetPath); "
            "    $s.Save(); "
            "    Write-Output ('OK|' + $i.l); "
            "  } catch { "
            "    $m = $_.Exception.Message; "
            "    Write-Output ('ERR|' + $i.l + '|' + $m); "
            "  } "
            "}"
        )

        completed = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps],
            capture_output=True,
            text=True
        )

        out = (completed.stdout or "").splitlines()
        err = (completed.stderr or "").strip()

        if completed.returncode != 0 and not out:
            raise RuntimeError(err or "PowerShell failed creating shortcuts (no stdout).")

        for line in out:
            if line.startswith("OK|"):
                ok_total += 1
            elif line.startswith("ERR|"):
                err_lines.append(line)

        if completed.returncode != 0 and err:
            err_lines.append(f"ERR|<batch>|{err}")

    return ok_total, err_lines


# ----------------------------
# GUI + counters
# ----------------------------
@dataclass
class Counters:
    start_epoch: float = 0.0
    end_epoch: float = 0.0
    key_dirs: int = 0
    files_seen: int = 0
    ext_matched: int = 0
    name_matched: int = 0
    processed: int = 0
    skip_ext: int = 0
    skip_name: int = 0
    skip_dup: int = 0
    errors: int = 0


class KeyDirCollectorApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Key-Directory Target Collector")
        self.root.geometry("1000x780")

        self.msgq = queue.Queue()
        self._stop_flag = threading.Event()
        self._scan_thread = None

        self._lock = threading.Lock()
        self.c = Counters()

        self._build_ui()
        self._poll_messages()
        self._tick_live_counter()

    # ---------- UI ----------
    def _build_ui(self):
        pad = {"padx": 10, "pady": 6}

        # Source
        f = tk.Frame(self.root)
        f.pack(fill="x", **pad)
        tk.Label(f, text="Top-level Source Directory").pack(anchor="w")
        r = tk.Frame(f)
        r.pack(fill="x")
        self.ent_source = tk.Entry(r)
        self.ent_source.pack(side="left", fill="x", expand=True)
        tk.Button(r, text="Browse", command=self._pick_source).pack(side="left")

        # Output
        f = tk.Frame(self.root)
        f.pack(fill="x", **pad)
        tk.Label(f, text="Output Directory").pack(anchor="w")
        r = tk.Frame(f)
        r.pack(fill="x")
        self.ent_output = tk.Entry(r)
        self.ent_output.pack(side="left", fill="x", expand=True)
        tk.Button(r, text="Browse", command=self._pick_output).pack(side="left")

        # Search settings
        lf = tk.LabelFrame(self.root, text="Search Settings")
        lf.pack(fill="x", **pad)
        lf.columnconfigure(1, weight=1)

        tk.Label(lf, text="Key Prefix").grid(row=0, column=0, sticky="w", padx=6, pady=4)
        self.ent_key = tk.Entry(lf, width=20)
        self.ent_key.insert(0, "700")
        self.ent_key.grid(row=0, column=1, sticky="w", padx=6, pady=4)

        tk.Label(lf, text="Extensions (csv or *), e.g. pdf").grid(row=1, column=0, sticky="w", padx=6, pady=4)
        self.ent_ext = tk.Entry(lf, width=20)
        self.ent_ext.insert(0, "pdf")
        self.ent_ext.grid(row=1, column=1, sticky="w", padx=6, pady=4)

        tk.Label(lf, text="Optional Name Filter").grid(row=2, column=0, sticky="w", padx=6, pady=4)
        self.ent_namefilter = tk.Entry(lf, width=60)
        self.ent_namefilter.grid(row=2, column=1, sticky="we", padx=6, pady=4)

        tk.Label(lf, text='Tag separator (between keydir and filename), e.g. " ;; "').grid(
            row=3, column=0, sticky="w", padx=6, pady=4
        )
        self.ent_tagsep = tk.Entry(lf, width=20)
        self.ent_tagsep.insert(0, " ;; ")
        self.ent_tagsep.grid(row=3, column=1, sticky="w", padx=6, pady=4)

        # Options
        of = tk.LabelFrame(self.root, text="Options")
        of.pack(fill="x", **pad)

        self.var_mode = tk.StringVar(value="SHORTCUT (.lnk)")
        tk.Label(of, text="Action (choose before Run Scan):").pack(anchor="w")
        tk.OptionMenu(of, self.var_mode, "COPY", "MOVE", "SHORTCUT (.lnk)").pack(anchor="w")

        row = tk.Frame(of)
        row.pack(fill="x", padx=6, pady=4)
        self.var_recursive = tk.BooleanVar(value=True)
        tk.Checkbutton(row, text="Recursive", variable=self.var_recursive).pack(side="left", padx=8)

        self.var_allow_duplicates = tk.BooleanVar(value=False)
        tk.Checkbutton(row, text="Allow duplicates (auto add (1),(2),...)", variable=self.var_allow_duplicates).pack(side="left", padx=8)

        # Name filter behavior
        row2 = tk.Frame(of)
        row2.pack(fill="x", padx=6, pady=4)
        self.var_case_ins = tk.BooleanVar(value=True)
        tk.Checkbutton(row2, text="Case-insensitive", variable=self.var_case_ins).pack(side="left", padx=8)

        self.var_match_stem = tk.BooleanVar(value=True)
        tk.Checkbutton(row2, text="Match against stem only", variable=self.var_match_stem).pack(side="left", padx=8)

        self.var_contains = tk.BooleanVar(value=True)
        tk.Checkbutton(row2, text="Contains match (when no wildcards)", variable=self.var_contains).pack(side="left", padx=8)

        self.var_regex = tk.BooleanVar(value=False)
        tk.Checkbutton(row2, text="Regex mode (filter is regex)", variable=self.var_regex).pack(side="left", padx=8)

        # Performance / Logging controls
        row3 = tk.Frame(of)
        row3.pack(fill="x", padx=6, pady=4)

        self.var_perf = tk.BooleanVar(value=True)
        tk.Checkbutton(row3, text="Performance mode (minimal logging)", variable=self.var_perf).pack(side="left", padx=8)

        tk.Label(row3, text="Progress update every N files:").pack(side="left", padx=8)
        self.ent_prog_files = tk.Entry(row3, width=6)
        self.ent_prog_files.insert(0, "500")
        self.ent_prog_files.pack(side="left")

        tk.Label(row3, text="or every N seconds:").pack(side="left", padx=8)
        self.ent_prog_secs = tk.Entry(row3, width=6)
        self.ent_prog_secs.insert(0, "1.0")
        self.ent_prog_secs.pack(side="left")

        tk.Label(row3, text="Max error lines:").pack(side="left", padx=8)
        self.ent_max_err = tk.Entry(row3, width=6)
        self.ent_max_err.insert(0, "25")
        self.ent_max_err.pack(side="left")

        self.var_diag = tk.BooleanVar(value=False)
        tk.Checkbutton(row3, text="Diagnostics (skip reasons)", variable=self.var_diag).pack(side="left", padx=8)

        # Live Counter
        cf = tk.LabelFrame(self.root, text="Live Counter")
        cf.pack(fill="x", **pad)
        self.var_counter = tk.StringVar(value="Idle.")
        tk.Label(cf, textvariable=self.var_counter, justify="left").pack(anchor="w", padx=8, pady=6)

        # Run controls
        rf = tk.Frame(self.root)
        rf.pack(fill="x", **pad)
        self.btn_run = tk.Button(rf, text="Run Scan", command=self._run_thread)
        self.btn_run.pack(side="left")
        self.btn_stop = tk.Button(rf, text="Stop", command=self._stop, state="disabled")
        self.btn_stop.pack(side="left", padx=10)
        self.status = tk.StringVar(value="Idle")
        tk.Label(rf, textvariable=self.status).pack(side="left", padx=20)

        # Log
        lf2 = tk.LabelFrame(self.root, text="Log")
        lf2.pack(fill="both", expand=True, **pad)
        self.txt_log = tk.Text(lf2, height=16)
        self.txt_log.pack(fill="both", expand=True)

    # ---------- UI helpers ----------
    def _pick_source(self):
        d = filedialog.askdirectory()
        if d:
            self.ent_source.delete(0, tk.END)
            self.ent_source.insert(0, d)

    def _pick_output(self):
        d = filedialog.askdirectory()
        if d:
            self.ent_output.delete(0, tk.END)
            self.ent_output.insert(0, d)

    def _log(self, msg: str):
        self.txt_log.insert(tk.END, msg + "\n")
        self.txt_log.see(tk.END)

    def _poll_messages(self):
        try:
            while True:
                kind, payload = self.msgq.get_nowait()
                if kind == "log":
                    self._log(payload)
                elif kind == "log_batch":
                    for line in payload:
                        self._log(line)
                elif kind == "status":
                    self.status.set(payload)
                elif kind == "scan_done":
                    self.btn_run.config(state="normal")
                    self.btn_stop.config(state="disabled")
        except queue.Empty:
            pass
        self.root.after(100, self._poll_messages)

    def _tick_live_counter(self):
        with self._lock:
            c = self.c
            if c.start_epoch <= 0:
                self.var_counter.set("Idle.")
            else:
                now = time.time()
                end = c.end_epoch if c.end_epoch > 0 else now
                elapsed = max(0.0, end - c.start_epoch)
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
                self.var_counter.set(
                    f"Time: {ts}  |  Elapsed: {elapsed:0.1f}s\n"
                    f"Key dirs: {c.key_dirs}\n"
                    f"Files seen: {c.files_seen}\n"
                    f"Ext matched: {c.ext_matched}  |  Ext skipped: {c.skip_ext}\n"
                    f"Name matched: {c.name_matched}  |  Name skipped: {c.skip_name}\n"
                    f"Processed outputs: {c.processed}\n"
                    f"Duplicate outputs skipped: {c.skip_dup}\n"
                    f"Errors: {c.errors}"
                )
        self.root.after(500, self._tick_live_counter)

    # ---------- Run ----------
    def _stop(self):
        self._stop_flag.set()
        self.msgq.put(("log", "Stop requested… (will stop after current directory/file)"))

    def _run_thread(self):
        if self._scan_thread and self._scan_thread.is_alive():
            return

        src = self.ent_source.get().strip()
        out = self.ent_output.get().strip()
        key = self.ent_key.get().strip()

        if not src or not out or not key:
            messagebox.showerror("Missing fields", "Source, Output, and Key Prefix are required.")
            return

        if not Path(src).exists():
            messagebox.showerror("Bad source", "Source directory does not exist.")
            return

        self._stop_flag.clear()

        with self._lock:
            self.c = Counters(start_epoch=time.time(), end_epoch=0.0)

        self.txt_log.delete("1.0", tk.END)
        self.btn_run.config(state="disabled")
        self.btn_stop.config(state="normal")

        self._scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        self._scan_thread.start()

    def _scan_worker(self):
        top = self.ent_source.get().strip()
        out_dir = self.ent_output.get().strip()
        keyprefix = self.ent_key.get().strip()

        mode = self.var_mode.get()
        recursive = self.var_recursive.get()
        allow_dups = self.var_allow_duplicates.get()

        perf = self.var_perf.get()
        diag = self.var_diag.get()

        # progress cadence
        try:
            prog_files = max(1, int(self.ent_prog_files.get().strip() or "500"))
        except ValueError:
            prog_files = 500
        try:
            prog_secs = float(self.ent_prog_secs.get().strip() or "1.0")
            prog_secs = max(0.2, prog_secs)
        except ValueError:
            prog_secs = 1.0
        try:
            max_err_lines = max(0, int(self.ent_max_err.get().strip() or "25"))
        except ValueError:
            max_err_lines = 25

        ext_set = parse_ext_list(self.ent_ext.get())
        name_filter_raw = self.ent_namefilter.get().strip()
        tag_sep = self.ent_tagsep.get() or " "

        case_ins = self.var_case_ins.get()
        match_stem = self.var_match_stem.get()
        contains_mode = self.var_contains.get()
        regex_mode = self.var_regex.get()

        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        # Compile regex once
        regex = None
        if name_filter_raw and regex_mode:
            flags = re.IGNORECASE if case_ins else 0
            try:
                regex = re.compile(name_filter_raw, flags=flags)
            except re.error as e:
                self.msgq.put(("log", f"FATAL: Invalid regex: {e}"))
                self.msgq.put(("status", "Failed"))
                with self._lock:
                    self.c.end_epoch = time.time()
                self.msgq.put(("scan_done", True))
                return

        tokens = []
        if name_filter_raw and (not regex_mode):
            tokens = re.split(r"[;,]+", name_filter_raw)
            tokens = [t.strip() for t in tokens if t.strip()]

        def norm(s: str) -> str:
            return s.lower() if case_ins else s

        def name_matches(filename: str) -> bool:
            if not name_filter_raw:
                return True
            target = Path(filename).stem if match_stem else filename
            if regex is not None:
                return bool(regex.search(target))
            target_n = norm(target)
            for tok in tokens:
                tok_n = norm(tok)
                if "*" in tok_n or "?" in tok_n:
                    if fnmatch.fnmatch(target_n, tok_n):
                        return True
                else:
                    if contains_mode:
                        if tok_n in target_n:
                            return True
                    else:
                        if tok_n == target_n:
                            return True
            return False

        def ext_matches(file_path: str) -> bool:
            if "*" in ext_set:
                return True
            suf = Path(file_path).suffix.lower().lstrip(".")
            return suf in ext_set

        # Minimal logging helpers
        log_buffer = []
        def flush_logs(force=False):
            if force or len(log_buffer) >= 40:
                self.msgq.put(("log_batch", log_buffer.copy()))
                log_buffer.clear()

        # Progress heartbeat
        last_progress_time = time.time()
        last_progress_files_seen = 0

        def maybe_progress(force=False):
            nonlocal last_progress_time, last_progress_files_seen
            now = time.time()
            with self._lock:
                seen = self.c.files_seen
                proc = self.c.processed
                errc = self.c.errors
            if force or (seen - last_progress_files_seen >= prog_files) or (now - last_progress_time >= prog_secs):
                log_buffer.append(f"PROGRESS: files_seen={seen}, processed={proc}, errors={errc}")
                flush_logs()
                last_progress_time = now
                last_progress_files_seen = seen

        # Start log (always concise)
        self.msgq.put(("status", "Scanning…"))
        self.msgq.put(("log", f"Mode={mode} | Recursive={recursive} | AllowDup={allow_dups} | PerfMode={perf}"))
        self.msgq.put(("log", f"KeyPrefix={keyprefix} | Ext={','.join(sorted(ext_set))} | TagSep={repr(tag_sep)}"))
        self.msgq.put(("log", f"Filter={name_filter_raw or '(none)'} | regex={regex_mode} | stem={match_stem} | contains={contains_mode} | case_ins={case_ins}"))
        self.msgq.put(("log", "-" * 100))

        shortcut_pairs = []
        error_lines = []  # keep first max_err_lines only

        # Phase 1: find key dirs
        key_dirs = []
        for d in iter_dirs(top, recursive=True if recursive else False):
            if self._stop_flag.is_set():
                break
            if os.path.basename(d).startswith(keyprefix):
                key_dirs.append(d)
        if os.path.basename(top).startswith(keyprefix):
            key_dirs.insert(0, top)

        with self._lock:
            self.c.key_dirs = len(key_dirs)

        if not perf:
            log_buffer.append(f"Found {len(key_dirs)} key directories.")
            flush_logs(force=True)

        # Phase 2: scan each key dir
        for kd in key_dirs:
            if self._stop_flag.is_set():
                break

            if (not perf) and len(log_buffer) < 10:
                log_buffer.append(f"[KEYDIR] {kd}")
                flush_logs()

            for fp in iter_files(kd, recursive=True if recursive else False):
                if self._stop_flag.is_set():
                    break

                with self._lock:
                    self.c.files_seen += 1

                if not ext_matches(fp):
                    with self._lock:
                        self.c.skip_ext += 1
                    # No logging in perf mode
                    continue

                with self._lock:
                    self.c.ext_matched += 1

                fname = os.path.basename(fp)
                if not name_matches(fname):
                    with self._lock:
                        self.c.skip_name += 1
                    if diag and (not perf) and len(log_buffer) < 200:
                        log_buffer.append(f"SKIP(name): {fname}")
                        flush_logs()
                    continue

                with self._lock:
                    self.c.name_matched += 1

                keydir_name = os.path.basename(kd)
                if mode == "SHORTCUT (.lnk)":
                    out_name = f"{keydir_name}{tag_sep}{fname}.lnk"
                else:
                    out_name = f"{keydir_name}{tag_sep}{fname}"

                dest = unique_path_if_allowed(out_path / out_name, allow_dups)
                if dest is None:
                    with self._lock:
                        self.c.skip_dup += 1
                    if diag and (not perf) and len(log_buffer) < 200:
                        log_buffer.append(f"SKIP(dup): {out_name}")
                        flush_logs()
                    continue

                try:
                    if mode == "COPY":
                        shutil.copy2(fp, str(dest))
                        with self._lock:
                            self.c.processed += 1
                    elif mode == "MOVE":
                        shutil.move(fp, str(dest))
                        with self._lock:
                            self.c.processed += 1
                    else:
                        shortcut_pairs.append((fp, str(dest)))

                except Exception as e:
                    with self._lock:
                        self.c.errors += 1
                    if len(error_lines) < max_err_lines:
                        error_lines.append(f"ERR: {fname} ({e})")

                # heartbeat progress
                maybe_progress()

        # Batch shortcut creation
        if (not self._stop_flag.is_set()) and mode == "SHORTCUT (.lnk)" and shortcut_pairs:
            log_buffer.append(f"Creating {len(shortcut_pairs)} shortcuts (batched PowerShell)…")
            flush_logs(force=True)

            try:
                ok_count, err_lines = create_shortcuts_batch_powershell(shortcut_pairs, chunk_size=200)
                with self._lock:
                    self.c.processed += ok_count
                    self.c.errors += sum(1 for ln in err_lines if ln.startswith("ERR|"))

                # store only first max_err_lines
                for ln in err_lines:
                    if len(error_lines) >= max_err_lines:
                        break
                    error_lines.append(ln)

            except Exception as e:
                with self._lock:
                    self.c.errors += 1
                if len(error_lines) < max_err_lines:
                    error_lines.append(f"FATAL shortcut batch: {e}")

        # End: freeze elapsed time
        with self._lock:
            self.c.end_epoch = time.time()

        # Final progress + summary
        maybe_progress(force=True)
        flush_logs(force=True)

        with self._lock:
            c = self.c

        self.msgq.put(("log", "-" * 100))
        self.msgq.put(("log", f"SUMMARY: key_dirs={c.key_dirs}, files_seen={c.files_seen}, "
                              f"ext_matched={c.ext_matched}, name_matched={c.name_matched}, "
                              f"processed={c.processed}, skip_ext={c.skip_ext}, skip_name={c.skip_name}, "
                              f"skip_dup={c.skip_dup}, errors={c.errors}"))

        if error_lines:
            self.msgq.put(("log", f"ERRORS (first {len(error_lines)}):"))
            for ln in error_lines:
                self.msgq.put(("log", f"  {ln}"))

        self.msgq.put(("status", "Stopped" if self._stop_flag.is_set() else "Done"))
        self.msgq.put(("scan_done", True))

    # ---------- Live counter ----------
    def _tick_live_counter(self):
        with self._lock:
            c = self.c
            if c.start_epoch <= 0:
                self.var_counter.set("Idle.")
            else:
                now = time.time()
                end = c.end_epoch if c.end_epoch > 0 else now
                elapsed = max(0.0, end - c.start_epoch)
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
                self.var_counter.set(
                    f"Time: {ts}  |  Elapsed: {elapsed:0.1f}s\n"
                    f"Key dirs: {c.key_dirs}\n"
                    f"Files seen: {c.files_seen}\n"
                    f"Ext matched: {c.ext_matched}  |  Ext skipped: {c.skip_ext}\n"
                    f"Name matched: {c.name_matched}  |  Name skipped: {c.skip_name}\n"
                    f"Processed outputs: {c.processed}\n"
                    f"Duplicate outputs skipped: {c.skip_dup}\n"
                    f"Errors: {c.errors}"
                )
        self.root.after(500, self._tick_live_counter)


if __name__ == "__main__":
    root = tk.Tk()
    app = KeyDirCollectorApp(root)
    root.mainloop()
