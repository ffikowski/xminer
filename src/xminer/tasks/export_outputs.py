from __future__ import annotations
import argparse, os, shlex, shutil, subprocess, sys, stat
from pathlib import Path
from ..config.params import Params  # <-- use the class (case matters!)

def _have(cmd: str) -> bool: return shutil.which(cmd) is not None
def _expand(p: str | None) -> str | None: return os.path.expanduser(p) if p else p

def _remote_has_rsync(ssh) -> bool:
    user_host, ssh_opts = _build_ssh(ssh["user"], ssh["host"], ssh["port"], ssh.get("identity_file"))
    argv = ["ssh", *ssh_opts, user_host, "bash", "-c",
            "command -v rsync >/dev/null && echo YES || echo NO"]
    try:
        out = subprocess.check_output(argv).decode().strip()
        return out == "YES"
    except subprocess.CalledProcessError:
        return False


def _list_remote_matches(ssh, remote_base: str, patterns: list[str]) -> list[str]:
    """
    Returns RELATIVE paths (to remote_base) matching patterns.
    Robust to noisy login/startup output from the remote shell.
    """
    user_host, ssh_opts = _build_ssh(ssh["user"], ssh["host"], ssh["port"], ssh.get("identity_file"))

    # Quote each pattern for the *remote* shell
    glob_parts = " ".join(shlex.quote(p) for p in patterns)

    # Use non-login bash (-c, NOT -lc), and fence output between markers.
    remote_cmd = (
        f"set -e; "
        f"shopt -s globstar nullglob dotglob 2>/dev/null || true; "
        f"cd {shlex.quote(remote_base)}; "
        f"printf '__XM_START__\\0'; "
        f"printf '%s\\0' {glob_parts}; "
        f"printf '__XM_END__\\0'"
    )

    argv = ["ssh", *ssh_opts, user_host, "bash", "-c", remote_cmd]
    out = subprocess.check_output(argv)

    tokens = out.decode("utf-8", "ignore").split("\0")
    # Keep only the slice strictly between the sentinels
    rels = []
    try:
        i = tokens.index("__XM_START__")
        j = tokens.index("__XM_END__", i + 1)
        rels = [t for t in tokens[i+1:j] if t]  # skip empties
    except ValueError:
        # No sentinels? fallback to previous behavior (still NUL-safe if lucky),
        # but also filter out lines that look like shell noise.
        rels = [t for t in tokens if t and ("\n" not in t) and (" " not in t)]

    return rels

def _scp_opts_from_ssh(ssh_opts: list[str]) -> list[str]:
    out = []
    it = iter(ssh_opts)
    for opt in it:
        if opt == "-p":                 # ssh port
            out.extend(["-P", next(it)])  # scp port
        else:
            out.append(opt)
    return out

def _build_ssh(user: str, host: str, port: int, identity_file: str | None):
    user_host = f"{user}@{host}"
    ssh_opts = ["-p", str(int(port)), "-o", "StrictHostKeyChecking=accept-new"]
    if identity_file:
        ssh_opts += ["-i", _expand(identity_file)]
    return user_host, ssh_opts


def _rsync_copy(ssh, remote_base, patterns, local_dest: Path, dry_run: bool):
    user_host, ssh_opts = _build_ssh(ssh["user"], ssh["host"], ssh["port"], ssh.get("identity_file"))
    rels = _list_remote_matches(ssh, remote_base, patterns)
    if not rels:
        print("No remote files matched patterns; nothing to copy.")
        return

    e_arg = " ".join(["ssh", *ssh_opts])  # <- was f"ssh {ssh_opts}"
    for rel in rels:
        src = f"{user_host}:{shlex.quote(remote_base.rstrip('/'))}/./{rel}"
        cmd = ["rsync", "-avR", "-e", e_arg, src, str(local_dest)]
        if dry_run:
            cmd.insert(1, "--dry-run")
        print("Running:", " ".join(shlex.quote(c) for c in cmd))
        subprocess.check_call(cmd)


def _scp_copy(ssh, remote_base, patterns, local_dest: Path, dry_run: bool):
    user_host, ssh_opts = _build_ssh(ssh["user"], ssh["host"], ssh["port"], ssh.get("identity_file"))
    rels = _list_remote_matches(ssh, remote_base, patterns)
    if not rels:
        print("No remote files matched patterns; nothing to copy.")
        return

    scp_opts = _scp_opts_from_ssh(ssh_opts)

    for rel in rels:
        local_path = local_dest / rel
        local_path.parent.mkdir(parents=True, exist_ok=True)

        src = f"{user_host}:{shlex.quote(remote_base.rstrip('/'))}/{rel}"
        argv = ["scp", *scp_opts, src, str(local_path)]

        if dry_run:
            print("[dry-run] Would run:", " ".join(shlex.quote(a) for a in argv))
        else:
            print("Running:", " ".join(shlex.quote(a) for a in argv))
            subprocess.check_call(argv)

def main(argv=None) -> int:
    P = Params()  # load parameters.yml via your production class

    # Helper that works whether Params exposes attributes or a dict-like .get()
    def g(key, default=None):
        return getattr(P, key, getattr(P, "get", lambda k, d=None: d)(key, default))

    ssh = {
        "host": P.EXPORT_SSH_HOST,
        "user": P.EXPORT_SSH_USER,
        "port": int(P.EXPORT_SSH_PORT),
        "identity_file": P.EXPORT_SSH_IDENTITY_FILE,
    }
    remote_base = P.EXPORT_REMOTE_BASE_DIR
    patterns = list(P.EXPORT_PATTERNS or [])
    local_dest = Path(_expand(P.EXPORT_LOCAL_DEST_DIR or ""))

    parser = argparse.ArgumentParser(description="Export specific CSV files from VPS to local directory.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    # minimal validation
    if not all([ssh["host"], ssh["user"], remote_base, patterns, str(local_dest)]):
        print("Missing export parameters. Check parameters.yml (ssh_*, remote_base_dir, export_patterns, local_dest_dir).",
              file=sys.stderr)
        return 1

    local_dest.mkdir(parents=True, exist_ok=True)
    local_rsync = _have("rsync")
    remote_rsync = _remote_has_rsync(ssh) if local_rsync else False
    try:
        if local_rsync and remote_rsync:
            _rsync_copy(ssh, remote_base, patterns, local_dest, args.dry_run)
        else:
            if local_rsync and not remote_rsync:
                print("Remote rsync not available; using scp with path preservation...", file=sys.stderr)
            elif not local_rsync:
                print("Local rsync not found; using scp with path preservation...", file=sys.stderr)
            _scp_copy(ssh, remote_base, patterns, local_dest, args.dry_run)
    except subprocess.CalledProcessError as e:
        print(f"Transfer failed (exit {e.returncode})", file=sys.stderr)
        return e.returncode
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
