from __future__ import annotations
import argparse, os, shlex, shutil, subprocess, sys
from pathlib import Path
from ..config.params import Params  # <-- use the class (case matters!)

def _have(cmd: str) -> bool: return shutil.which(cmd) is not None
def _expand(p: str | None) -> str | None: return os.path.expanduser(p) if p else p

def _build_ssh(user: str, host: str, port: int, identity_file: str | None):
    user_host = f"{user}@{host}"
    ssh_opts = [f"-p {int(port)}"]
    if identity_file: ssh_opts.append(f"-i {shlex.quote(_expand(identity_file))}")
    ssh_opts.append("-o StrictHostKeyChecking=accept-new")
    return user_host, " ".join(ssh_opts)

def _rsync_copy(ssh, remote_base, patterns, local_dest: Path, dry_run: bool):
    user_host, ssh_opts = _build_ssh(ssh["user"], ssh["host"], ssh["port"], ssh.get("identity_file"))
    src = f"{user_host}:{shlex.quote(remote_base.rstrip('/'))}/"
    cmd = ["rsync", "-av", "-e", f"ssh {ssh_opts}", *[f"--include={p}" for p in patterns], "--exclude=*",
           src, str(local_dest)]
    if dry_run: cmd.insert(1, "--dry-run")
    print("Running:", " ".join(shlex.quote(c) for c in cmd))
    subprocess.check_call(cmd)

def _scp_copy(ssh, remote_base, patterns, local_dest: Path, dry_run: bool):
    user_host, _ = _build_ssh(ssh["user"], ssh["host"], ssh["port"], ssh.get("identity_file"))
    for p in patterns:
        remote = f"{user_host}:{shlex.quote(remote_base.rstrip('/'))}/{p}"
        cmd = ["scp", "-r", "-P", str(ssh["port"])]
        if ssh.get("identity_file"): cmd += ["-i", _expand(ssh["identity_file"])]
        cmd += ["-o", "StrictHostKeyChecking=accept-new", remote, str(local_dest)]
        print(("[dry-run] Would run:" if dry_run else "Running:"), " ".join(shlex.quote(c) for c in cmd))
        if not dry_run: subprocess.check_call(cmd)

def main(argv=None) -> int:
    P = Params()  # load parameters.yml via your production class

    # Helper that works whether Params exposes attributes or a dict-like .get()
    def g(key, default=None):
        return getattr(P, key, getattr(P, "get", lambda k, d=None: d)(key, default))

    ssh = {
        "host": g("ssh_host"),
        "user": g("ssh_user"),
        "port": int(g("ssh_port", 22)),
        "identity_file": g("ssh_identity_file"),
    }
    remote_base = g("remote_base_dir")
    patterns = list(g("export_patterns", []))
    local_dest = Path(_expand(g("local_dest_dir")) or "")

    parser = argparse.ArgumentParser(description="Export specific CSV files from VPS to local directory.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    # minimal validation
    if not all([ssh["host"], ssh["user"], remote_base, patterns, str(local_dest)]):
        print("Missing export parameters. Check parameters.yml (ssh_*, remote_base_dir, export_patterns, local_dest_dir).",
              file=sys.stderr)
        return 1

    local_dest.mkdir(parents=True, exist_ok=True)
    try:
        if _have("rsync"):
            _rsync_copy(ssh, remote_base, patterns, local_dest, args.dry_run)
        else:
            print("rsync not found, using scp fallback...", file=sys.stderr)
            _scp_copy(ssh, remote_base, patterns, local_dest, args.dry_run)
    except subprocess.CalledProcessError as e:
        print(f"Transfer failed (exit {e.returncode})", file=sys.stderr)
        return e.returncode
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
