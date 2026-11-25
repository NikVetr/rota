#!/usr/bin/env python3
"""Validate one cluster inventory and render immutable scheduler configuration."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex

RENDER_FORMAT = 5
HOST_KEYS = {"id", "ssh", "cpu_units", "memory_mib", "enabled"}
# Opt-in host settings. They add resource-table columns only when some host sets one.
HOST_OPTIONS = {"gpu_units": 0, "owner_idle_seconds": 0, "owner_active": "hold"}
ADDRESS = r"[A-Za-z0-9_][A-Za-z0-9_.:-]*"

def read_config(path):
    data = json.loads(Path(path).read_text())
    required = {"shared_root", "coordinator", "hosts"}
    optional = {"shared_io_capacity", "io_pressure_host", "network_interface", "disk", "backfill"}
    if not isinstance(data, dict) or not required <= data.keys() or data.keys() - required - optional:
        raise ValueError("config requires shared_root, coordinator, hosts; unknown keys are rejected")
    root = data["shared_root"]
    # The root reaches job tags and process names unquoted, so keep it shell-inert.
    if (not isinstance(root, str) or not re.fullmatch(r"(/[A-Za-z0-9._+=@,:%~^#-]+)+/?", root)
            or ".." in root.split("/") or "." in root.split("/")):
        raise ValueError("shared_root must be a normalized absolute directory using only "
                         "letters, digits and ._+=@,:%~^#- (no spaces, quotes or shell metacharacters)")
    data["shared_root"] = root.rstrip("/")
    hosts = data["hosts"]
    if not isinstance(hosts, list) or not hosts:
        raise ValueError("hosts must be a nonempty list")
    ids, targets, primaries, fallbacks = set(), set(), set(), []
    for host in hosts:
        if (not isinstance(host, dict) or not HOST_KEYS <= host.keys()
                or host.keys() - HOST_KEYS - {"fallback"} - HOST_OPTIONS.keys()):
            raise ValueError("each host requires id, ssh, cpu_units, memory_mib, enabled "
                             "(optional fallback, gpu_units, owner_idle_seconds, owner_active)")
        name, target = host["id"], host["ssh"]
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,31}", name):
            raise ValueError("host id must be 1–32 letters/digits/._- and start with a letter/digit")
        if not isinstance(target, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@:-]*", target):
            raise ValueError("ssh must be a hostname, SSH alias or user@host; put ports/keys in SSH config")
        if name in ids or target in targets:
            raise ValueError("host IDs and SSH targets must be unique")
        ids.add(name)
        targets.add(target)
        primaries.add(target.rpartition("@")[2])
        if "fallback" in host:
            # A second network (such as Tailscale) for scheduler SSH when the primary is down.
            if not isinstance(host["fallback"], str) or not re.fullmatch(ADDRESS, host["fallback"]):
                raise ValueError(f"{name}: fallback must be a hostname or address")
            fallbacks.append(host["fallback"])
        for field in ("cpu_units", "memory_mib"):
            if type(host[field]) is not int or host[field] <= 0:
                raise ValueError(f"{name}: {field} must be a positive integer")
        if type(host["enabled"]) is not bool:
            raise ValueError(f"{name}: enabled must be true or false")
        if "gpu_units" in host and (type(host["gpu_units"]) is not int or host["gpu_units"] < 0):
            raise ValueError(f"{name}: gpu_units must be a nonnegative integer")
        idle = host.get("owner_idle_seconds", 0)
        if type(idle) is not int or (idle and not 60 <= idle <= 86400):
            raise ValueError(f"{name}: owner_idle_seconds must be 0 (off) or 60..86400")
        if host.get("owner_active", "hold") not in {"hold", "suspend"}:
            raise ValueError(f"{name}: owner_active must be hold or suspend")
        if "owner_active" in host and not idle:
            raise ValueError(f"{name}: owner_active needs owner_idle_seconds")
    if len(set(fallbacks)) != len(fallbacks) or primaries & set(fallbacks):
        raise ValueError("fallback addresses must be unique and differ from every ssh host")
    if data["coordinator"] not in ids or not any(h["enabled"] for h in hosts):
        raise ValueError("coordinator must name a host and at least one worker must be enabled")
    capacity = data.setdefault("shared_io_capacity", 0)
    if type(capacity) is not int or capacity < 0:
        raise ValueError("shared_io_capacity must be a nonnegative integer")
    pressure = data.setdefault("io_pressure_host", "")
    if (capacity and pressure not in ids) or (not capacity and pressure):
        raise ValueError("positive I/O capacity requires an io_pressure_host; zero capacity requires none")
    # Opt-in: short jobs may use capacity held for a large job when forecast to finish first.
    if type(data.setdefault("backfill", False)) is not bool:
        raise ValueError("backfill must be true or false")
    for field in ("network_interface", "disk"):
        value = data.setdefault(field, "")
        if not isinstance(value, str) or (value and not re.fullmatch(r"[A-Za-z0-9_.:-]+", value)):
            raise ValueError(f"invalid {field}")
    return data


def resource_table(hosts):
    columns = ["host_id", "ssh_target", "cpu_units", "memory_mib", "enabled"]
    options = [key for key in HOST_OPTIONS if any(key in host for host in hosts)]
    if options:  # one consistent extended header; hosts without a setting get its default
        options = list(HOST_OPTIONS)
    rows = ["\t".join(columns + options)]
    for h in hosts:
        values = [h["id"], h["ssh"], h["cpu_units"], h["memory_mib"], int(h["enabled"])]
        values += [h.get(key, HOST_OPTIONS[key]) for key in options]
        rows.append("\t".join(str(value) for value in values))
    return "\n".join(rows) + "\n"


def render(data, parent):
    canonical = json.dumps(data, sort_keys=True, indent=2) + "\n"
    # Bump RENDER_FORMAT whenever rendered files change, so existing versions are never rewritten.
    digest = hashlib.sha256(f"{RENDER_FORMAT}\n{canonical}".encode()).hexdigest()[:16]
    directory = Path(parent).resolve() / digest
    hosts = data["hosts"]
    coordinator = next(h["ssh"] for h in hosts if h["id"] == data["coordinator"])
    env = {
        "PARALLEL_WORKERS_CONFIG_DIR": str(directory),
        "PARALLEL_WORKERS_ROOT": data["shared_root"],
        "PARALLEL_WORKERS_COORDINATOR": coordinator,
        "PARALLEL_WORKERS_HOSTS_FILE": str(directory / "hosts.txt"),
        "PARALLEL_WORKERS_RESOURCES_FILE": str(directory / "adaptive-resources.tsv"),
        "ADAPTIVE_CLUSTER_HOSTS_FILE": str(directory / "hosts.txt"),
        "ADAPTIVE_SHARED_IO_CAPACITY": str(data["shared_io_capacity"]),
        "ADAPTIVE_IO_PRESSURE_HOST": data["io_pressure_host"],
        "ADAPTIVE_BACKFILL": str(int(data["backfill"])),
        "PARALLEL_WORKERS_NETWORK_INTERFACE": data["network_interface"],
        "PARALLEL_WORKERS_DISK": data["disk"],
    }
    files = {
        "cluster.json": canonical,
        "hosts.txt": "".join(f"{h['ssh']} {h['cpu_units'] if h['enabled'] else 0} {h['id']}\n" for h in hosts),
        "adaptive-resources.tsv": resource_table(hosts),
        "environment.sh": "# Generated from cluster.json; source this on admin and coordinator hosts.\n" + "".join(
            f"export {key}={shlex.quote(value)}\n" for key, value in env.items()),
        "monitor-environment.json": json.dumps(env, indent=2) + "\n",
        # Installed into ~/.ssh by admin/install_ssh_routes.sh; ssh keeps checking each host
        # key under the primary name, so a fallback network cannot impersonate a host.
        "ssh_config": "# Generated from cluster.json; installed by admin/install_ssh_routes.sh.\n" + "".join(
            f"Host {h['ssh'].rpartition('@')[2]}\n"
            f"  ProxyCommand ~/.ssh/parallel-workers-route.sh %h %p {h['fallback']}\n"
            for h in hosts if "fallback" in h),
    }
    directory.mkdir(parents=True, exist_ok=True)
    for name, contents in files.items():
        path = directory / name
        if path.exists():
            if path.read_text() != contents:
                raise ValueError(f"generated configuration was modified: {path}")
        else:
            with path.open("x") as handle:
                handle.write(contents)
            path.chmod(0o644)
    return directory / "environment.sh"


def load_monitor_environment():
    """Use the installer's local copy even when the share or login shell is unavailable."""
    config_home = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    try:
        settings = json.loads((config_home / "parallel-workers/monitor-environment.json").read_text())
    except FileNotFoundError:
        return
    for key, value in settings.items():
        if key.startswith("PARALLEL_WORKERS_"):
            os.environ.setdefault(key, value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--output-dir", type=Path, help="write versioned files here and print the source command")
    output.add_argument("--render", action="store_true",
                        help="write under the inventory's shared_root/hosts/config and print the environment.sh path")
    args = parser.parse_args()
    try:
        data = read_config(args.config)
        if args.render:
            shared = Path(data["shared_root"])
            if not shared.is_dir():
                raise ValueError(f"shared_root is unavailable: {shared}. Mount it before rendering.")
            print(render(data, shared / "hosts/config"))
        elif args.output_dir:
            print(f"source {shlex.quote(str(render(data, args.output_dir)))}")
        else:
            print(f"Valid configuration: {len(data['hosts'])} hosts; coordinator {data['coordinator']}")
    except (OSError, ValueError, TypeError) as error:
        parser.exit(2, f"cluster config: {error}\n")


if __name__ == "__main__":
    main()
