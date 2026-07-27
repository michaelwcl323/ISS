#!/usr/bin/env python3
"""Run ISS experiments.

Edit the parameters at the beginning of ``local()``, then run:

    python3 cloudlab_execution/run_experiment.py --local
"""

from __future__ import annotations

import argparse
import base64
import csv
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time


ISS_IMPORT_PATH = Path("src/github.com/hyperledger-labs/mirbft")
LOCAL_GENERATOR = Path(
    "deployment/scripts/experiment-configuration/generate-local-config.sh"
)
CLOUDLAB_SETTINGS = Path(__file__).resolve().parent / "cloudlab_settings.json"


@dataclass(frozen=True)
class LocalConfig:
    peers: int
    failures: int
    clients: int
    duration: int
    throughput: int
    orderer: str
    batch_size: int
    segment_length: int
    view_change_timeout: int
    leader_policy: str
    authentication: bool


@dataclass(frozen=True)
class CloudLabHost:
    hostname: str
    username: str
    port: int
    region: str
    private_hostname: str


def local(*, dry_run: bool = False) -> None:
    """Configure and execute one local experiment.

    Change the values in this block. All peers, clients, and the discovery
    master will run on the current machine.
    """

    # ================================================================
    # EDIT LOCAL EXPERIMENT PARAMETERS HERE
    # ================================================================
    config = LocalConfig(
        peers=4,                   # Number of non-faulty peers
        failures=0,                # Additional faulty peers
        clients=4,                 # Client processes
        duration=60,               # Experiment duration, seconds
        throughput=20000,          # Target throughput, requests/second
        orderer="Pbft",            # Pbft, HotStuff, Raft, or Dummy
        batch_size=4096,           # Maximum requests per batch
        segment_length=32,         # Entries per segment
        view_change_timeout=60000, # Milliseconds
        leader_policy="Simple",    # Simple, Single, Backoff, etc.
        authentication=True,       # Authenticate client requests
    )
    # ================================================================
    # END OF PARAMETERS
    # ================================================================

    validate_local_config(config)
    repository, gopath = locate_iss()
    deployment_directory = repository / "deployment"
    base_generator = repository / LOCAL_GENERATOR
    generated_config = create_local_generator(base_generator.read_text(), config)

    environment = os.environ.copy()
    environment["GOPATH"] = str(gopath)
    environment["GO111MODULE"] = "off"
    environment["PATH"] = f"{gopath / 'bin'}:{environment.get('PATH', '')}"

    generated_directory = deployment_directory / ".generated-configs"
    generated_name = "local-config.generated.sh"
    generated_path = generated_directory / generated_name
    relative_generated_path = generated_path.relative_to(deployment_directory)
    command = [
        "./deploy.sh",
        "local",
        "new",
        str(relative_generated_path),
    ]

    print_local_summary(config, repository, command)
    if dry_run:
        print("\nDry run: the experiment was not started.")
        return

    generated_directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=generated_directory,
        prefix="local-config-",
        suffix=".sh",
        delete=False,
    ) as temporary:
        temporary.write(generated_config)
        actual_generated_path = Path(temporary.name)

    actual_generated_path.chmod(0o755)
    actual_relative_path = actual_generated_path.relative_to(deployment_directory)
    command[-1] = str(actual_relative_path)

    try:
        result_directory = run_deployment(
            command,
            deployment_directory,
            environment,
        )
        print_result_summary(result_directory)
    finally:
        actual_generated_path.unlink(missing_ok=True)


def remote(*, dry_run: bool = False, install: bool = False) -> None:
    """Run an experiment on hosts from cloudlab_settings.json."""

    # ================================================================
    # EDIT REMOTE EXPERIMENT PARAMETERS HERE
    # The controller below runs both master and client. Every entry in
    # cloudlab_settings.json is used as a consensus peer.
    # ================================================================
    controller_hostname = "10.10.1.11"
    controller_private_hostname = "10.10.1.11"

    config = LocalConfig(
        peers=10,
        failures=0,
        clients=10,                 # Client processes on 10.10.1.11
        duration=60,
        throughput=60000,
        orderer="HotStuff",
        batch_size=4096,
        segment_length=16,
        view_change_timeout=60000,
        leader_policy="Simple",
        authentication=True,
    )
    # ================================================================

    validate_local_config(config)
    settings = load_cloudlab_settings()
    key_path = Path(settings["key"]["path"]).expanduser().resolve()
    hosts = parse_cloudlab_hosts(settings)
    peer_count = config.peers + config.failures
    if len(hosts) < peer_count:
        raise ValueError(
            f"Remote experiment needs {peer_count} peer hosts "
            f"({config.peers} correct + {config.failures} faulty), "
            f"but settings contains {len(hosts)}"
        )
    peers = hosts[:peer_count]
    template_host = peers[0]
    master = CloudLabHost(
        hostname=controller_hostname,
        username=template_host.username,
        port=template_host.port,
        region=template_host.region,
        private_hostname=controller_private_hostname,
    )
    client = master
    selected = [master, *peers]
    ensure_uniform_ssh(selected)
    if install:
        install_remote_environments(
            selected,
            key_path,
            settings,
            dry_run=dry_run,
        )
    if install and dry_run:
        remote_home = remote_home_directory(master, key_path)
    else:
        remote_home = preflight_remote_hosts(
            selected,
            key_path,
            dry_run=dry_run,
        )

    repository = Path(__file__).resolve().parents[1]
    deployment_directory = repository / "deployment"
    base_generator = repository / LOCAL_GENERATOR
    generated_config = create_local_generator(base_generator.read_text(), config)

    print_remote_summary(config, key_path, master, peers, client)
    if dry_run:
        print("\nDry run: SSH was checked, but the experiment was not started.")
        return

    result_directory = run_direct_remote_experiment(
        repository=repository,
        deployment_directory=deployment_directory,
        generated_config=generated_config,
        config=config,
        settings=settings,
        key_path=key_path,
        master=master,
        peers=peers,
        remote_home=remote_home,
    )
    print_result_summary(result_directory)


def load_cloudlab_settings() -> dict:
    if not CLOUDLAB_SETTINGS.is_file():
        raise FileNotFoundError(
            f"CloudLab settings not found: {CLOUDLAB_SETTINGS}"
        )
    with CLOUDLAB_SETTINGS.open() as source:
        settings = json.load(source)
    if "key" not in settings or "path" not in settings["key"]:
        raise ValueError("cloudlab_settings.json is missing key.path")
    if not isinstance(settings.get("hosts"), list) or not settings["hosts"]:
        raise ValueError("cloudlab_settings.json must contain a non-empty hosts list")
    key_path = Path(settings["key"]["path"]).expanduser()
    if not key_path.is_file():
        raise FileNotFoundError(f"SSH private key not found: {key_path}")
    return settings


def parse_cloudlab_hosts(settings: dict) -> list[CloudLabHost]:
    result = []
    for index, raw in enumerate(settings["hosts"]):
        try:
            hostname = str(raw["hostname"])
            username = str(raw["username"])
        except KeyError as error:
            raise ValueError(f"hosts[{index}] is missing {error.args[0]}") from error
        result.append(
            CloudLabHost(
                hostname=hostname,
                username=username,
                port=int(raw.get("port", 22)),
                region=str(raw.get("region", "cloudlab")),
                private_hostname=str(raw.get("private_hostname", hostname)),
            )
        )
    return result


def ensure_uniform_ssh(hosts: list[CloudLabHost]) -> None:
    users = {host.username for host in hosts}
    ports = {host.port for host in hosts}
    if len(users) != 1:
        raise ValueError("All selected hosts must use the same SSH username")
    if len(ports) != 1:
        raise ValueError("All selected hosts must use the same SSH port")


def ssh_command(key_path: Path, host: CloudLabHost, remote_command: str) -> list[str]:
    return [
        "ssh",
        "-A",
        "-i",
        str(key_path),
        "-p",
        str(host.port),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=8",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        f"{host.username}@{host.hostname}",
        remote_command,
    ]


def unique_hosts(hosts: list[CloudLabHost]) -> list[CloudLabHost]:
    result = []
    seen = set()
    for host in hosts:
        identity = (host.username, host.hostname, host.port)
        if identity not in seen:
            seen.add(identity)
            result.append(host)
    return result


def remote_home_directory(host: CloudLabHost, key_path: Path) -> str:
    completed = subprocess.run(
        ssh_command(key_path, host, "printf '%s' \"$HOME\""),
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        detail = completed.stderr.strip() or "could not determine remote HOME"
        raise RuntimeError(f"{host.hostname}: {detail}")
    return completed.stdout.strip()


def install_remote_environments(
    hosts: list[CloudLabHost],
    key_path: Path,
    settings: dict,
    *,
    dry_run: bool,
) -> None:
    """Clone ISS and execute setup_environment.py on every selected host."""

    repository = settings.get("repo")
    if not isinstance(repository, dict):
        raise ValueError("cloudlab_settings.json is missing repo")
    try:
        repository_name = str(repository["name"])
        repository_url = str(repository["url"])
        repository_branch = str(repository["branch"])
    except KeyError as error:
        raise ValueError(f"cloudlab_settings repo is missing {error.args[0]}") from error

    hosts = unique_hosts(hosts)
    print(f"\nInstalling ISS environment on {len(hosts)} CloudLab hosts...")
    for number, host in enumerate(hosts, start=1):
        print(
            f"\n[{number}/{len(hosts)}] Installing "
            f"{host.username}@{host.hostname}",
            flush=True,
        )
        home = remote_home_directory(host, key_path)
        bootstrap_directory = f"{home}/.cache/iss-cloudlab"
        source_directory = f"{bootstrap_directory}/{repository_name}"
        remote_setup_script = (
            f"{source_directory}/cloudlab_execution/setup_environment.py"
        )

        quoted_bootstrap = shlex.quote(bootstrap_directory)
        quoted_source = shlex.quote(source_directory)
        quoted_url = shlex.quote(repository_url)
        quoted_branch = shlex.quote(repository_branch)
        clone_or_update = (
            f"mkdir -p {quoted_bootstrap} && "
            f"if [ -d {quoted_source}/.git ]; then "
            f"git -C {quoted_source} fetch origin {quoted_branch} && "
            f"git -C {quoted_source} checkout {quoted_branch} && "
            f"git -C {quoted_source} merge --ff-only origin/{quoted_branch}; "
            f"else git clone --branch {quoted_branch} --single-branch "
            f"{quoted_url} {quoted_source}; fi"
        )

        if dry_run:
            print("+ " + " ".join(ssh_command(
                key_path, host, clone_or_update
            )))
            print(
                f"+ ssh ... python3 {remote_setup_script} "
                f"--source {source_directory} --gopath {home}/iss-gopath"
            )
            continue

        subprocess.run(
            ssh_command(key_path, host, clone_or_update),
            check=True,
        )
        subprocess.run(
            ssh_command(
                key_path,
                host,
                f"test -f {shlex.quote(remote_setup_script)} || "
                f"{{ echo 'Missing {remote_setup_script} in GitHub checkout' "
                f">&2; exit 20; }}",
            ),
            check=True,
        )
        setup_command = (
            f"python3 {shlex.quote(remote_setup_script)} "
            f"--source {quoted_source} "
            f"--gopath {shlex.quote(home + '/iss-gopath')}"
        )
        subprocess.run(
            ssh_command(key_path, host, setup_command),
            check=True,
        )


def preflight_remote_hosts(
    hosts: list[CloudLabHost],
    key_path: Path,
    *,
    dry_run: bool,
) -> str:
    print("\nChecking CloudLab hosts...")
    homes = set()
    for index, host in enumerate(hosts):
        command = ssh_command(
            key_path,
            host,
            "printf '%s\\n' \"$HOME\"; "
            "test -x \"$HOME/.local/toolchains/go1.21.2/bin/go\" "
            "&& echo go=ok || echo go=missing; "
            "command -v protoc >/dev/null "
            "&& echo protoc=ok || echo protoc=missing; "
            "test -x \"$HOME/iss-gopath/bin/discoveryslave\" "
            "&& echo discoveryslave=ok || echo discoveryslave=missing",
        )
        print(f"  [{index}] {host.username}@{host.hostname}:{host.port}", end="")
        if dry_run:
            # A dry run still checks connectivity and required tools.
            pass
        completed = subprocess.run(
            command,
            text=True,
            capture_output=True,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(
                f"SSH/environment check failed for {host.hostname}: {detail}"
            )
        lines = [line for line in completed.stdout.splitlines() if line.strip()]
        checks = set(lines[1:])
        missing = [
            name
            for name in ("go", "protoc", "discoveryslave")
            if f"{name}=ok" not in checks
        ]
        if len(lines) < 4 or missing:
            raise RuntimeError(
                f"{host.hostname} is missing: {', '.join(missing) or 'unknown'}. "
                "Run with --remote --install (without --dry-run) first."
            )
        homes.add(lines[0])
        print("  OK")
    if len(homes) != 1:
        raise ValueError(f"Selected hosts have different home directories: {homes}")
    return homes.pop()


def instance_line(identifier: str, host: CloudLabHost, tag: str) -> str:
    return (
        f"{identifier} {host.hostname} {host.private_hostname} "
        f"{tag} {host.region}\n"
    )


def print_remote_summary(
    config: LocalConfig,
    key_path: Path,
    master: CloudLabHost,
    peers: list[CloudLabHost],
    client: CloudLabHost,
) -> None:
    print("\nISS remote experiment")
    print(f"  Settings:          {CLOUDLAB_SETTINGS}")
    print(f"  SSH key:           {key_path}")
    print(f"  Master:            {master.hostname}")
    print(f"  Peers:             {len(peers)}")
    for index, host in enumerate(peers):
        print(f"    peer {index:<2}         {host.private_hostname}")
    print(f"  Client:            {client.hostname}")
    print(f"  Duration:          {config.duration} seconds")
    print(f"  Target throughput: {config.throughput} req/s")
    print(f"  Protocol:          {config.orderer}")


def next_direct_remote_directory(deployment_directory: Path) -> Path:
    root = deployment_directory / "deployment-data"
    root.mkdir(parents=True, exist_ok=True)
    numbers = []
    for path in root.glob("direct-remote-*"):
        try:
            numbers.append(int(path.name.rsplit("-", 1)[1]))
        except ValueError:
            continue
    return root / f"direct-remote-{max(numbers, default=-1) + 1:04d}"


def patch_master_commands(
    template: str,
    *,
    master_ip: str,
    master_port: int,
    run_directory: Path,
    peer_binary: str,
    client_binary: str,
) -> str:
    """Remove all legacy SCP commands and use node-local config/results."""

    replacements = {
        "$own_public_ip": master_ip,
        "$master_port": str(master_port),
        "$status_file": str(run_directory / "status"),
        "$ready_file": str(run_directory / "master-ready"),
        "$ssh_key_file": "",
    }
    for old, new in replacements.items():
        template = template.replace(old, new)

    template = re.sub(
        r"(?m)^(exec-start\s+peers\s+\S+\s+)orderingpeer(?=\s)",
        rf"\g<1>{peer_binary}",
        template,
    )
    template = re.sub(
        r"(?m)^(exec-start\s+\S*client\s+\S+\s+)orderingclient(?=\s)",
        rf"\g<1>{client_binary}",
        template,
    )

    output = []
    for line in template.splitlines():
        if "stubborn-scp.sh" in line and "-config.log" in line:
            match = re.match(r"exec-start\s+(\S+)", line)
            if not match:
                raise RuntimeError(f"Cannot patch config command: {line}")
            tag = match.group(1)
            output.append(
                f"exec-start {tag} /dev/null "
                "cp config/config-0000.yml config/config.yml"
            )
        elif "stubborn-scp.sh" in line and "-logs.log" in line:
            match = re.match(r"exec-start\s+(\S+)", line)
            if not match:
                raise RuntimeError(f"Cannot patch log command: {line}")
            output.append(f"exec-start {match.group(1)} /dev/null true")
        elif re.match(r"exec-start\s+\S+\s+\S+\s+tc\s+qdisc\s+", line):
            match = re.match(r"exec-start\s+(\S+)\s+(\S+)", line)
            if not match:
                raise RuntimeError(f"Cannot patch bandwidth command: {line}")
            output.append(f"exec-start {match.group(1)} {match.group(2)} true")
        else:
            output.append(line)
    return "\n".join(output) + "\n"


def disable_direct_remote_tls(run_directory: Path) -> None:
    """Match Ladon's direct CloudLab mode without changing ISS defaults."""

    config_files = sorted((run_directory / "config").glob("config-*.yml"))
    if not config_files:
        raise RuntimeError("The generator did not produce any ISS config files")

    for config_file in config_files:
        contents = config_file.read_text()
        updated, count = re.subn(
            r"(?m)^UseTLS:\s+\S+",
            "UseTLS:               false",
            contents,
            count=1,
        )
        if count != 1:
            raise RuntimeError(f"Missing UseTLS setting in {config_file}")
        config_file.write_text(updated)


def prepare_direct_remote_files(
    deployment_directory: Path,
    generated_config: str,
    run_directory: Path,
    master_ip: str,
    master_port: int,
    peer_binary: str,
    client_binary: str,
) -> None:
    run_directory.mkdir(parents=True)
    generator = run_directory / "remote-config.generated.sh"
    generator.write_text(generated_config)
    generator.chmod(0o755)

    subprocess.run(
        [str(generator), str(run_directory), "0"],
        cwd=deployment_directory,
        check=True,
    )
    disable_direct_remote_tls(run_directory)
    subprocess.run(
        [
            "python3",
            "scripts/generate-master-commands.py",
            "remote",
            str(run_directory / "deployment.dpl"),
            str(run_directory / "master-commands-template.cmd"),
            str(run_directory),
        ],
        cwd=deployment_directory,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    template = (run_directory / "master-commands-template.cmd").read_text()
    commands = patch_master_commands(
        template,
        master_ip=master_ip,
        master_port=master_port,
        run_directory=run_directory,
        peer_binary=peer_binary,
        client_binary=client_binary,
    )
    commands += f"\nwrite-file {run_directory / 'status'} DONE\n"
    (run_directory / "master-commands.cmd").write_text(commands)


def remote_source_directory(settings: dict, remote_home: str) -> str:
    repository_name = str(settings["repo"]["name"])
    return f"{remote_home}/.cache/iss-cloudlab/{repository_name}"


def prepare_peer_command(
    *,
    remote_home: str,
    remote_source: str,
    remote_run: str,
    config_data: str,
    client_public_key: bytes,
    host: CloudLabHost,
) -> str:
    encoded_config = base64.b64encode(config_data.encode()).decode()
    encoded_client_public_key = base64.b64encode(client_public_key).decode()
    peer_binary = f"{remote_home}/iss-gopath/bin/orderingpeer"
    peer_pattern = f"^{re.escape(peer_binary)}( |$)"
    return " && ".join(
        [
            (
                f"pkill -TERM -f {shlex.quote(peer_pattern)} "
                "2>/dev/null || true"
            ),
            f"rm -rf {shlex.quote(remote_run)}",
            f"mkdir -p {shlex.quote(remote_run + '/config')}",
            (
                f"printf %s {shlex.quote(encoded_config)} | base64 -d > "
                f"{shlex.quote(remote_run + '/config/config-0000.yml')}"
            ),
            (
                f"cp -r {shlex.quote(remote_source + '/tls-data')} "
                f"{shlex.quote(remote_run + '/tls-data')}"
            ),
            f"cd {shlex.quote(remote_run + '/tls-data')}",
            f"./generate.sh -f {shlex.quote(host.hostname)} "
            f"{shlex.quote(host.private_hostname)}",
            (
                f"printf %s {shlex.quote(encoded_client_public_key)} | "
                "base64 -d > client-ecdsa-256.pem"
            ),
        ]
    )


def direct_slave_command(
    remote_home: str,
    remote_run: str,
    host: CloudLabHost,
    master_ip: str,
    master_port: int,
) -> str:
    binary_directory = f"{remote_home}/iss-gopath/bin"
    binary = f"{binary_directory}/discoveryslave"
    inner = (
        f"cd {shlex.quote(remote_run)} && "
        f"export PATH={shlex.quote(binary_directory)}:$PATH && "
        f"echo $$ > control.pid && "
        f"exec {shlex.quote(binary)} peers "
        f"{shlex.quote(master_ip + ':' + str(master_port))} "
        f"{shlex.quote(host.hostname)} {shlex.quote(host.private_hostname)}"
    )
    return f"bash -lc {shlex.quote(inner)}"


def remote_cleanup_command(remote_home: str, remote_run: str) -> str:
    peer_binary = f"{remote_home}/iss-gopath/bin/orderingpeer"
    peer_pattern = f"^{re.escape(peer_binary)}( |$)"
    return "; ".join(
        [
            (
                f"pkill -TERM -f {shlex.quote(peer_pattern)} "
                "2>/dev/null || true"
            ),
            (
                f"test -f {shlex.quote(remote_run + '/control.pid')} && "
                f"kill $(cat {shlex.quote(remote_run + '/control.pid')}) "
                "2>/dev/null || true"
            ),
        ]
    )


def collect_peer_results(
    peers: list[CloudLabHost],
    key_path: Path,
    remote_run: str,
    run_directory: Path,
) -> None:
    for index, host in enumerate(peers, start=1):
        print(f"Collecting peer {index}/{len(peers)}: {host.hostname}")
        archive = run_directory / f"peer-{index}.tar.gz"
        command = (
            f"tar -C {shlex.quote(remote_run)} -czf - experiment-output "
            "2>/dev/null"
        )
        with archive.open("wb") as output:
            completed = subprocess.run(
                ssh_command(key_path, host, command),
                stdout=output,
            )
        if completed.returncode != 0:
            raise RuntimeError(f"Could not collect results from {host.hostname}")
        with tarfile.open(archive, "r:gz") as source:
            source.extractall(run_directory)
        archive.unlink()


def analyze_direct_results(
    deployment_directory: Path,
    run_directory: Path,
) -> None:
    experiment_output = run_directory / "experiment-output/0000"
    if not experiment_output.is_dir():
        raise RuntimeError(f"No experiment output found at {experiment_output}")
    subprocess.run(
        [
            "scripts/analyze/analyze-parallel.sh",
            "-q",
            "queries/ethereum.sql",
            "-q",
            "queries/aggregates.sql",
            "-q",
            "queries/histograms.sql",
            str(experiment_output),
        ],
        cwd=deployment_directory,
        check=True,
    )
    with (run_directory / "result-summary.csv").open("w") as summary:
        subprocess.run(
            [
                "scripts/analyze/summarize.sh",
                str(run_directory / "deployment.csv"),
                str(run_directory / "experiment-output"),
            ],
            cwd=deployment_directory,
            stdout=summary,
            check=True,
        )


def run_direct_remote_experiment(
    *,
    repository: Path,
    deployment_directory: Path,
    generated_config: str,
    config: LocalConfig,
    settings: dict,
    key_path: Path,
    master: CloudLabHost,
    peers: list[CloudLabHost],
    remote_home: str,
) -> Path:
    """Run without deploy.sh or any code/binary SCP/rsync."""

    master_port = int(settings.get("port", 9999))
    run_directory = next_direct_remote_directory(deployment_directory)
    local_gopath = Path.home() / "iss-gopath"
    remote_binary_directory = f"{remote_home}/iss-gopath/bin"
    prepare_direct_remote_files(
        deployment_directory,
        generated_config,
        run_directory,
        master.private_hostname,
        master_port,
        peer_binary=f"{remote_binary_directory}/orderingpeer",
        client_binary=str(local_gopath / "bin/orderingclient"),
    )
    config_file = run_directory / "config/config-0000.yml"
    config_data = config_file.read_text()

    master_binary = local_gopath / "bin/discoverymaster"
    slave_binary = local_gopath / "bin/discoveryslave"
    for binary in (master_binary, slave_binary):
        if not binary.is_file():
            raise RuntimeError(f"Local binary not found: {binary}")

    shutil.copytree(
        repository / "tls-data",
        run_directory / "tls-data",
        dirs_exist_ok=True,
    )
    subprocess.run(
        [
            str(run_directory / "tls-data/generate.sh"),
            "-f",
            master.hostname,
            master.private_hostname,
        ],
        cwd=run_directory / "tls-data",
        check=True,
    )
    client_public_key_file = (
        run_directory / "tls-data/client-ecdsa-256.pem"
    )
    client_public_key = client_public_key_file.read_bytes()

    remote_source = remote_source_directory(settings, remote_home)
    run_name = run_directory.name
    remote_run = f"{remote_home}/.cache/iss-cloudlab/runs/{run_name}"
    peer_processes: list[tuple[CloudLabHost, subprocess.Popen, object]] = []
    master_process: subprocess.Popen | None = None
    client_process: subprocess.Popen | None = None

    try:
        print("\nPreparing node-local peer directories...")
        for host in peers:
            subprocess.run(
                ssh_command(
                    key_path,
                    host,
                    prepare_peer_command(
                        remote_home=remote_home,
                        remote_source=remote_source,
                        remote_run=remote_run,
                        config_data=config_data,
                        client_public_key=client_public_key,
                        host=host,
                    ),
                ),
                check=True,
            )

        master_log = (run_directory / "master.log").open("w")
        master_process = subprocess.Popen(
            [
                str(master_binary),
                str(master_port),
                "file",
                str(run_directory / "master-commands.cmd"),
            ],
            cwd=run_directory,
            stdout=master_log,
            stderr=subprocess.STDOUT,
        )
        time.sleep(1)

        print(f"Starting {len(peers)} remote peers...")
        for index, host in enumerate(peers, start=1):
            log = (run_directory / f"peer-ssh-{index}.log").open("w")
            process = subprocess.Popen(
                ssh_command(
                    key_path,
                    host,
                    direct_slave_command(
                        remote_home,
                        remote_run,
                        host,
                        master.private_hostname,
                        master_port,
                    ),
                ),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            peer_processes.append((host, process, log))

        client_log = (run_directory / "client-slave.log").open("w")
        client_environment = os.environ.copy()
        client_environment["PATH"] = (
            f"{local_gopath / 'bin'}:{client_environment.get('PATH', '')}"
        )
        client_process = subprocess.Popen(
            [
                str(slave_binary),
                "1client",
                f"{master.private_hostname}:{master_port}",
                master.hostname,
                master.private_hostname,
            ],
            cwd=run_directory,
            env=client_environment,
            stdout=client_log,
            stderr=subprocess.STDOUT,
        )

        timeout = config.duration + 300
        master_return = master_process.wait(timeout=timeout)
        if master_return != 0:
            raise RuntimeError(
                f"Discovery master exited with {master_return}; "
                f"see {run_directory / 'master.log'}"
            )
        client_process.wait(timeout=30)
        for host, process, _log in peer_processes:
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired as error:
                raise RuntimeError(f"Peer did not stop: {host.hostname}") from error

        collect_peer_results(peers, key_path, remote_run, run_directory)
        analyze_direct_results(deployment_directory, run_directory)
        return run_directory
    finally:
        for process in (client_process, master_process):
            if process is not None and process.poll() is None:
                process.terminate()
        for host, process, log in peer_processes:
            if process.poll() is None:
                process.terminate()
            log.close()
            subprocess.run(
                ssh_command(
                    key_path,
                    host,
                    remote_cleanup_command(remote_home, remote_run),
                ),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )


def validate_local_config(config: LocalConfig) -> None:
    positive_values = {
        "peers": config.peers,
        "clients": config.clients,
        "duration": config.duration,
        "throughput": config.throughput,
        "batch_size": config.batch_size,
        "segment_length": config.segment_length,
        "view_change_timeout": config.view_change_timeout,
    }
    for name, value in positive_values.items():
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero")

    if config.failures < 0:
        raise ValueError("failures cannot be negative")

    valid_orderers = {"Pbft", "HotStuff", "Raft", "Dummy"}
    if config.orderer not in valid_orderers:
        raise ValueError(
            f"orderer must be one of: {', '.join(sorted(valid_orderers))}"
        )
    valid_policies = {"Simple", "Single", "Backoff", "Blacklist", "Combined"}
    if config.leader_policy not in valid_policies:
        raise ValueError(
            "leader_policy must be one of: "
            + ", ".join(sorted(valid_policies))
        )


def locate_iss() -> tuple[Path, Path]:
    """Locate the GOPATH copy produced by setup_environment.py."""

    gopath_value = os.environ.get("GOPATH")
    gopath = (
        Path(gopath_value).expanduser()
        if gopath_value
        else Path.home() / "iss-gopath"
    ).resolve()
    repository = gopath / ISS_IMPORT_PATH

    if not (repository / LOCAL_GENERATOR).is_file():
        raise FileNotFoundError(
            f"ISS was not found at {repository}. Run "
            "'python3 cloudlab_execution/setup_environment.py' first, "
            "then run 'source ~/.bashrc'."
        )
    return repository, gopath


def replace_scalar(text: str, name: str, value: object) -> str:
    pattern = re.compile(rf"^{re.escape(name)}=.*$", re.MULTILINE)
    updated, count = pattern.subn(f'{name}="{value}"', text, count=1)
    if count != 1:
        raise RuntimeError(f"Missing setting in generator: {name}")
    return updated


def replace_array(text: str, name: str, value: object) -> str:
    pattern = re.compile(rf"^{re.escape(name)}=\([^)]*\).*$", re.MULTILINE)
    updated, count = pattern.subn(f"{name}=({value})", text, count=1)
    if count != 1:
        raise RuntimeError(f"Missing array setting in generator: {name}")
    return updated


def boolean(value: bool) -> str:
    return str(value).lower()


def replace_throughput(text: str, config: LocalConfig) -> str:
    authentication = "Auth" if config.authentication else "NoAuth"
    single = "Single" if config.leader_policy == "Single" else ""
    variable = f"throughputs{authentication}{single}{config.orderer}"
    setting = f"{variable}[{config.peers}]"
    pattern = re.compile(rf"^{re.escape(setting)}=.*$", re.MULTILINE)
    updated, count = pattern.subn(
        f'{setting}="{config.throughput}"',
        text,
        count=1,
    )
    if count == 0:
        # The original scripts predeclare only 4/8/16/32/64/128 peers.
        # CloudLab topologies may use other sizes, such as 10 peers.
        declaration = re.compile(
            rf"^({re.escape(variable)}=.*)$",
            re.MULTILINE,
        )
        updated, declaration_count = declaration.subn(
            rf'\1\n{setting}="{config.throughput}"',
            text,
            count=1,
        )
        if declaration_count != 1:
            raise RuntimeError(
                f"The base generator does not define throughput array {variable}"
            )
    return updated


def create_local_generator(text: str, config: LocalConfig) -> str:
    scalar_settings = {
        "clients1": config.clients,
        "clients16": "",
        "clients32": "",
        "systemSizes": config.peers,
        "durations": config.duration,
        "orderers": config.orderer,
        "batchsizes": config.batch_size,
        "segmentLengths": config.segment_length,
        "viewChangeTimeouts": config.view_change_timeout,
        "leaderPolicies": config.leader_policy,
        "auths": boolean(config.authentication),
        "crashTimings": "EpochEnd",
    }
    for name, value in scalar_settings.items():
        text = replace_scalar(text, name, value)

    text = replace_array(text, "failureCounts", config.failures)
    return replace_throughput(text, config)


def print_local_summary(
    config: LocalConfig,
    repository: Path,
    command: list[str],
) -> None:
    print("\nISS local experiment")
    print(f"  Repository:          {repository}")
    print(f"  Correct peers:       {config.peers}")
    print(f"  Faulty peers:        {config.failures}")
    print(f"  Clients:             {config.clients}")
    print(f"  Duration:            {config.duration} seconds")
    print(f"  Target throughput:   {config.throughput} req/s")
    print(f"  Orderer:             {config.orderer}")
    print(f"  Batch size:          {config.batch_size}")
    print(f"  Segment length:      {config.segment_length}")
    print(f"  Leader policy:       {config.leader_policy}")
    print(f"  Authentication:      {config.authentication}")
    print("\nCommand:")
    print("  " + " ".join(shlex.quote(part) for part in command))


def run_deployment(
    command: list[str],
    deployment_directory: Path,
    environment: dict[str, str],
) -> Path:
    """Run deploy.sh while hiding its two unreadable wide CSV lines."""

    process = subprocess.Popen(
        command,
        cwd=deployment_directory,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if process.stdout is None:
        raise RuntimeError("Could not capture deployment output")

    result_directory: Path | None = None
    summary_phase = False

    for line in process.stdout:
        stripped = line.rstrip("\n")
        if stripped == "Generating result summary.":
            summary_phase = True
            print(stripped, flush=True)
            continue

        # summarize.sh prints a 77-column header and row. Preserve them in
        # result-summary.csv, but do not flood the terminal with those lines.
        if summary_phase and stripped.count(",") > 20:
            continue

        print(stripped, flush=True)
        marker = "Done. Experiment data directory:"
        if stripped.startswith(marker):
            raw_path = stripped[len(marker):].strip()
            candidate = Path(raw_path)
            result_directory = (
                candidate
                if candidate.is_absolute()
                else deployment_directory / candidate
            )

    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    if result_directory is None:
        raise RuntimeError("Deployment completed without reporting a result directory")
    return result_directory.resolve()


def display_value(
    row: dict[str, str],
    key: str,
    *,
    unit: str = "",
    decimals: int = 2,
) -> str:
    raw = row.get(key, "").strip()
    if raw in {"", "None", "null", "NULL"}:
        return "N/A"
    try:
        number = float(raw)
    except ValueError:
        return raw
    value = f"{number:.{decimals}f}"
    return f"{value} {unit}".rstrip()


def print_result_summary(result_directory: Path) -> None:
    summary_file = result_directory / "result-summary.csv"
    if not summary_file.is_file():
        print(f"\nResult summary was not found: {summary_file}")
        return

    with summary_file.open(newline="") as source:
        rows = list(csv.DictReader(source))
    if not rows:
        print(f"\nResult summary contains no experiment rows: {summary_file}")
        return

    for row in rows:
        target_raw = row.get("target-throughput", "")
        actual_raw = row.get("throughput-raw", "")
        achievement = "N/A"
        try:
            target = float(target_raw)
            actual = float(actual_raw)
            if target:
                achievement = f"{actual / target * 100:.1f}%"
        except (TypeError, ValueError):
            pass

        truncated_available = (
            row.get("nreq-trunc", "").strip() not in {"", "0", "None"}
        )
        metrics = [
            ("Experiment", row.get("exp", "N/A")),
            ("Topology", f"{row.get('peers', '?')} peers, "
                         f"{row.get('clients', '?')} client machine(s)"),
            ("Protocol", f"{row.get('orderer', 'N/A')} / "
                         f"{row.get('leader-policy', 'N/A')} leaders"),
            ("Duration", display_value(row, "duration-raw", unit="s")),
            ("Target throughput", display_value(
                row, "target-throughput", unit="req/s", decimals=0
            )),
            ("Actual throughput", display_value(
                row, "throughput-raw", unit="req/s"
            )),
            ("Target achieved", achievement),
            ("Average latency", display_value(
                row, "latency-avg-raw", unit="ms"
            )),
            ("P95 latency", display_value(
                row, "latency-95pctile-raw", unit="ms"
            )),
            ("Latency stddev", display_value(
                row, "latency-stdev-raw", unit="ms"
            )),
            ("Sampled requests", display_value(
                row, "nreq-raw", decimals=0
            )),
            ("Proposal rate", display_value(
                row, "propose-rate-raw", unit="batch/s"
            )),
            ("Epochs min/avg/max",
             f"{display_value(row, 'epochs-min')} / "
             f"{display_value(row, 'epochs-avg')} / "
             f"{display_value(row, 'epochs-max')}"),
            ("View changes", display_value(
                row, "viewchanges-total", decimals=0
            )),
            ("Stable-window data", "available" if truncated_available else "N/A"),
        ]

        label_width = max(len(label) for label, _ in metrics)
        value_width = max(len(value) for _, value in metrics)
        border = f"+-{'-' * label_width}-+-{'-' * value_width}-+"

        print("\nExperiment result")
        print(border)
        for label, value in metrics:
            print(f"| {label:<{label_width}} | {value:<{value_width}} |")
        print(border)

    print(f"\nFull CSV: {summary_file}")
    if any(
        row.get("nreq-trunc", "").strip() in {"", "0", "None"}
        for row in rows
    ):
        print(
            "Note: stable-window metrics are unavailable. Use duration >= 30 "
            "seconds because the analyzer removes 5 seconds at each end."
        )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an ISS experiment.")
    parser.add_argument(
        "--local",
        action="store_true",
        help="run the configuration defined inside local()",
    )
    parser.add_argument(
        "--remote",
        action="store_true",
        help="run remote() using cloudlab_settings.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and display operations without executing the experiment",
    )
    parser.add_argument(
        "--install",
        action="store_true",
        help="with --remote, clone ISS and set up every CloudLab host first",
    )
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    if arguments.local == arguments.remote:
        print("Choose exactly one mode: --local or --remote", file=sys.stderr)
        return 2
    if arguments.install and not arguments.remote:
        print("--install can only be used with --remote", file=sys.stderr)
        return 2

    try:
        if arguments.local:
            local(dry_run=arguments.dry_run)
        else:
            remote(
                dry_run=arguments.dry_run,
                install=arguments.install,
            )
    except (
        FileNotFoundError,
        RuntimeError,
        ValueError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"\n[iss-runner] ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
