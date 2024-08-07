import asyncio
import json
import logging
import os
import shutil
from collections.abc import Iterable
from pathlib import Path

from .logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger())

async def init_sshd_service(child_env):
    if Path("/tmp/dropbear").is_dir():
        shutil.rmtree("/tmp/dropbear")
    Path("/tmp/dropbear").mkdir(parents=True, exist_ok=True)
    auth_path = Path("/home/work/.ssh/authorized_keys")
    if not auth_path.is_file():
        auth_path.parent.mkdir(parents=True, exist_ok=True)
        auth_path.parent.chmod(0o700)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"sshd init error: {stderr.decode('utf8')}")
        pub_key = stdout.splitlines()[1]
        auth_path.write_bytes(pub_key)
        auth_path.chmod(0o600)

        proc = await asyncio.create_subprocess_exec(
            *[
                "/opt/kernel/dropbearconvert",
                "dropbear",
                "openssh",
                "/tmp/dropbear/id_dropbear",
                "/home/work/id_container",
            ],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=child_env,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"sshd init error: {stderr.decode('utf8')}")
            stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"sshd init error: {stderr.decode('utf8')}")

    cluster_privkey_src_path = Path("/home/config/ssh/id_cluster")
    cluster_ssh_port_mapping_path = Path("/home/config/ssh/port-mapping.json")
    user_ssh_config_path = Path("/home/work/.ssh/config")
    if cluster_privkey_src_path.is_file():
        replicas = {
            k: v
            for k, v in map(
                lambda pair: pair.rsplit(":", maxsplit=1),
                os.environ.get("BACKENDAI_CLUSTER_REPLICAS", "main:1").split(","),
            )
        }
        if cluster_ssh_port_mapping_path.is_file():
            cluster_ssh_port_mapping = json.loads(cluster_ssh_port_mapping_path.read_text())
            with open(user_ssh_config_path, "a") as f:
                for host, (hostname, port) in cluster_ssh_port_mapping.items():
                    f.write(f"\nHost {host}\n")
                    f.write(f"\tHostName {hostname}\n")
                    f.write(f"\tPort {port}\n")
                    f.write("\tStrictHostKeyChecking no\n")
                    f.write("\tIdentityFile /home/config/ssh/id_cluster\n")
        else:
            for role_name, role_replica in replicas.items():
                try:
                    existing_ssh_config = user_ssh_config_path.read_text()
                    if "\nHost {role_name}*\n" in existing_ssh_config:
                        continue
                except FileNotFoundError:
                    pass
                with open(user_ssh_config_path, "a") as f:
                    f.write(f"\nHost {role_name}*\n")
                    f.write("\tPort 2200\n")
                    f.write("\tStrictHostKeyChecking no\n")
                    f.write("\tIdentityFile /home/config/ssh/id_cluster\n")
    cluster_pubkey_src_path = Path("/home/config/ssh/id_cluster.pub")
    if cluster_pubkey_src_path.is_file():
        pubkey = cluster_pubkey_src_path.read_bytes()
        with open(auth_path, "ab") as f:
            f.write(b"\n")
            f.write(pubkey)
            f.write(b"\n")


async def prepare_sshd_service(service_info):
    port_config = service_info["port"]
    if isinstance(port_config, Iterable):
        for port in port_config:
            cmdargs.extend(["-p", f"0.0.0.0:{port}"])
    else:
        cmdargs.extend(["-p", f"0.0.0.0:{port_config}"])
    env = {}
    return cmdargs, env


async def prepare_ttyd_service(service_info):
    shell = "sh"
    if Path("/bin/zsh").exists():
        shell = "zsh"
    elif Path("/bin/bash").exists():
        shell = "bash"
    elif Path("/bin/ash").exists():
        shell = "ash"

    cmdargs = ["/opt/backend.ai/bin/ttyd", "-p", service_info["port"], f"/bin/{shell}"]
    if shell != "ash":
        cmdargs += ["-c", f"export SHELL=/bin/{shell}; /opt/kernel/tmux -2 attach"]
    return cmdargs, {}