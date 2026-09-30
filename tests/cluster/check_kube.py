# Copyright 2026 Redpanda Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import pickle
import socket
import sys
import time

import pytest

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.cluster.kube import KubeCluster, KubectlError, KubectlRemoteAccount
from ducktape.cluster.remoteaccount import RemoteCommandError

# Stands in for kubectl: logs its argv, runs `exec` commands locally, and answers `get pod` from canned values.
# The pod named "missing" doesn't exist; "unready" exists but isn't Ready.
FAKE_KUBECTL = """#!%s
import json, os, subprocess, sys
args = sys.argv[1:]
with open(os.environ["FAKE_KUBECTL_LOG"], "a") as log:
    log.write(json.dumps(args) + "\\n")
get = "get" in args
pod = args[args.index("pod") + 1] if get else args[args.index("--") - 1]
if pod == "missing":
    sys.stderr.write('Error from server (NotFound): pods "missing" not found\\n')
    sys.exit(1)
if get:
    sys.stdout.write("10.1.2.3" if "jsonpath={.status.podIP}" in args else "False" if pod == "unready" else "True")
    sys.exit(0)
status = subprocess.call(args[args.index("--") + 1:])
if status:
    sys.stderr.write("command terminated with exit code %%d\\n" %% status)
sys.exit(status)
""" % sys.executable


@pytest.fixture
def kubectl_log(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(FAKE_KUBECTL)
    kubectl.chmod(0o755)
    log = tmp_path / "kubectl.log"
    monkeypatch.setenv("PATH", "%s%s%s" % (bin_dir, os.pathsep, os.environ["PATH"]))
    monkeypatch.setenv("FAKE_KUBECTL_LOG", str(log))
    return log


def kubectl_calls(log):
    return [json.loads(line) for line in log.read_text().splitlines()]


@pytest.fixture
def account(kubectl_log):
    return KubectlRemoteAccount("dt-node-0", namespace="ducktape-proto")


class CheckKubectlRemoteAccount(object):

    def check_exec_argv(self, kubectl_log):
        KubectlRemoteAccount("p", namespace="ns", container="c", kubeconfig="/kc").ssh("true")
        KubectlRemoteAccount("p").create_file("/dev/null", "x")
        calls = kubectl_calls(kubectl_log)
        assert calls[0] == ["--kubeconfig", "/kc", "--namespace", "ns", "exec", "-c", "c", "p", "--",
                            "bash", "-c", "true"]
        assert calls[1] == ["exec", "-i", "p", "--", "bash", "-c", "cat > /dev/null"]

    def check_ssh_exit_status(self, account):
        assert account.ssh("exit 0") == 0
        assert account.ssh("echo oops >&2; exit 3", allow_fail=True) == 3
        with pytest.raises(RemoteCommandError) as e:
            account.ssh("echo oops >&2; exit 3")
        assert e.value.exit_status == 3
        assert e.value.msg == b"oops\n"

    def check_kubectl_failure_is_not_a_command_failure(self, kubectl_log):
        missing = KubectlRemoteAccount("missing")
        with pytest.raises(KubectlError, match="NotFound"):
            missing.ssh("true", allow_fail=True)
        with pytest.raises(KubectlError, match="NotFound"):
            list(missing.ssh_capture("true", allow_fail=True))

    def check_ssh_output(self, account):
        assert account.ssh_output("echo out; echo err >&2") == b"out\nerr\n"
        assert account.ssh_output("echo out; echo err >&2", combine_stderr=False) == b"out\n"
        assert account.ssh_output("echo out; exit 1", allow_fail=True) == b"out\n"
        with pytest.raises(socket.timeout):
            account.ssh_output("sleep 5", timeout_sec=0.5)

    def check_ssh_capture(self, account):
        assert list(account.ssh_capture("printf 'a\\nb\\nc'")) == ["a\n", "b\n", "c"]
        assert list(account.ssh_capture("echo a; echo b >&2", callback=str.upper)) == ["A\n", "B\n"]
        assert list(account.ssh_capture("echo a; echo b >&2", combine_stderr=False)) == ["a\n"]

        lines = account.ssh_capture("echo a; exit 2")
        assert next(lines) == "a\n"
        with pytest.raises(RemoteCommandError):
            next(lines)
        assert list(account.ssh_capture("echo a; exit 2", allow_fail=True)) == ["a\n"]

    def check_ssh_capture_timeouts(self, account):
        lines = account.ssh_capture("sleep 1; echo late")
        start = time.time()
        assert not lines.has_next(timeout_sec=0.2)
        assert time.time() - start < 0.9
        assert lines.has_next(timeout_sec=5)
        assert list(lines) == ["late\n"]

        with pytest.raises(socket.timeout):
            list(account.ssh_capture("sleep 5", timeout_sec=0.5))

    def check_background_process_outlives_exec(self, account):
        pid = account.ssh_output("nohup sleep 30 >/dev/null 2>&1 & echo $!", timeout_sec=5).strip().decode()
        try:
            assert account.alive(pid)
        finally:
            account.signal(pid, 9)

    def check_path_tests(self, account, tmp_path):
        (tmp_path / "f").write_text("x")
        (tmp_path / "d").mkdir()
        (tmp_path / "link").symlink_to(tmp_path / "f")
        (tmp_path / "dangling").symlink_to(tmp_path / "nowhere")
        space = tmp_path / "with space"
        space.write_text("x")

        assert account.isfile(str(tmp_path / "f")) and not account.isfile(str(tmp_path / "d"))
        assert account.isdir(str(tmp_path / "d")) and not account.isdir(str(tmp_path / "f"))
        assert account.islink(str(tmp_path / "link")) and not account.islink(str(tmp_path / "f"))
        assert account.isfile(str(tmp_path / "link"))
        assert account.exists(str(tmp_path / "dangling"))
        assert not account.exists(str(tmp_path / "nowhere"))
        assert account.exists(str(space))

    def check_files(self, account, tmp_path):
        path = str(tmp_path / "it's here")
        account.create_file(path, "hello\n")
        assert account.open(path).read() == b"hello\n"
        account.create_file(path, b"\x00bytes")
        assert account.open(path, "rb").read() == b"\x00bytes"
        with account.open(path, "w") as f:
            f.write("text, ")
            f.write(b"bytes")
        with account.open(path, "a") as f:
            f.write(b"!")
        assert account.open(path).read() == b"text, bytes!"

        account.mkdir(str(tmp_path / "m"), 0o700)
        assert (tmp_path / "m").stat().st_mode & 0o777 == 0o700
        with pytest.raises(IOError):
            account.mkdir(str(tmp_path / "m"))
        account.mkdirs(str(tmp_path / "a/b/c"))
        account.remove(str(tmp_path / "a"))
        assert not (tmp_path / "a").exists()

    def check_copy(self, account, tmp_path):
        src = tmp_path / "src"
        (src / "sub").mkdir(parents=True)
        (src / "top.txt").write_text("top")
        (src / "sub" / "leaf.bin").write_bytes(b"\x00\x01")
        remote = tmp_path / "remote"
        local = tmp_path / "local"
        remote.mkdir()
        local.mkdir()

        # Into an existing directory: the basename is re-anchored under it.
        account.copy_to(str(src), str(remote))
        assert (remote / "src" / "sub" / "leaf.bin").read_bytes() == b"\x00\x01"
        account.copy_to(str(src / "top.txt"), str(remote / "renamed.txt"))
        assert (remote / "renamed.txt").read_text() == "top"

        account.copy_from(str(remote / "src"), str(local))
        assert (local / "src" / "top.txt").read_text() == "top"
        assert (local / "src" / "sub" / "leaf.bin").read_bytes() == b"\x00\x01"
        account.copy_from(str(remote / "renamed.txt"), str(local / "back.txt"))
        assert (local / "back.txt").read_text() == "top"

        with pytest.raises(RemoteCommandError):
            account.copy_from(str(remote / "nothing"), str(local / "nothing"))

    def check_monitor_log(self, account, tmp_path):
        log = str(tmp_path / "log")
        account.create_file(log, "old line\n")
        with account.monitor_log(log) as monitor:
            account.ssh("echo 'new line' >> '%s'" % log)
            monitor.wait_until("new line", timeout_sec=5)

    def check_available_and_pod_ip(self, kubectl_log):
        assert KubectlRemoteAccount("dt-node-0").available()
        assert not KubectlRemoteAccount("unready").available()
        assert not KubectlRemoteAccount("missing").available()
        assert KubectlRemoteAccount("dt-node-0").pod_ip() == "10.1.2.3"
        with pytest.raises(KubectlError, match="NotFound"):
            KubectlRemoteAccount("missing").pod_ip()

    def check_no_paramiko(self, account):
        with pytest.raises(NotImplementedError):
            account.ssh_client
        with pytest.raises(NotImplementedError):
            account.sftp_client
        account.close()


class CheckKubeCluster(object):
    cluster_json = {
        "nodes": [
            {"externally_routable_ip": None,
             "kubectl": {"pod": "dt-node-%d" % i, "namespace": "ducktape-proto", "container": None,
                         "kubeconfig": None}}
            for i in range(3)
        ] + [{"ssh_config": {"host": "worker1", "hostname": "10.9.9.9"}}]
    }

    def check_nodes(self, kubectl_log):
        cluster = KubeCluster(self.cluster_json)
        assert len(cluster) == 4

        nodes = cluster.alloc(ClusterSpec.simple_linux(4))
        pods = [n.account for n in nodes if isinstance(n.account, KubectlRemoteAccount)]
        assert sorted(a.pod for a in pods) == ["dt-node-0", "dt-node-1", "dt-node-2"]
        assert all(a.hostname == a.externally_routable_ip == "10.1.2.3" for a in pods)
        assert [n.account.hostname for n in nodes if n.account not in pods] == ["worker1"]
        cluster.free(nodes)
        assert len(cluster.available()) == 4

    def check_given_externally_routable_ip_is_kept(self, kubectl_log):
        cluster = KubeCluster({"nodes": [{"externally_routable_ip": "1.2.3.4", "kubectl": {"pod": "p"}}]})
        account = cluster.alloc(ClusterSpec.simple_linux(1))[0].account
        assert (account.hostname, account.externally_routable_ip) == ("10.1.2.3", "1.2.3.4")

    def check_unready_pods_are_not_allocated(self, kubectl_log):
        cluster = KubeCluster({"nodes": [{"kubectl": {"pod": "unready"}}, {"kubectl": {"pod": "p"}}]})
        nodes = cluster.alloc(ClusterSpec.simple_linux(1))
        assert [n.account.pod for n in nodes] == ["p"]

    def check_invalid(self, kubectl_log):
        with pytest.raises(ValueError, match="NotFound"):
            KubeCluster({"nodes": [{"kubectl": {"pod": "missing"}}]})
        with pytest.raises(ValueError):
            KubeCluster({"nodes": [{"kubectl": {"pod": "p", "typo": 1}}]})

    def check_pickleable(self, kubectl_log):
        cluster = KubeCluster(self.cluster_json)
        pickle.loads(pickle.dumps(cluster))

    def check_cluster_file(self, kubectl_log, tmp_path):
        cluster_file = tmp_path / "cluster.json"
        cluster_file.write_text(json.dumps(self.cluster_json))
        assert len(KubeCluster(cluster_file=str(cluster_file))) == 4
