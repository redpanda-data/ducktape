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

"""Kubernetes pods as ducktape nodes, reached with ``kubectl exec`` instead of ssh."""

import io
import logging
import os
import re
import select
import shlex
import socket
import subprocess
import tarfile
import tempfile

from ducktape.cluster.json import JsonCluster
from ducktape.cluster.linux_remoteaccount import LinuxRemoteAccount
from ducktape.cluster.remoteaccount import RemoteAccountError, RemoteAccountSSHConfig, RemoteCommandError, \
    SSHOutputIter

# kubectl exec reports a remote command's non-zero exit status with this line on stderr.
# A failure without it is kubectl's own: the pod is gone, the API server is unreachable, RBAC denied the exec.
_REMOTE_EXIT = re.compile(rb"^command terminated with exit code \d+\n?", re.MULTILINE)


class KubectlError(RemoteAccountError):
    """kubectl itself failed, as opposed to the command it ran in the pod."""


class KubectlRemoteAccount(LinuxRemoteAccount):
    """A RemoteAccount whose node is a Kubernetes pod.

    Commands run under ``bash -c`` in the pod, like an ssh login shell would, so the image needs bash,
    and tar for directory copies. There is no ssh or sftp client.
    """

    def __init__(self, pod, namespace=None, container=None, kubeconfig=None, externally_routable_ip=None,
                 logger=None, ssh_exception_checks=None):
        # The base class takes its hostname from the ssh config, so this one only names the pod.
        super().__init__(RemoteAccountSSHConfig(host=pod, hostname=pod), externally_routable_ip, logger,
                         ssh_exception_checks)
        self.pod = pod
        self.namespace = namespace
        self.container = container
        self.kubeconfig = kubeconfig

    def __str__(self):
        return "%s/%s" % (self.namespace, self.pod) if self.namespace else self.pod

    @property
    def ssh_client(self):
        raise NotImplementedError("%s is reached with kubectl exec and has no ssh client; use ssh*()" % self)

    @property
    def sftp_client(self):
        raise NotImplementedError("%s is reached with kubectl exec and has no sftp client; "
                                  "use copy_to(), copy_from() and the file methods" % self)

    def _kubectl(self, *args):
        argv = ["kubectl"]
        if self.kubeconfig:
            argv += ["--kubeconfig", self.kubeconfig]
        if self.namespace:
            argv += ["--namespace", self.namespace]
        return argv + list(args)

    def _exec_argv(self, cmd, combine_stderr, stdin):
        if combine_stderr:
            # Merge in the pod, so kubectl's own stderr stays separate and _check_exit can read it.
            cmd = "exec 2>&1\n" + cmd
        return self._kubectl("exec", *(["-i"] if stdin else []), *(["-c", self.container] if self.container else []),
                             self.pod, "--", "bash", "-c", cmd)

    def _check_exit(self, cmd, exit_status, stderr, allow_fail):
        if exit_status == 0:
            return
        msg, remote_failed = _REMOTE_EXIT.subn(b"", stderr)
        if not remote_failed:
            raise KubectlError(self, "kubectl exec failed with exit status %d running '%s': %s\n"
                               "Check that the pod is Running and that kubectl reaches its cluster and namespace."
                               % (exit_status, cmd, stderr.decode(errors="replace").strip()))
        if not allow_fail:
            raise RemoteCommandError(self, cmd, exit_status, msg)
        self._log(logging.DEBUG, "Running command '%s' exited with status %d and message: %s" % (cmd, exit_status, msg))

    def _run(self, cmd, allow_fail=False, combine_stderr=False, input=None, stdout=subprocess.PIPE, timeout_sec=None):
        """Run cmd in the pod to completion. Return its exit status and its stdout, or None if stdout isn't a pipe."""
        self._log(logging.DEBUG, "Running kubectl exec command: %s" % cmd)
        with tempfile.TemporaryFile() as stderr:
            proc = subprocess.Popen(self._exec_argv(cmd, combine_stderr, input is not None),
                                    stdin=subprocess.DEVNULL if input is None else subprocess.PIPE,
                                    stdout=stdout, stderr=stderr)
            try:
                out, _ = proc.communicate(input, timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise socket.timeout("%s: command '%s' timed out after %ss" % (self, cmd, timeout_sec))
            stderr.seek(0)
            self._check_exit(cmd, proc.returncode, stderr.read(), allow_fail)
        return proc.returncode, out

    def ssh(self, cmd, allow_fail=False):
        return self._run(cmd, allow_fail, stdout=subprocess.DEVNULL)[0]

    def ssh_output(self, cmd, allow_fail=False, combine_stderr=True, timeout_sec=None):
        out = self._run(cmd, allow_fail, combine_stderr, timeout_sec=timeout_sec)[1]
        self._log(logging.DEBUG, "Returning ssh command output:\n%s" % out)
        return out

    def ssh_capture(self, cmd, allow_fail=False, callback=None, combine_stderr=True, timeout_sec=None):
        self._log(logging.DEBUG, "Running kubectl exec command: %s" % cmd)
        stderr = tempfile.TemporaryFile()
        proc = subprocess.Popen(self._exec_argv(cmd, combine_stderr, False),
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=stderr)
        stdout = _PipeLines(proc.stdout, timeout_sec)

        def output_generator():
            for line in iter(stdout.readline, ''):
                yield line if callback is None else callback(line)
            try:
                proc.wait()
                stderr.seek(0)
                self._check_exit(cmd, proc.returncode, stderr.read(), allow_fail)
            finally:
                proc.stdout.close()
                stderr.close()

        return SSHOutputIter(output_generator, stdout)

    def _succeeds(self, cmd):
        return self._run(cmd, allow_fail=True, stdout=subprocess.DEVNULL)[0] == 0

    def islink(self, path):
        return self._succeeds("test -L %s" % shlex.quote(path))

    def isdir(self, path):
        return self._succeeds("test -d %s" % shlex.quote(path))

    def exists(self, path):
        """Test that the path exists, but don't follow symlinks."""
        path = shlex.quote(path)
        return self._succeeds("test -e %s || test -L %s" % (path, path))

    def isfile(self, path):
        return self._succeeds("test -f %s" % shlex.quote(path))

    def open(self, path, mode='r'):
        """Like the sftp file this replaces: reads return bytes, and writes upload on close."""
        if 'r' in mode:
            return io.BytesIO(self._run("cat %s" % shlex.quote(path))[1])
        return _UploadOnClose(self, path, append='a' in mode)

    def create_file(self, path, contents, append=False):
        if isinstance(contents, str):
            contents = contents.encode()
        self._run("cat %s %s" % (">>" if append else ">", shlex.quote(path)), input=contents,
                  stdout=subprocess.DEVNULL)

    def mkdir(self, path, mode=0o755):
        try:
            self._run("mkdir -m %o %s" % (mode, shlex.quote(path)), stdout=subprocess.DEVNULL)
        except RemoteCommandError as e:
            # sftp's mkdir raises IOError, and callers catch that.
            raise IOError(str(e)) from e

    def copy_to(self, src, dest):
        if self.isdir(dest):
            dest = self._re_anchor_basename(src, dest)

        if not os.path.isdir(src):
            with open(src, "rb") as f:
                self.create_file(dest, f.read())
            return

        tar = io.BytesIO()
        with tarfile.open(fileobj=tar, mode="w") as t:
            t.add(src, arcname=".")
        dest = shlex.quote(dest)
        self._run("mkdir -p %s && tar --no-same-owner -x -C %s" % (dest, dest), input=tar.getvalue(),
                  stdout=subprocess.DEVNULL)

    def copy_from(self, src, dest):
        if os.path.isdir(dest):
            dest = self._re_anchor_basename(src, dest)

        if not self.isdir(src):
            with open(dest, "wb") as f:
                self._run("cat %s" % shlex.quote(src), stdout=f)
            return

        os.mkdir(dest)
        with tempfile.TemporaryFile() as tar:
            # GNU tar exits 1 when a file changed while it was read (a log still being written); the archive is fine.
            self._run("tar -c -C %s . || [ $? -eq 1 ]" % shlex.quote(src), stdout=tar)
            tar.seek(0)
            with tarfile.open(fileobj=tar) as t:
                t.extractall(dest, filter="tar")

    def _get_pod(self, jsonpath):
        return subprocess.run(self._kubectl("get", "pod", self.pod, "-o", "jsonpath=" + jsonpath),
                              capture_output=True, text=True)

    def available(self):
        """True if the pod exists and is Ready."""
        proc = self._get_pod('{.status.conditions[?(@.type=="Ready")].status}')
        if proc.returncode == 0 and proc.stdout.strip() == "True":
            return True
        self._log(logging.WARNING, "pod is not Ready: %s" % (proc.stderr.strip() or "Ready=%r" % proc.stdout.strip()))
        return False

    def pod_ip(self):
        proc = self._get_pod("{.status.podIP}")
        ip = proc.stdout.strip()
        if proc.returncode != 0 or not ip:
            raise KubectlError(self, "can't get the pod IP: %s\nCheck that the pod exists and is Running, "
                               "and that kubectl reaches its cluster and namespace."
                               % (proc.stderr.strip() or "the pod has no IP yet"))
        return ip


class _UploadOnClose(io.BytesIO):
    def __init__(self, account, path, append):
        super().__init__()
        self._account = account
        self._path = path
        self._append = append

    def write(self, data):
        return super().write(data.encode() if isinstance(data, str) else data)

    def close(self):
        if not self.closed:
            self._account.create_file(self._path, self.getvalue(), self._append)
        super().close()


class _PipeLines(object):
    """Text lines from a pipe, with a per-read timeout that raises socket.timeout, like a paramiko ChannelFile.

    It is its own ``channel``, so SSHOutputIter.has_next can set and restore the timeout.
    """

    def __init__(self, pipe, timeout):
        self.channel = self
        self._pipe = pipe
        self._timeout = timeout
        self._buf = b""

    def settimeout(self, timeout):
        self._timeout = timeout

    def gettimeout(self):
        return self._timeout

    def readline(self):
        while b"\n" not in self._buf:
            if not select.select([self._pipe], [], [], self._timeout)[0]:
                raise socket.timeout()
            chunk = os.read(self._pipe.fileno(), 65536)
            if not chunk:
                break
            self._buf += chunk
        line, sep, self._buf = self._buf.partition(b"\n")
        return (line + sep).decode(errors="replace")


class KubeCluster(JsonCluster):
    """A JsonCluster whose nodes may be Kubernetes pods.

    A node with a "kubectl" block becomes a KubectlRemoteAccount; a node with an "ssh_config" works as in
    JsonCluster. Pod names don't resolve in DNS, so a pod's hostname, and its externally_routable_ip unless
    given, is the pod IP::

        {"nodes": [{"externally_routable_ip": null,
                    "kubectl": {"pod": "dt-node-0", "namespace": "ducktape-proto",
                                "container": null, "kubeconfig": null}}]}

    A null kubeconfig means kubectl's default: in-cluster config, or $KUBECONFIG and ~/.kube/config.
    """

    def _make_remote_account(self, ninfo, make_remote_account_func, ssh_exception_checks):
        if "kubectl" not in ninfo:
            return super()._make_remote_account(ninfo, make_remote_account_func, ssh_exception_checks)

        account = KubectlRemoteAccount(externally_routable_ip=ninfo.get("externally_routable_ip"),
                                       ssh_exception_checks=ssh_exception_checks, **ninfo["kubectl"])
        account.hostname = account.pod_ip()
        if account.externally_routable_ip is None:
            account.externally_routable_ip = account.hostname
        return account
