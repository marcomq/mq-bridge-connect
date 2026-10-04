#!/usr/bin/env python3
"""Round-trips messages through Redpanda Connect connectors with the `mqb` CLI,
and runs its processors as mq-bridge middlewares.

Each case in cases.toml is a stream template in the format of Redpanda
Connect's own integration tests (an `output:` and an `input:` block with
`$PORT`, `$ID`, `$VARn` placeholders), so one can be pasted from upstream
unchanged. The runner starts the broker the case names, publishes N messages
through the output, reads them back through the input and compares. A
`[middlewares.*]` case feeds `input` lines through a file-to-file route that
carries the middlewares, and compares what arrives with `expect`.

    python3 tests/endpoints/run.py                 # everything runnable here
    python3 tests/endpoints/run.py redis nats      # cases whose name matches
    python3 tests/endpoints/run.py --list

Needs `mqb` on PATH and the plugin where mqb discovers it, or `--plugin`.
Standard library only (Python 3.11+). Cases whose broker cannot be started
are skipped; `--strict` turns a skip into a failure, which is what CI uses.
"""

import argparse
import concurrent.futures
import http.server
import json
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import urllib.parse
import uuid

HERE = pathlib.Path(__file__).resolve().parent
RUN = uuid.uuid4().hex[:8]

# What upstream's StreamTestOpt* defaults substitute into a template.
DEFAULTS = {
    "MAX_IN_FLIGHT": "1",
    "OUTPUT_BATCH_COUNT": "0",
    "OUTPUT_META_EXCLUDE_PREFIX": "",
    "INPUT_BATCH_COUNT": "0",
}


SCHEMA = json.dumps({
    "type": "record", "name": "r",
    "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}],
})  # fmt: skip


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def substitute(text: str, values: dict) -> str:
    # Longest name first, so `$VAR10` is not read as `$VAR1` followed by `0`.
    for name in sorted(values, key=len, reverse=True):
        text = text.replace(f"${name}", str(values[name]))
    return text


def section(template: str, drop: str) -> str:
    """The template without the top-level block `drop`, which mq-bridge owns."""
    out, keep = [], True
    for line in template.splitlines():
        top = re.match(r"([A-Za-z_]+):", line)
        if top:
            keep = top.group(1) != drop
        if keep:
            out.append(line)
    return "\n".join(out) + "\n"


def endpoint(yaml: str) -> str:
    return "connect://?yaml=" + urllib.parse.quote(yaml, safe="")


def tail(path: pathlib.Path, lines: int = 12) -> str:
    if not path.exists():
        return ""
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])


class Broker:
    def __init__(self, name: str, spec: dict, docker: bool):
        self.name, self.spec, self.docker = name, spec, docker
        self.port = None
        self.container = self.process = None
        self.unavailable = None
        self.captured = {}

    def start(self):
        exe = self.spec.get("exec")
        if exe and shutil.which(exe[0]):
            self.port = free_port()
            argv = [substitute(a, {"PORT": self.port}) for a in exe]
            self.process = subprocess.Popen(
                argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        elif not self.docker:
            self.unavailable = "no docker"
            return
        else:
            self.container = f"mqb-endpoints-{RUN}-{self.name}"
            argv = ["docker", "run", "-d", "--rm", "--name", self.container]
            ports = {"": self.spec["port"], **self.spec.get("ports", {})}
            for port in ports.values():
                argv += ["-p", f"127.0.0.1::{port}"]
            for key, value in self.spec.get("env", {}).items():
                argv += ["-e", f"{key}={value}"]
            argv += [self.spec["image"], *self.spec.get("args", [])]
            started = subprocess.run(argv, capture_output=True, text=True)
            if started.returncode:
                self.unavailable = started.stderr.strip().splitlines()[-1]
                return
            for name, port in ports.items():
                mapped = subprocess.run(
                    ["docker", "port", self.container, f"{port}/tcp"],
                    capture_output=True,
                    text=True,
                ).stdout
                self.captured[name] = int(mapped.split()[0].rsplit(":", 1)[1])
            self.port = self.captured.pop("")
        if not self.wait_ready():
            self.unavailable = "broker did not become ready"
            return
        # What a template needs and only the running broker knows: a host key.
        for name, command in self.spec.get("capture", {}).items():
            if self.container:
                command = ["docker", "exec", self.container, *command]
            self.captured[name] = subprocess.run(
                command, capture_output=True, text=True
            ).stdout.strip()

    def wait_ready(self) -> bool:
        pattern = self.spec.get("ready_log")
        deadline = time.time() + self.spec.get("ready_timeout", 60)
        while time.time() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port), 1).close()
                if not (pattern and self.container):
                    return True
                logs = subprocess.run(
                    ["docker", "logs", self.container], capture_output=True, text=True
                )
                if re.search(pattern, logs.stdout + logs.stderr):
                    return True
            except OSError:
                pass
            time.sleep(0.5)
        return False

    def stop(self):
        if self.process:
            self.process.kill()
        if self.container:
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)


def run_case(name: str, case: dict, broker, args, workdir: pathlib.Path):
    """Returns (passed, detail)."""
    case_dir = workdir / name
    case_dir.mkdir()
    run_id = f"{RUN}{name.replace('_', '')}"
    count = case.get("count", 10)
    values = {
        **DEFAULTS,
        "ID": run_id,
        "DIR": case_dir,
        "PORT": broker.port if broker else free_port(),
        **(broker.captured if broker else {}),
        **case.get("vars", {}),
    }
    template = substitute(case["template"], values)
    generate = (
        "input:\n  generate:\n"
        f"    count: {count}\n"
        '    interval: ""\n'
        "    mapping: |\n"
        f'      root = {{"id": counter(), "run": "{run_id}"}}\n'
        f'      meta mqbtest = "{run_id}"\n'
    )
    received = case_dir / "received.jsonl"
    # `stdio`: what the publishing process prints is the reading one's stdin.
    stdio, piped = case.get("stdio"), case_dir / "stdout"
    mqb = [args.mqb, "copy"]
    if args.plugin:
        mqb += ["--plugin", args.plugin]
    env = {**os.environ, **{k: substitute(v, values) for k, v in case.get("env", {}).items()}}

    def consume():
        log = open(case_dir / "input.log", "w")
        return subprocess.Popen(
            [*mqb, endpoint(section(template, "output")), f"file://{received}"],
            stdin=open(piped) if stdio else None, stdout=log, stderr=log, env=env,
        )  # fmt: skip

    def produce():
        with open(case_dir / "output.log", "w") as log:
            return subprocess.run(
                [*mqb, endpoint(generate), endpoint(section(template, "input")), "--drain"],
                stdout=open(piped, "w") if stdio else log, stderr=log, env=env,
                timeout=case.get("timeout", 60),
            ).returncode  # fmt: skip

    # What the connectors will not create themselves -- a queue, a bucket --
    # comes from a stream of its own, run to completion first.
    if "setup" in case:
        setup = substitute(case["setup"], values)
        with open(case_dir / "setup.log", "w") as log:
            code = subprocess.run(
                [*mqb, endpoint(section(setup, "output")), endpoint(section(setup, "input")),
                 "--wait", "3"],
                stdout=log, stderr=log, env=env, timeout=60,
            ).returncode  # fmt: skip
        if code:
            return False, f"setup exited {code}\n{tail(case_dir / 'setup.log')}"

    # A subscription only sees what is published after it exists, so the input
    # goes first unless the case says the data has to be there before it.
    output_first = case.get("order") == "output_first"
    consumer = None
    try:
        if output_first:
            if code := produce():
                return False, f"output exited {code}\n{tail(case_dir / 'output.log')}"
        consumer = consume()
        if not output_first:
            time.sleep(case.get("settle", 1.5))
            if code := produce():
                return False, f"output exited {code}\n{tail(case_dir / 'output.log')}"

        ids, rows = set(), []
        deadline = time.time() + case.get("timeout", 30)
        while time.time() < deadline and len(ids) < count:
            time.sleep(0.2)
            rows = []
            if received.exists():
                for line in received.read_text().splitlines():
                    try:
                        row = json.loads(line)
                        row["payload"] = json.loads(row["payload"])
                        rows.append(row)
                    except (ValueError, KeyError, TypeError):
                        continue
            rows = [r for r in rows if isinstance(r["payload"], dict)]
            rows = [r for r in rows if r["payload"].get("run") == run_id]
            ids = {r["payload"].get("id") for r in rows if isinstance(r["payload"].get("id"), int)}
    except subprocess.TimeoutExpired:
        return False, f"output timed out\n{tail(case_dir / 'output.log')}"
    finally:
        if consumer:
            consumer.terminate()
            try:
                consumer.wait(5)
            except subprocess.TimeoutExpired:
                consumer.kill()

    if ids != set(range(1, count + 1)):
        return False, (
            f"received {len(ids)} of {count}\n"
            f"-- output.log\n{tail(case_dir / 'output.log')}\n"
            f"-- input.log\n{tail(case_dir / 'input.log')}"
        )
    if case.get("metadata"):
        lost = [r for r in rows if r.get("metadata", {}).get("mqbtest") != run_id]
        if lost:
            return False, f"metadata lost on {len(lost)} of {len(rows)} messages"
    return True, f"{count} messages" + (", metadata" if case.get("metadata") else "")


def echo_server() -> int:
    """An HTTP server answering a POST with its body uppercased, for `connect_http`,
    and a GET as a schema registry holding one Avro schema would."""

    class Upper(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0))).upper()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            body = json.dumps({"subject": "s", "version": 1, "id": 1, "schema": SCHEMA}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.schemaregistry.v1+json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upper)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_port


def canonical(payload: str):
    try:
        return json.dumps(json.loads(payload), sort_keys=True)
    except ValueError:
        return payload


def run_middleware(name: str, case: dict, args, workdir: pathlib.Path, http_port: int):
    """Feeds `input` through the middlewares of a file-to-file route. Returns (passed, detail)."""
    case_dir = workdir / name
    case_dir.mkdir()
    source, received, log_path = (case_dir / n for n in ("in.txt", "out.jsonl", "mqb.log"))
    source.write_text("".join(line + "\n" for line in case["input"]))
    middlewares = json.loads(substitute(json.dumps(case["middlewares"]), {"HTTP": http_port, "DIR": case_dir}))
    route = {
        "concurrency": 1,
        "input": {"file": {"path": str(source)}, "middlewares": middlewares},
        "output": {"file": {"path": str(received)}},
    }
    argv = [args.mqb, "--config-str", json.dumps({"routes": {name: route}})]
    argv += ["--no-ui", "--no-metrics"]
    if args.plugin:
        argv += ["--plugin", args.plugin]
    expect = sorted(canonical(p) for p in case.get("expect", []))
    error = case.get("error")

    got = []
    with open(log_path, "w") as log:
        process = subprocess.Popen(argv, stdout=log, stderr=log)
        try:
            deadline = time.time() + case.get("timeout", 15)
            while time.time() < deadline:
                time.sleep(0.2)
                if received.exists():
                    lines = received.read_text().splitlines()
                    got = sorted(canonical(json.loads(line)["payload"]) for line in lines)
                if error and error in log_path.read_text(errors="replace"):
                    break
                if not error and len(got) >= len(expect) and (expect or process.poll() is not None):
                    # Long enough for a message that should have been dropped to show up.
                    time.sleep(0.5)
                    break
        finally:
            process.terminate()
            try:
                process.wait(5)
            except subprocess.TimeoutExpired:
                process.kill()

    if error:
        if error not in log_path.read_text(errors="replace"):
            return False, f"no error containing {error!r}\n{tail(log_path)}"
        if got:
            return False, f"the batch was rejected, yet {len(got)} messages got through"
        return True, "rejected as expected"
    if got != expect:
        return False, f"expected {expect}\n     got {got}\n{tail(log_path, 4)}"
    return True, f"{len(case['input'])} in, {len(got)} out"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("filters", nargs="*", help="run cases whose name contains one of these")
    parser.add_argument("--cases", default=HERE / "cases.toml", type=pathlib.Path)
    parser.add_argument("--mqb", default="mqb", help="the mqb binary (default: from PATH)")
    parser.add_argument("--plugin", help="libmq_bridge_connect to load instead of a discovered one")
    parser.add_argument("--strict", action="store_true", help="a skipped case is a failure")
    parser.add_argument("--list", action="store_true", help="print the cases and exit")
    parser.add_argument("-j", "--jobs", type=int, default=4)
    parser.add_argument("--keep", action="store_true", help="keep the logs of passing runs too")
    args = parser.parse_args()

    config = tomllib.loads(args.cases.read_text())
    cases = {
        name: case
        for name, case in (config["cases"] | config.get("middlewares", {})).items()
        if not args.filters or any(f in name for f in args.filters)
    }
    if args.list:
        for name, case in cases.items():
            print(f"{name:24} {case.get('broker', '-'):12} {case.get('xfail', '')}")
        return 0
    if not shutil.which(args.mqb):
        sys.exit(f"{args.mqb} not found; install mq-bridge-app or pass --mqb")

    try:
        docker = subprocess.run(["docker", "info"], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        docker = False

    workdir = pathlib.Path(tempfile.mkdtemp(prefix="mqb-endpoints-"))
    brokers = {
        name: Broker(name, config["brokers"][name], docker)
        for name in sorted({c["broker"] for c in cases.values() if "broker" in c})
    }
    http_port = echo_server()
    results = {}

    def one(name):
        case = cases[name]
        broker = brokers.get(case.get("broker"))
        started = time.time()
        if broker and broker.unavailable:
            return name, "SKIP", 0.0, broker.unavailable
        try:
            if "middlewares" in case:
                passed, detail = run_middleware(name, case, args, workdir, http_port)
            else:
                passed, detail = run_case(name, case, broker, args, workdir)
        except Exception as error:  # a broken case must not take the run down
            passed, detail = False, f"{type(error).__name__}: {error}"
        if "xfail" in case:
            status = "XPASS" if passed else "XFAIL"
            detail = case["xfail"] if not passed else f"expected to fail: {case['xfail']}"
        else:
            status = "PASS" if passed else "FAIL"
        return name, status, time.time() - started, detail

    try:
        with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
            list(pool.map(Broker.start, brokers.values()))
            for name, status, seconds, detail in pool.map(one, cases):
                results[name] = (status, seconds, detail)
                first, *rest = detail.splitlines() or [""]
                print(f"{status:5} {name:24} {seconds:5.1f}s  {first}", flush=True)
                for line in rest:
                    print(f"      {line}")
    finally:
        for broker in brokers.values():
            broker.stop()

    bad = {"FAIL", "XPASS"} | ({"SKIP"} if args.strict else set())
    failed = [name for name, (status, _, _) in results.items() if status in bad]
    tally = {}
    for status, _, _ in results.values():
        tally[status] = tally.get(status, 0) + 1
    print("\n" + ", ".join(f"{n} {s}" for s, n in sorted(tally.items())))

    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as out:
            out.write("| Connector case | Result | Detail |\n| :--- | :--- | :--- |\n")
            for name, (status, _, detail) in results.items():
                first = (detail.splitlines() or [""])[0]
                out.write(f"| `{name}` | {status} | {first} |\n")

    if failed or args.keep:
        print(f"logs: {workdir}")
    else:
        shutil.rmtree(workdir, ignore_errors=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
