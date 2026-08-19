"""
Integration test for the master-isolation guarantee (design §3, F1-F3):
a frozen/slow replica must NEVER stall the master's write path.

Strategy: freeze the replica container so it stops draining its replication
stream, then hammer the master with writes and assert every write still
returns quickly and the master keeps serving.

Harness requirement: the tests run inside the pymarketstore container, so this
test needs a usable container CLI (docker OR podman) with access to the engine
that manages the replica container — mount the runtime socket into the pyclient
container (/var/run/docker.sock, or $XDG_RUNTIME_DIR/podman/podman.sock). When
no engine is reachable the test skips rather than failing — the same guarantee
is covered at the Go level by replication/sender_test.go and
replication/grpc_server_test.go (-race).
"""
import os
import time
import shutil
import subprocess

import numpy as np
import pandas as pd
import pytest
import pymarketstore as pymkts

REPLICA_CONTAINER = os.getenv("REPLICA_CONTAINER_NAME", "replication_tests_mstore_replica1")

master = pymkts.Client(f"http://127.0.0.1:{os.getenv('MASTER_PORT', 5996)}/rpc")


def _detect_engine() -> str | None:
    """Return the container CLI in use (docker or podman), or None if neither is
    reachable. CONTAINER_ENGINE overrides autodetection."""
    override = os.getenv("CONTAINER_ENGINE")
    candidates = [override] if override else ["docker", "podman"]
    for engine in candidates:
        if engine and shutil.which(engine):
            return engine
    return None


ENGINE = _detect_engine()


def _engine(*args) -> bool:
    """Run a container-engine command; return True on success, False if unusable."""
    if ENGINE is None:
        return False
    try:
        return subprocess.run([ENGINE, *args], check=False,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    except Exception:
        return False


def test_master_writes_survive_frozen_replica():
    # Freeze the replica so it stops draining its replication stream.
    if not _engine("pause", REPLICA_CONTAINER):
        pytest.skip("no container engine (docker/podman) reachable from the test "
                    "container; isolation is covered by the Go -race unit tests")
    try:
        for i in range(200):
            data = np.array([((pd.Timestamp('2019-01-01 00:00').value / 10 ** 9) + i, 1.0, 2.0)],
                            dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')])
            t0 = time.time()
            master.write(data, 'ISO/1Min/OHLCV')
            assert time.time() - t0 < 2.0, "master write stalled while replica was frozen"
    finally:
        _engine("unpause", REPLICA_CONTAINER)
    master.destroy('ISO/1Min/OHLCV')
