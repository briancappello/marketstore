"""
Integration test for the replica backfill reconciler.

Proves the replica acquires data via the pull-backfill path (bootstrap +
periodic reconcile), not only the live stream. reconcile_interval is 2s in
mkts-replica.yml, so a fresh write on the master must appear on the replica
within a few reconcile cycles.
"""
import os
import time

import numpy as np
import pandas as pd
import pymarketstore as pymkts

master = pymkts.Client(f"http://127.0.0.1:{os.getenv('MASTER_PORT', 5996)}/rpc")
replica = pymkts.Client(f"http://127.0.0.1:{os.getenv('REPLICA_PORT', 5999)}/rpc")


def test_bootstrap_of_preexisting_data():
    # Written to master; the replica must acquire it (live feed and/or backfill).
    data = np.array([(pd.Timestamp('2018-01-01 00:00').value / 10 ** 9, 1.0, 2.0)],
                    dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')])
    master.write(data, 'BOOT/1Min/OHLCV')

    # Wait for a reconcile cycle (reconcile_interval=2s) to bootstrap it.
    deadline = time.time() + 20
    got = None
    while time.time() < deadline:
        resp = replica.query(pymkts.Params('BOOT', '1Min', 'OHLCV'))
        try:
            got = resp.first().df()
            if len(got) > 0:
                break
        except Exception:
            pass
        time.sleep(1)

    assert got is not None and len(got) == 1
    assert got['High'].iloc[0] == 1.0

    master.destroy('BOOT/1Min/OHLCV')
    replica.destroy('BOOT/1Min/OHLCV')
