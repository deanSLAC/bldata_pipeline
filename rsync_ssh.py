#!/usr/bin/env python3
"""Shared SSH transport for every rsync this pipeline runs.

Both sync_data.py and bllogs_pipeline/sync_logs.py use this, so the transport
settings cannot drift between them -- which matters, because they connect to the
same host with the same key and therefore share one multiplexed connection.
"""

import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Connection-multiplexing socket. %C is a 40-char hash of (local host, remote
# host, port, user), so one socket per distinct destination. Keep the prefix
# short: the whole path must fit in a unix socket address (108 bytes).
CONTROL_PATH = os.path.join(SCRIPT_DIR, ".mux-%C")

# How long the master connection outlives its last client. Longer than the
# 60-second cron interval on purpose, so consecutive ticks reuse one
# authenticated connection instead of making a new one.
CONTROL_PERSIST = "180"


def build_rsync_ssh(ssh_key):
    """Return rsync's -e argument for our SSH transport, or [] for the default.

    Three things are going on here:

    IdentitiesOnly=yes plus PreferredAuthentications=publickey and
    GSSAPIAuthentication=no reduce each connection to exactly ONE
    authentication attempt. The DTN offers gssapi-keyex, gssapi-with-mic,
    password and keyboard-interactive as well as publickey, and every method
    the client tries counts against the server's MaxAuthTries -- which is the
    likely source of the "Too many authentication failures" disconnects that
    dominate sync.log.

    ControlMaster=auto plus ControlPersist collapse all of a run's rsyncs onto
    ONE TCP connection and ONE authentication. This host was opening 56
    connections per tick, 80,640 per day; with multiplexing it is closer to one
    per master lifetime. At 0.31 ms RTT to the DTN a single TCP flow is not a
    throughput constraint (the bandwidth-delay product is ~35 KB).

    If the socket is ever left stale -- ssh logs "cannot bind to path ...:
    Address already in use" -- ssh falls back to an ordinary unmultiplexed
    connection, so syncs keep working. Delete .mux-* to restore multiplexing.
    """
    if not ssh_key:
        return []
    key_path = os.path.expanduser(ssh_key)
    opts = [
        f"-i {key_path}",
        "-o IdentitiesOnly=yes",
        "-o PreferredAuthentications=publickey",
        "-o GSSAPIAuthentication=no",
        "-o ControlMaster=auto",
        f"-o ControlPath={CONTROL_PATH}",
        f"-o ControlPersist={CONTROL_PERSIST}",
    ]
    return ["-e", "ssh " + " ".join(opts)]
