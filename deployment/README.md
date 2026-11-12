# BMDG deployment example

The binary is `bmdg`. `bmd-gatewayd` remains a transition symlink for old
scripts and units.

The production layout keeps Gateway separate from Recorder:

```text
/opt/bmdg/releases/<release-id>/   release binary, tools and Contracts Python
/opt/bmdg/current -> releases/<release-id>
/etc/bmdg/gateway.json             root:bmdg 0640
/var/lib/bmdg-consumer/            consumer summary state
```

```sh
# Install the Linux release package prepared by CI (never build on the VPS)
sudo install -d -m 0755 /opt/bmdg/releases/<release-id>
sudo tar -xzf bmdg-linux-x86_64-<sha>.tar.gz -C /opt/bmdg/releases/<release-id>
sudo ln -sfn /opt/bmdg/releases/<release-id> /opt/bmdg/current

# Create the service account (once)
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin bmdg

# Install configuration and unit
sudo install -d -m 0750 -o root -g bmdg /etc/bmdg
sudo install -m 0640 -o root -g bmdg examples/gateway-production.json /etc/bmdg/gateway.json
sudo install -m 0644 deployment/bmdg.service /etc/systemd/system/bmdg.service

sudo systemctl daemon-reload
sudo systemctl start bmdg
sudo systemctl status bmdg
```

During the 2h acceptance run, start the `bmdg-consumer.service` probe as the
only 40-stream consumer. Enable `bmdg` and `bmdg-consumer` only after the run
passes.

The unit intentionally uses `RestartSec=300`: BMDG's shared-IP API budget is
in-memory, while Binance's server-side counters persist. A full quiet window is
required after a crash or replacement.

BMDG exposes insecure gRPC on numeric loopback only. Remote consumers should
use an SSH tunnel, WireGuard or another authenticated transport; do not expose
the gRPC port publicly.
