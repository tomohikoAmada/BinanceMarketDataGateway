# BMDG deployment example

The binary is `bmdg`. `bmd-gatewayd` remains a transition symlink for old
scripts and units.

```sh
# Build/install the daemon
cmake --install build/bmdg-release --prefix /usr/local

# Create the service account (once)
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin bmdg

# Install configuration and unit
sudo install -d -m 0750 -o root -g bmdg /etc/bmdg
sudo install -m 0640 -o root -g bmdg examples/gateway.json /etc/bmdg/gateway.json
sudo install -m 0644 deployment/bmdg.service /etc/systemd/system/bmdg.service

sudo systemctl daemon-reload
sudo systemctl enable --now bmdg
sudo systemctl status bmdg
```

The unit intentionally uses `RestartSec=300`: BMDG's shared-IP API budget is
in-memory, while Binance's server-side counters persist. A full quiet window is
required after a crash or replacement.

BMDG exposes insecure gRPC on numeric loopback only. Remote consumers should
use an SSH tunnel, WireGuard or another authenticated transport; do not expose
the gRPC port publicly.
