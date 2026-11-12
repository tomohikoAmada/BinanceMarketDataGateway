# BMDG consumer probe

`consume_gateway.py` attaches to an already running `bmdg` and verifies the
20-key / 40-stream contract. It never starts/stops the daemon and never
connects to Binance.

Run it with a client-only virtual environment containing the pinned versions
from `requirements-client.txt`, plus the generated Python package from the
fixed Contracts revision:

```sh
python3 -m venv /opt/bmdg/current/client-venv
/opt/bmdg/current/client-venv/bin/pip install -r /opt/bmdg/current/tools/requirements-client.txt
PYTHONPATH=/opt/bmdg/current/contracts-src/src \
  /opt/bmdg/current/client-venv/bin/python /opt/bmdg/current/tools/consume_gateway.py \
  --target 127.0.0.1:50051 \
  --config /etc/bmdg/gateway.json \
  --profile live40 \
  --duration-seconds 0 \
  --output /var/lib/bmdg-consumer/summary.json
```

`--duration-seconds 0` runs until SIGTERM. A positive value is a finite
development or acceptance run. The summary is updated atomically every 30
seconds and contains per-stream first snapshot, updates/events, resubscriptions,
gap reasons, errors and fixed-bucket latency facts. Negative UTC-clock
latencies are retained, not clamped.

Production acceptance uses `--supervisor`: one order-book process plus one
independent process per Spot symbol (aggTrade + BookTicker), so a burst on one
BookTicker stream cannot starve the order-book or other symbols. The supervisor
merges the shard summaries and runs one `GetGatewayStatus` loop. Its 40
subscriptions still count against the gateway's 48-stream cap.

`acceptance_monitor.py` samples the running units, consumer summary, archive
timer and host resources into JSONL and writes a final verdict. It is an
observability tool only; it does not start, stop or reconfigure the services.

The `bmdg-consumer.service` unit is the thin same-host deployment wrapper.
During acceptance it is the only live40 consumer; stop it before a real
business consumer starts so the gateway's 48-stream cap is not exceeded.
